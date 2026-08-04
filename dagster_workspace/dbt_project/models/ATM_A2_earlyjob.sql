/*
Created: 2026-03-18
Description: 特定條件異常交易清單產出 (本國自然人與外籍移工學生)
Change Log:
- 2026-05-22 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
- 2026-03-18 [REFACT] Refactor from python script
*/

{{ config(
    materialized='incremental',
    alias='mrt_ATM_A2_earlyjob', 
    tags=["ATM A2", "daily_job"]
) }}

{% set target_date = var("target_date", "2026-02-01") %}
{% set target_ym = target_date[:7] %}

-- 1. T_BASE 系列: 保持乾淨的 SELECT，進行初步過濾與型別轉換
WITH T_CUST_PS_BASE AS (
    SELECT 
        ACT_NO
        , ID
        , ID_DUP_NO
        , STATUS_PBA_CODE
    FROM {{ ref('snp_dim_cust_act') }}
    WHERE NULLIF(LTRIM(RTRIM(ID)), '') IS NOT NULL
    {% if target_date == run_started_at.strftime("%Y-%m-%d") %}
      AND dbt_valid_to IS NULL
    {% else %}
      AND CAST('{{ target_date }}' AS DATETIME) >= dbt_valid_from 
      AND CAST('{{ target_date }}' AS DATETIME) < ISNULL(dbt_valid_to, '9999-12-31')
    {% endif %}
)

, T_CUST_BASE AS (
    SELECT 
        ID
        , ID_DUP_NO
        , ID_TYPE
        , BIRTH_DATE
        , OCUP_CODE
        , NATION_CODE
        , CASE 
            WHEN BIRTH_DATE LIKE '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]' 
            THEN YEAR(GETDATE()) - CAST(SUBSTRING(BIRTH_DATE, 1, 4) AS INT)
            ELSE 0 
          END AS [年齡]
        , CASE 
            WHEN OCUP_CODE = '73' 
            OR (
                OCUP_CODE = '50' 
                AND LOWER(ISNULL(NATION_CODE, '')) NOT IN ('', 'tw', '  ')
            ) 
            THEN 'Y'
            ELSE '' 
          END AS [外籍]
    FROM {{ ref('snp_cust') }}
    WHERE NULLIF(LTRIM(RTRIM(ID)), '') IS NOT NULL
      AND TABLE_DATE = '{{ target_ym }}'
)

, T_ACT_BASE AS (
    SELECT 
        ps.ACT_NO
        , c.[年齡]
        , c.OCUP_CODE AS [職業]
        , c.NATION_CODE AS [國籍]
        , c.ID_TYPE
        , c.BIRTH_DATE
        , c.[外籍]
        , w.STATUS_PBA_CODE AS [帳戶狀態]
    FROM T_CUST_PS_BASE ps
    LEFT JOIN T_CUST_BASE c 
        ON ps.ID = c.ID 
        AND ps.ID_DUP_NO = c.ID_DUP_NO
    LEFT JOIN {{ source('database', 'T_CUST_WARNINGFLG') }} w 
        ON ps.ACT_NO = w.ACT_NO
        AND w.TABLE_DATE = '{{ target_date }}'
)

, T_TXN_PS_BASE AS (
    SELECT
        ACT_NO
        , CPU_DATE
        , TXN_TIME
        , Seq_ID
        , FUNC_CODE
        , DR_FLG
        , CAST(TXN_AMT AS BIGINT) AS TXN_AMT
        , CAST(PS_BAL AS BIGINT) AS PS_BAL
    FROM {{ ref('txn_ps_net') }}
    WHERE NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
      AND TABLE_DATE = '{{ target_date }}'
)


-- 2. T_TXN_PS_AGG 系列: 處理運算邏輯與聚合
, T_TXN_PS_AGG1 AS (
    SELECT 
        ACT_NO
        , CPU_DATE
        , TXN_TIME
        , TXN_AMT
        , PS_BAL
        , FUNC_CODE
        , DR_FLG
        , ROW_NUMBER() OVER(
            PARTITION BY ACT_NO 
            ORDER BY CPU_DATE DESC, TXN_TIME DESC
          ) AS RN_LAST
    FROM T_TXN_PS_BASE
)

, T_TXN_PS_AGG2 AS (
    SELECT 
        ACT_NO
        , MAX(
            CASE 
                WHEN RN_LAST = 1 
                THEN PS_BAL 
            END
          ) AS [本日最後餘額]
        , SUM(
            CASE 
                WHEN {{ is_inward_large('ATM_A2') }} 
                THEN 1 
                ELSE 0 
            END
          ) AS [指定入帳次數]
        , SUM(
            CASE 
                WHEN {{ is_inward_large('ATM_A2') }} 
                THEN TXN_AMT 
                ELSE 0 
            END
          ) AS [累計入帳金額]
        , SUM(
            CASE 
                WHEN {{ is_atm_outward_all('ATM_A2') }} 
                THEN 1 
                ELSE 0 
            END
          ) AS [ATM提領次數]
        , SUM(
            CASE 
                WHEN {{ is_outward_all('ATM_A2') }} 
                THEN TXN_AMT 
                ELSE 0 
            END
          ) AS [累計出帳金額]
    FROM T_TXN_PS_AGG1
    GROUP BY ACT_NO
)


-- 3. T_TXN_PS_FULL: 合併帳戶特徵與交易特徵，並計算比率
, T_TXN_PS_FULL AS (
    SELECT 
        a.ACT_NO
        , a.[年齡]
        , a.[職業]
        , a.[國籍]
        , a.ID_TYPE
        , a.BIRTH_DATE
        , a.[外籍]
        , a.[帳戶狀態]
        , ISNULL(m.[累計出帳金額], 0) AS [累計出帳金額]
        , ISNULL(m.[累計入帳金額], 0) AS [累計入帳金額]
        , ROUND(
            {{ safe_divide('m.[累計出帳金額]', 'm.[累計入帳金額]', '0') }}
            , 2
          ) AS [出入帳比率]
        , ISNULL(m.[本日最後餘額], 0) AS [本日最後餘額]
        , ISNULL(m.[指定入帳次數], 0) AS [指定入帳次數]
        , ISNULL(m.[ATM提領次數], 0) AS [ATM提領次數]
    FROM T_ACT_BASE a
    LEFT JOIN T_TXN_PS_AGG2 m 
        ON a.ACT_NO = m.ACT_NO
)


-- 4. T_TXN_PS_FINAL: 條件篩選與最終輸出
, T_TXN_PS_FINAL AS (
    SELECT 
        ACT_NO AS [帳號]
        , [年齡]
        , [職業]
        , [國籍]
        , [帳戶狀態]
        , [累計出帳金額]
        , [累計入帳金額]
        , [出入帳比率]
        , [本日最後餘額]
        , [外籍]
        , BIRTH_DATE
        , [指定入帳次數]
        , [ATM提領次數]
    FROM T_TXN_PS_FULL
    WHERE (
        -- 條件 1: 本國自然人
        (
            (
                ID_TYPE = '1' 
                AND LOWER(ISNULL([國籍], '')) IN ('', 'tw', '  ') 
                OR ID_TYPE IS NULL
            )
            AND [ATM提領次數] BETWEEN 3 AND 7
            AND [出入帳比率] BETWEEN 0 AND 3.62
            AND [指定入帳次數] BETWEEN 2 AND 5
            AND [累計入帳金額] BETWEEN 68178 AND 105994
            AND [累計出帳金額] BETWEEN 85000 AND 102047
            AND [本日最後餘額] <= 40000
        )
        OR 
        -- 條件 2: 外籍移工學生
        (
            [外籍] = 'Y'
            AND [ATM提領次數] BETWEEN 2 AND 8
            AND [出入帳比率] BETWEEN 0.92 AND 1.01
            AND [指定入帳次數] BETWEEN 1 AND 8
            AND [累計入帳金額] BETWEEN 86000 AND 102437
            AND [累計出帳金額] BETWEEN 90000 AND 101074
            AND [本日最後餘額] <= 40000
        )
    )
)

SELECT 
    [帳號]
    , [年齡]
    , [職業]
    , [國籍]
    , [帳戶狀態]
    , [累計出帳金額]
    , [累計入帳金額]
    , [出入帳比率]
    , [本日最後餘額]
    , [外籍]
    , BIRTH_DATE
    , [指定入帳次數]
    , [ATM提領次數]
    , 'earlyjob' AS remark
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL;