/*
Created: 2026-03-18
Description: 特定條件異常交易清單產出 (外籍客戶、特定出入帳區間、入帳為千元整數)
Change Log:
- 2026-05-22 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_ATM_B3', 
    tags=["ATM B3", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定 (使用 dbt var 以支援 Dagster 排程動態傳入)
-- ======================================================================
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
        , c.OCUP_CODE AS [職業]
        , c.[年齡]
        , c.NATION_CODE AS [國籍]
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


-- 2. T_TXN_PS_FULL: 算出所有特徵工程 (此處先做 JOIN 並建立排序特徵)
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
                WHEN {{ is_inward_all('ATM_B3') }} 
                AND TXN_AMT % 1000 = 0 
                THEN 1 
                ELSE 0 
            END
          ) AS [累計匯款次數]
        , SUM(
            CASE 
                WHEN {{ is_inward_all('ATM_B3') }} 
                AND TXN_AMT % 1000 = 0 
                THEN TXN_AMT 
                ELSE 0 
            END
          ) AS [累計匯款金額]
        , SUM(
            CASE 
                WHEN {{ is_atm_outward_all('ATM_B3') }} 
                THEN 1 
                ELSE 0 
            END
          ) AS [累計提款次數]
        , SUM(
            CASE 
                WHEN {{ is_atm_outward_all('ATM_B3') }} 
                THEN TXN_AMT 
                ELSE 0 
            END
          ) AS [累計提款金額]
    FROM T_TXN_PS_AGG1
    GROUP BY ACT_NO
)

, T_TXN_PS_FULL AS (
    SELECT 
        a.ACT_NO
        , a.[職業]
        , a.[年齡]
        , a.[國籍]
        , a.[帳戶狀態]
        , a.[外籍]
        , ISNULL(m.[累計匯款金額], 0) AS [累計匯款金額]
        , ISNULL(m.[累計匯款次數], 0) AS [累計匯款次數]
        , ISNULL(m.[累計提款金額], 0) AS [累計提款金額]
        , ISNULL(m.[累計提款次數], 0) AS [累計提款次數]
        , ISNULL(m.[本日最後餘額], 0) AS [本日最後餘額]
        , ROUND(
            {{ safe_divide('m.[累計提款金額]', 'm.[累計匯款金額]', '0') }}
            , 2
          ) AS [出入帳比]
    FROM T_ACT_BASE a
    LEFT JOIN T_TXN_PS_AGG2 m 
        ON a.ACT_NO = m.ACT_NO
)


-- 3. T_TXN_PS_FINAL: 條件篩選
, T_TXN_PS_FINAL AS (
    SELECT *
    FROM T_TXN_PS_FULL
    WHERE [外籍] = 'Y'
      AND [出入帳比] BETWEEN 0.77 AND 2.00
      AND [累計提款金額] BETWEEN 81316 AND 100035
      AND [累計提款次數] BETWEEN 2 AND 8
      AND [累計匯款金額] BETWEEN 42133 AND 150933
      AND [累計匯款次數] BETWEEN 1 AND 10
      AND [本日最後餘額] <= 16208
)


-- ==================== 4. 最終輸出 ====================
SELECT
    f.ACT_NO AS [帳號]
    , f.[職業]
    , f.[年齡]
    , f.[國籍]
    , f.[帳戶狀態]
    , f.[累計匯款金額]
    , f.[累計匯款次數]
    , f.[累計提款金額]
    , f.[累計提款次數]
    , f.[本日最後餘額]
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL f
-- ORDER BY f.ACT_NO