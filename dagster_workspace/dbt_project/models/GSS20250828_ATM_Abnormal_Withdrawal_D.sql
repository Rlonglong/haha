/*
Created: 2026-04-10
Description: 特定條件異常交易清單產出 (高額快速匯轉出)
Change Log:
- 2026-05-22 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
- 2026-04-10 [REFACT] Refactor from python script
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_ATM_D', 
    tags=["ATM D", "daily_job"]
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
        , PBA_INT_CODE
        , STATUS_PBA_CODE
    FROM {{ ref('snp_dim_cust_act') }}
    WHERE NULLIF(LTRIM(RTRIM(ID)), '') IS NOT NULL
      AND NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
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
        , OCUP_CODE
        , CASE 
            WHEN BIRTH_DATE LIKE '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]' 
            THEN YEAR(GETDATE()) - CAST(SUBSTRING(BIRTH_DATE, 1, 4) AS INT)
            ELSE NULL 
        END AS [年齡]
    FROM {{ ref('snp_cust') }}
    WHERE NULLIF(LTRIM(RTRIM(ID)), '') IS NOT NULL
      AND TABLE_DATE = '{{ target_ym }}'
)

, T_ACT_BASE AS (
    SELECT 
        ps.ACT_NO
        , ps.PBA_INT_CODE
        , c.OCUP_CODE AS [職業代碼]
        , c.[年齡]
        , w.STATUS_PBA_CODE AS [帳戶狀態]
    FROM T_CUST_PS_BASE ps
    LEFT JOIN T_CUST_BASE c 
        ON ps.ID = c.ID 
        AND ps.ID_DUP_NO = c.ID_DUP_NO
    LEFT JOIN {{ source('database', 'T_CUST_WARNINGFLG') }} w 
        ON ps.ACT_NO = w.ACT_NO
        AND w.TABLE_DATE = '{{ target_ym }}'
)

, T_TXN_PS_BASE AS (
    SELECT
        CPU_DATE
        , TXN_TIME
        , Seq_ID
        , ACT_NO
        , FUNC_CODE
        , DR_FLG
        , CAST(TXN_AMT AS BIGINT) AS TXN_AMT
        , CAST(PS_BAL AS BIGINT) AS PS_BAL
        , CONVERT(
            DATETIME
            , CPU_DATE + ' ' + STUFF(STUFF(TXN_TIME, 5, 0, ':'), 3, 0, ':')
          ) AS DATETIME
    FROM {{ ref('txn_ps_net') }}
    WHERE NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
      AND NULLIF(LTRIM(RTRIM(DR_FLG)), '') IS NOT NULL
      AND TABLE_DATE = '{{ target_date }}'
)


-- 2. T_TXN_PS_FULL: 算出所有特徵工程 (此處先做 JOIN 並建立排序特徵)
, T_TXN_PS_AGG1 AS (
    SELECT 
        ACT_NO
        , CPU_DATE
        , TXN_TIME
        , PS_BAL
        , TXN_AMT
        , DR_FLG
        , DATETIME
        , FUNC_CODE
        , ROW_NUMBER() OVER(
            PARTITION BY ACT_NO 
            ORDER BY CPU_DATE ASC, TXN_TIME ASC
          ) AS RN_FIRST
        , ROW_NUMBER() OVER(
            PARTITION BY ACT_NO 
            ORDER BY CPU_DATE DESC, TXN_TIME DESC
          ) AS RN_LAST
    FROM T_TXN_PS_BASE
)

, T_TXN_PS_AGG2 AS (
    SELECT 
        ACT_NO
        , SUM(
            CASE 
                WHEN {{ is_inward_all('ATM_D') }} 
                THEN TXN_AMT 
                ELSE 0 
            END
          ) AS [當日累計入帳]
        , SUM(
            CASE 
                WHEN {{ is_atm_outward_all('ATM_D') }} 
                THEN TXN_AMT 
                ELSE 0 
            END
          ) AS [當日累計出帳]
        , MIN(
            CASE 
                WHEN {{ is_inward_all('ATM_D') }} 
                THEN DATETIME 
            END
          ) AS [初次入帳時間]
        , MIN(
            CASE 
                WHEN {{ is_atm_outward_all('ATM_D') }} 
                THEN DATETIME 
            END
          ) AS [初次出帳時間]
        , MAX(
            CASE 
                WHEN RN_LAST = 1 
                THEN PS_BAL 
            END
          ) AS [當日餘額]
        , MAX(
            CASE 
                WHEN RN_FIRST = 1 
                THEN CASE 
                    WHEN DR_FLG = '0' 
                    THEN PS_BAL - TXN_AMT 
                    ELSE PS_BAL + TXN_AMT 
                END
            END
          ) AS [前日餘額]
    FROM T_TXN_PS_AGG1
    GROUP BY ACT_NO
)

, T_TXN_PS_FULL AS (
    SELECT 
        a.ACT_NO
        , a.PBA_INT_CODE
        , a.[職業代碼]
        , a.[年齡]
        , a.[帳戶狀態]
        , m.[當日累計入帳]
        , m.[當日累計出帳]
        , ROUND({{ safe_divide('m.[當日累計出帳]', 'm.[當日累計入帳]', '0') }} * 100, 2) AS [提存比%]
        , ISNULL(m.[當日餘額], 0) AS [當日餘額]
        , ISNULL(m.[前日餘額], 0) AS [前日餘額]
        , CASE 
            WHEN m.[初次入帳時間] IS NOT NULL 
            AND m.[初次出帳時間] IS NOT NULL
            THEN ROUND(CAST(DATEDIFF(SECOND, m.[初次入帳時間], m.[初次出帳時間]) AS FLOAT) / 60.0, 2)
            ELSE NULL 
          END AS [初次匯入匯出時間差(分)]
    FROM T_ACT_BASE a
    INNER JOIN T_TXN_PS_AGG2 m 
        ON a.ACT_NO = m.ACT_NO
    WHERE ISNULL(PBA_INT_CODE, '') NOT IN ('3', '4', '6', '8')
)


-- 3. T_TXN_PS_FINAL: 條件篩選
, T_TXN_PS_FINAL AS (
    SELECT
        '{{ target_date }}' AS [CPU_DATE]
        , '' AS [身分證號]
        , [ACT_NO] AS [局帳號14碼]
        , [職業代碼]
        , [年齡]
        , [帳戶狀態]
        , [當日累計入帳]
        , [當日累計出帳]
        , [提存比%]
        , [當日餘額]
        , [前日餘額]
        , [初次匯入匯出時間差(分)]
    FROM T_TXN_PS_FULL
    WHERE [當日累計入帳] >= 95000
      AND [提存比%] >= 90
      AND (
          [當日餘額] <= 1000 
          OR [前日餘額] <= 1000
      )
      AND [初次匯入匯出時間差(分)] BETWEEN 0 AND 30
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    [CPU_DATE]
    , [身分證號]
    , [局帳號14碼]
    , [職業代碼]
    , [年齡]
    , [帳戶狀態]
    , [當日累計入帳]
    , [當日累計出帳]
    , [提存比%]
    , [當日餘額]
    , [前日餘額]
    , [初次匯入匯出時間差(分)]
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL;