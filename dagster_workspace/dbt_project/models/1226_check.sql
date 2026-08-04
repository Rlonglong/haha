/*
Created: 2026-03-11
Description: 1226 交易檢核 (低收入戶與特定交易)
Change Log:
- 2026-05-22 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
- 2026-03-11 [REFACT] Refactor from python script
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_1226_check', 
    tags=["1226_check", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定 (使用 dbt var 以支援 Dagster 排程動態傳入)
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}
{% set target_ym = target_date[:7] %}


-- 1. T_BASE 系列: 把所有表需要用到的欄位先 select 進來，並嚴格加上對應的日期分區限制
WITH T_CUST_BASE AS (
    SELECT
        ID
        , ID_DUP_NO
        , OCUP_CODE
        , NATION_CODE
    FROM {{ source('database', 'T_CUST') }}
    WHERE TABLE_DATE = '{{ target_ym }}'
)

, T_CUST_PS_BASE AS (
    SELECT 
        ACT_NO
        , ID
        , ID_DUP_NO
        -- , PBA_INT_CODE
        , STATUS_PBA_CODE
    FROM {{ ref('snp_dim_cust_act') }}
    WHERE 1=1
    {% if target_date == run_started_at.strftime("%Y-%m-%d") %}
      AND dbt_valid_to IS NULL
    {% else %}
      AND CAST('{{ target_date }}' AS DATETIME) >= dbt_valid_from 
      AND CAST('{{ target_date }}' AS DATETIME) < ISNULL(dbt_valid_to, '9999-12-31')
    {% endif %}
)

, T_LOW_INCOME_BASE AS (
    SELECT
        ACCT_NBR_ORI
    FROM {{ source('database', 'T_LOW_INCOME') }}
    WHERE TABLE_DATE = '{{ target_date }}'
)

, T_TXN_PS_BASE AS (
    SELECT 
        ACT_NO
        , CPU_DATE
        , TXN_TIME
        , Seq_ID
        , FUNC_CODE
        , TXN_AMT
        , PS_BAL
        , TXN_MEMO
    FROM {{ ref('txn_ps_net') }}
    WHERE LTRIM(RTRIM(TXN_MEMO)) = 'A' 
      AND FUNC_CODE = '1226' 
      AND TXN_AMT = 1
      AND CPU_DATE = '{{ target_date }}'
)


-- 2. T_TXN_PS_FULL: 算出所有特徵工程 (此處先做 JOIN 並建立排序特徵)
, T_TXN_PS_AGG1 AS (
    SELECT 
        a.ACT_NO
        , a.CPU_DATE
        , a.TXN_TIME
        , a.Seq_ID
        , a.FUNC_CODE
        , a.TXN_AMT
        , a.PS_BAL
        , a.TXN_MEMO
        , c.OCUP_CODE
        , c.NATION_CODE
        , CASE 
            WHEN d.ACCT_NBR_ORI IS NOT NULL 
            THEN 1
            ELSE 0
          END AS [是否為低收入戶]
    FROM T_TXN_PS_BASE a
    LEFT JOIN T_CUST_PS_BASE b
        ON a.ACT_NO = b.ACT_NO
    LEFT JOIN T_CUST_BASE c
        ON b.ID = c.ID
        AND b.ID_DUP_NO = c.ID_DUP_NO
    LEFT JOIN T_LOW_INCOME_BASE d
        ON a.ACT_NO = d.ACCT_NBR_ORI
)

, T_TXN_PS_FULL AS (
    SELECT 
        *
        -- 建立分組排序，以便在 FINAL 直接抓取第一筆
        , ROW_NUMBER() OVER(
            PARTITION BY ACT_NO 
            ORDER BY CPU_DATE ASC, TXN_TIME ASC
          ) AS RN_FIRST
    FROM T_TXN_PS_AGG1
)


-- 3. T_TXN_PS_FINAL: 條件篩選
, T_TXN_PS_FINAL AS (
    SELECT
        FUNC_CODE
        , ACT_NO
        , OCUP_CODE
        , NATION_CODE
        , TXN_AMT
        , PS_BAL
        , TXN_MEMO
        , [是否為低收入戶]
    FROM T_TXN_PS_FULL
    WHERE RN_FIRST = 1
      AND (
          PS_BAL <= 3000
          OR (
              PS_BAL > 3000 
              AND (
                  OCUP_CODE = '73' 
                  OR (
                      OCUP_CODE = '50' 
                      AND ISNULL(NATION_CODE, '') <> '  ' 
                      AND ISNULL(NATION_CODE, '') <> 'TW'
                  )
                  OR [是否為低收入戶] = 1
              )
          )
      )
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    {{ target_date }} AS table_date
    , FUNC_CODE
    , ACT_NO
    , OCUP_CODE
    , NATION_CODE
    , TXN_AMT
    , PS_BAL
    , TXN_MEMO
    , [是否為低收入戶]
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL;