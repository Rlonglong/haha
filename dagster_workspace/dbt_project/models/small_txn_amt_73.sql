/*
Created: 2026-03-13
Description: 
Change Log:
- 2026-05-23 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
- 2026-03-13 [REFACT] Refactor from python script
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_FOREIGN_SMALL_TXN', 
    tags=["FOREIGN_SMALL_TXN", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定 (使用 dbt var 以支援 Dagster 排程動態傳入)
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}
{% set target_ym = target_date[:7] %}

-- 1. T_BASE 系列: 保持乾淨的 SELECT，進行初步過濾與型別轉換
WITH T_CUST_BASE AS (
    SELECT
        ID
        , ID_TYPE
        , OCUP_CODE
    FROM {{ ref('snp_cust') }}
    WHERE NULLIF(LTRIM(RTRIM(ID)), '') IS NOT NULL
      AND OCUP_CODE = '73'
      AND TABLE_DATE = '{{ target_ym }}'
)

, T_CUST_PS_BASE AS (
    SELECT 
        ACT_NO
        , ID
    FROM {{ ref('snp_dim_cust_act') }}
    WHERE NULLIF(LTRIM(RTRIM(ID)), '') IS NOT NULL
    {% if target_date == run_started_at.strftime("%Y-%m-%d") %}
      AND dbt_valid_to IS NULL
    {% else %}
      AND CAST('{{ target_date }}' AS DATETIME) >= dbt_valid_from 
      AND CAST('{{ target_date }}' AS DATETIME) < ISNULL(dbt_valid_to, '9999-12-31')
    {% endif %}
)

, T_ACT_BASE AS (
    SELECT 
        ps.ACT_NO
    FROM T_CUST_PS_BASE ps
    INNER JOIN T_CUST_BASE c 
        ON ps.ID = c.ID
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
        , ACCT_NBR_ORI
        , CONVERT(
            DATETIME
            , CONVERT(VARCHAR(10), CPU_DATE, 120) + ' ' + STUFF(STUFF(TXN_TIME, 5, 0, ':'), 3, 0, ':')
        )
    FROM {{ ref('txn_ps_net') }}
    WHERE NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
      AND NULLIF(LTRIM(RTRIM(DR_FLG)), '') IS NOT NULL
      AND FUNC_CODE IN {{ get_config()['FOREIGN_SMALL']['func_codes'] }}
      AND CPU_DATE = '{{ target_date }}'
)


-- 2. T_TXN_PS_FULL: 算出所有特徵工程 (此處先做 JOIN 並建立排序特徵)
, T_TXN_PS_AGG1 AS (
    SELECT 
        t.ACT_NO
        , t.CPU_DATE
        , t.TXN_TIME
        , t.Seq_ID
        , t.DATETIME
        , t.TXN_AMT
        , t.DR_FLG
        , t.ACCT_NBR_ORI
    FROM T_ACT_BASE a
    INNER JOIN T_TXN_PS_BASE t 
        ON a.ACT_NO = t.ACT_NO
)

, T_TXN_PS_FULL AS (
    SELECT 
        *
        , LAG(DR_FLG) OVER(
            PARTITION BY ACT_NO 
            ORDER BY CPU_DATE ASC, TXN_TIME ASC
          ) AS PREV_DRFLG
        , LAG(DATETIME) OVER(
            PARTITION BY ACT_NO 
            ORDER BY CPU_DATE ASC, TXN_TIME ASC
          ) AS PREV_DATETIME
        , LAG(TXN_AMT) OVER(
            PARTITION BY ACT_NO 
            ORDER BY CPU_DATE ASC, TXN_TIME ASC
          ) AS PREV_TXNAMT
    FROM T_TXN_PS_AGG1
)


-- 3. T_TXN_PS_FINAL: 條件篩選
, T_TXN_PS_FINAL AS (
    SELECT DISTINCT 
        ACCT_NBR_ORI AS [ACT_NO]
    FROM T_TXN_PS_FULL
    WHERE DR_FLG = '1'
      AND PREV_DRFLG = '0'
      AND TXN_AMT <= 4000
      AND PREV_TXNAMT <= 4000
      AND DATEDIFF(MINUTE, PREV_DATETIME, DATETIME) <= 30
)


-- ==================== 4. 最終輸出 ====================
SELECT ACT_NO
FROM T_TXN_PS_FINAL
-- ORDER BY ACT_NO;