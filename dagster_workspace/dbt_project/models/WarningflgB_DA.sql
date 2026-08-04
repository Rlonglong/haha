/*
Created: 2026-05-29
Description: 警示帳戶180日約轉檔
Change Log:
- 2026-05-29 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
*/

{{ config(
    materialized='incremental',
    alias='mrt_WARNINGFLG_180D_DA', 
    tags=["RISK_DA", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}

-- 1. T_BASE 系列: 保持乾淨的 SELECT，進行初步過濾與型別轉換
WITH T_WARNING_BASE AS (
    SELECT 
        ACT_NO
        , [警示日期]
        , [警示標記]
    FROM {{ source('database', 'mrt_WARNINGFLG_B_IPDEVICE') }}
    WHERE TABLE_DATE = CAST('{{ target_date }}' AS DATE)
)

, T_CUST_DA_BASE AS (
    SELECT 
        ACT_NO_OUT
        , LTRIM(RTRIM(ACT_NO_IN)) AS ACT_NO_IN
        , ORI_REQ_DATE
    FROM {{ source('database', 'T_CUST_DA') }}
    WHERE TABLE_DATE >= DATEADD(DAY, -179, CAST('{{ target_date }}' AS DATE))
      AND TABLE_DATE <= CAST('{{ target_date }}' AS DATE)
      AND APPLY_FLG = '0'
      AND BANK_NO = '700'
      AND NOT IS_GIRO_ACT_FLG
)


-- 2. T_TXN_PS_FULL: 算出所有特徵工程
, T_DA_OUT AS (
    -- 180日內經警示帳戶設定為約轉帳號 (來源端為警示帳戶)
    SELECT 
        d.ACT_NO_IN AS ACT_NO
        , d.ORI_REQ_DATE AS [設定日期]
        , '180日內經警示帳戶設定為約轉帳號' AS NOTE
    FROM T_CUST_DA_BASE d
    INNER JOIN T_WARNING_BASE w 
        ON d.ACT_NO_OUT = w.ACT_NO
    WHERE w.[警示標記] = 'B'
)

, T_DA_IN AS (
    -- 180日內設定警示帳戶為約轉帳號 (目的端為警示帳戶)
    SELECT 
        d.ACT_NO_OUT AS ACT_NO
        , d.ORI_REQ_DATE AS [設定日期]
        , '180日內設定警示帳戶為約轉帳號' AS NOTE
    FROM T_CUST_DA_BASE d
    INNER JOIN T_WARNING_BASE w 
        ON d.ACT_NO_IN = w.ACT_NO
    WHERE w.[警示標記] = 'B'
)

, T_DA_COMBINED AS (
    -- 取代 Python 的 pd.concat
    SELECT * FROM T_DA_OUT
    UNION ALL
    SELECT * FROM T_DA_IN
)


-- 3. T_TXN_PS_FINAL: 條件篩選
, T_TXN_PS_FINAL AS (
    SELECT 
        ACT_NO
        , [設定日期]
        , [警示標記]
        , [警示日期]
        , NOTE
        , CASE 
            WHEN [警示日期] = CONVERT(VARCHAR(8), DATEADD(DAY, -1, CAST('{{ target_date }}' AS DATE)), 112) 
            THEN 1 ELSE 0 
          END AS IS_NEW_WARNING_TODAY
    FROM T_DA_COMBINED
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    ACT_NO
    , [設定日期]
    , [警示標記]
    , [警示日期]
    , NOTE
    , IS_NEW_WARNING_TODAY
    , CAST('{{ target_date }}' AS DATE) AS TABLE_DATE
FROM T_TXN_PS_FINAL;