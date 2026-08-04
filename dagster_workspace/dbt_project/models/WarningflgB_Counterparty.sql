/*
Created: 2026-05-29
Description: 警示帳戶180日轉帳交易對象(限郵局)
Change Log:
- 2026-05-29 [REFACT] Refactor from python script. Switch data source from derived warning TXN table to T_TXN_PS to fix the 180-day time window coverage issue.
*/

{{ config(
    materialized='incremental',
    alias='mrt_WARNINGFLG_B_COUNTERPARTY', 
    tags=["RISK_COUNTERPARTY", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}


-- 1. T_BASE 系列
WITH T_WARNING_BASE AS (
    SELECT 
        ACT_NO
        , CASE 
            WHEN STATUS_ECTRL_FLG = 'Y' THEN ISNULL(STATUS_PBA_CODE, '') + '(衍管)'
            ELSE STATUS_PBA_CODE 
          END AS [帳戶狀態]
        , [警示日期]
        , CASE 
            WHEN ISNULL([解除警示日期], '') > ISNULL([警示日期], '') THEN NULL 
            ELSE [警示標記] 
          END AS [警示標記]
    FROM {{ source('database', 'mrt_WARNINGFLG_B_IPDEVICE') }}
    WHERE TABLE_DATE = CAST('{{ target_date }}' AS DATE)
      AND IS_VALID_ACT_FLG = 'Y'
)

, T_TXN_PS_BASE AS (
    SELECT 
        ACT_NO
        , LTRIM(RTRIM(OWN_TRANS_ACCT)) AS OWN_TRANS_ACCT
    FROM {{ ref('txn_ps_net') }}
    WHERE CPU_DATE >= DATEADD(DAY, -179, CAST('{{ target_date }}' AS DATE))
      AND CPU_DATE <= {{ target_date }}
      AND (BNK_ID = '700' OR BNK_ID = '   ')
      AND NULLIF(LTRIM(RTRIM(OWN_TRANS_ACCT)), '') IS NOT NULL
      AND (
          {{ is_inward_all('WARNINGFLG_B_COUNTERPARTY') }}
            OR {{ is_outward_all('WARNINGFLG_B_COUNTERPARTY') }}
      )
)


-- 2. T_TXN_PS_FULL
, T_B_TXN_FILTERED AS (
    SELECT 
        t.OWN_TRANS_ACCT
        , t.ACT_NO AS [警示帳戶]
    FROM T_TXN_PS_BASE t
    INNER JOIN T_WARNING_BASE w 
        ON t.ACT_NO = w.ACT_NO
    WHERE w.[警示標記] = 'B'
)

, T_ACT_FORMATTED AS (
    SELECT DISTINCT
        [警示帳戶]
        , OWN_TRANS_ACCT AS ACT_NO
    FROM T_B_TXN_FILTERED
)

, T_COUNTERPARTY_AGG AS (
    SELECT 
        ACT_NO
        , COUNT([警示帳戶]) AS [交易對象警示帳戶數]
    FROM T_ACT_FORMATTED
    GROUP BY ACT_NO
)


-- 3. T_TXN_PS_FINAL
, T_TXN_PS_FINAL AS (
    SELECT 
        c.ACT_NO
        , c.[交易對象警示帳戶數]
        , w.[帳戶狀態]
        , w.[警示標記]
        , w.[警示日期]
    FROM T_COUNTERPARTY_AGG c
    LEFT JOIN T_WARNING_BASE w 
        ON c.ACT_NO = w.ACT_NO
    WHERE c.[交易對象警示帳戶數] >= 2
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    ACT_NO
    , [交易對象警示帳戶數]
    , [帳戶狀態]
    , [警示標記]
    , [警示日期]
    , CAST('{{ target_date }}' AS DATE) AS TABLE_DATE
FROM T_TXN_PS_FINAL
-- ORDER BY [交易對象警示帳戶數] DESC;