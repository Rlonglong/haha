/*
Created: 2026-05-27
Description: 新增警示帳戶之90日交易明細檔
Change Log:
- 2026-05-27 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_WARNINGFLG_B_TXN', 
    tags=["WARNING_TXN", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定 (90天區間: T-89 ~ T)
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}


-- 1. T_BASE 系列: 保持乾淨的 SELECT，進行初步過濾與型別轉換
WITH T_WARNING_BASE AS (
    -- 找出昨天剛變成警示戶的帳號 (確保去重複)
    SELECT DISTINCT 
        ACT_NO
    FROM {{ source('database', 'mrt_WARNINGFLG_BONUS') }}
    WHERE TABLE_DATE = '{{ target_date }}'
      AND [警示日期] = DATEADD(DAY, -1, CAST('{{ target_date }}' AS DATE))
      AND NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
)

, T_TXN_PS_BASE AS (
    -- 取出近 90 天內的所有交易
    SELECT 
        CPU_DATE
        , TXN_TIME
        , ACT_NO
        , TXN_NAME
        , FUNC_CODE
        , CHANNEL_FLG
        , DR_FLG
        , CAST(TXN_AMT AS BIGINT) AS TXN_AMT
        , CAST(PS_BAL AS FLOAT) AS PS_BAL
        , TXN_TYPE
        , TXN_BRH
        , OWN_TRANS_ACCT
        , BNK_ID
    FROM {{ ref('txn_ps_net') }}
    WHERE CPU_DATE >= DATEADD(DAY, -89, CAST('{{ target_date }}' AS DATE))
      AND CPU_DATE <= '{{ target_date }}'
      AND NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
)


-- 2. T_TXN_PS_FULL: 算出所有特徵工程 (利用 INNER JOIN 精準篩選警示戶交易)
-- 3. T_TXN_PS_FINAL: 單純過濾與欄位收斂
, T_TXN_PS_FINAL AS (
    SELECT 
        t.CPU_DATE
        , t.TXN_TIME
        , t.ACT_NO
        , t.TXN_NAME
        , t.FUNC_CODE
        , t.CHANNEL_FLG
        , t.DR_FLG
        , t.TXN_AMT
        , t.PS_BAL
        , t.TXN_TYPE
        , t.TXN_BRH
        , t.OWN_TRANS_ACCT
        , t.BNK_ID
    FROM T_TXN_PS_BASE t
    INNER JOIN T_WARNING_BASE w 
        ON t.ACT_NO = w.ACT_NO
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    *
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL
{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1 FROM {{ this }} t
    WHERE t.ACT_NO = T_TXN_PS_FINAL.ACT_NO
      AND t.TABLE_DATE = '{{ target_date }}'
)
{% endif %}
-- ORDER BY ACT_NO, CPU_DATE, TXN_TIME;