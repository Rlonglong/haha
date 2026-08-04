/*
Created: 2026-05-23
Description: 中信通報名單產出 (當日新增警示/衍生管制戶，近30日轉出至中信之對手帳號)
Change Log:
- 2026-05-23 [REFACT] Refactor from python script. 取代 Python 的 drop_duplicates，並直接產出電子科所需之字串格式。
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_CTBC_OUTPUT', 
    tags=["CTBC_OUTPUT", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}

-- 1. T_BASE 系列: 保持乾淨的 SELECT，進行初步過濾與型別轉換
WITH T_CUST_CHG_BASE AS (
    SELECT DISTINCT 
        ACT_NO
    FROM {{ source('database', 'T_CUST_CHG') }}
    WHERE CAST(TXN_DATE AS DATE) = CAST('{{ target_date }}' AS DATE)
      AND FUNC_CODE IN {{ get_config()['CTBC_OUTPUT']['warn_func_codes'] }}
      AND ISNULL(TXM_MEMO, '') <> 'RELEASE'
)

, T_TXN_PS_BASE AS (
    
    SELECT 
        ACT_NO
        , LTRIM(RTRIM(BNK_ID)) AS BNK_ID
        , LTRIM(RTRIM(OWN_TRANS_ACCT)) AS OWN_TRANS_ACCT
    FROM {{ ref('txn_ps_net') }}
    WHERE TABLE_DATE >= REPLACE(CAST(DATEADD(DAY, -29, CAST('{{ target_date }}' AS DATE)) AS VARCHAR(10)), '-', '')
      AND TABLE_DATE <= REPLACE(CAST('{{ target_date }}' AS DATE), '-', '')
      AND DR_FLG = '1'
      AND LTRIM(RTRIM(BNK_ID)) = '822'
)


-- 2. T_TXN_PS_FULL: 算出所有特徵工程 (兩表交集)
, T_TXN_PS_FULL AS (
    SELECT 
        t.BNK_ID
        , t.OWN_TRANS_ACCT
    FROM T_TXN_PS_BASE t
    INNER JOIN T_CUST_CHG_BASE c 
        ON t.ACT_NO = c.ACT_NO
)


-- 3. T_TXN_PS_FINAL: 條件篩選與去重複
, T_TXN_PS_FINAL AS (
    SELECT DISTINCT
        BNK_ID
        , OWN_TRANS_ACCT
    FROM T_TXN_PS_FULL
    WHERE NULLIF(OWN_TRANS_ACCT, '') IS NOT NULL
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    [BNK_ID]
    , [OWN_TRANS_ACCT]
    -- 直接產出電子科指定的格式：BNK_ID + 4個空白 + OWN_TRANS_ACCT
    , [BNK_ID] + '    ' + [OWN_TRANS_ACCT] AS [FORMATTED_TXT]
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL
-- ORDER BY [OWN_TRANS_ACCT];