/*
Created: 2026-05-23
Description: 高風險ip交易.sql名單前處理: IP黑名單累積檔 (收錄警示戶曾使用過之 IP + 轉入行庫 + 轉入帳號)
Change Log:
- 2026-05-23 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_BLACK_IP_LIST', 
    unique_key=['警示帳號', 'IP', 'TRNS_IN_BANK', 'TRNS_IN_ACT'],
    tags=["HIGH_RISK_IP", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}
-- 因為只需要比對新變成警示的帳戶過去 30 天的交易，所以只往前抓 30 天
{% set start_date_30 = (modules.datetime.datetime.strptime(target_date, '%Y-%m-%d') - modules.datetime.timedelta(days=30)).strftime('%Y-%m-%d') %}

WITH T_ALL_TRANS AS (
    -- 聯集三個通路的成功轉出交易 (IWEBATM, IPOST, MPOST)
    SELECT ACT_NO, IP, TRNS_IN_BANK, TRNS_IN_ACT 
    FROM {{ source('database', 'T_IWEBATM') }}
    WHERE LOG_REQ_DATE >= REPLACE('{{ start_date_30 }}', '-', '') AND RESULT_FLG = '0' AND CAST(TXN_AMT AS BIGINT) > 0
    UNION
    SELECT ACT_NO, IP, TRNS_IN_BANK, TRNS_IN_ACT 
    FROM {{ source('database', 'T_IPOST') }}
    WHERE LOG_REQ_DATE >= REPLACE('{{ start_date_30 }}', '-', '') AND RESULT_FLG = '0' AND CAST(TXN_AMT AS BIGINT) > 0
    UNION
    SELECT ACT_NO, IP, TRNS_IN_BANK, TRNS_IN_ACT 
    FROM {{ source('database', 'T_MPOST') }}
    WHERE LOG_REQ_DATE >= REPLACE('{{ start_date_30 }}', '-', '') AND RESULT_FLG = '0' AND CAST(TXN_AMT AS BIGINT) > 0
)

, T_WARNFLG AS (
    SELECT 
        ACT_NO
        , TABLE_DATE AS [警示日期]
        , WARN_MARK AS [警示標記]
    FROM {{ source('database', 'T_CUST_WARNINGFLG') }}
    WHERE TABLE_DATE = '{{ target_date }}' AND WARN_MARK = 'B'
)

, NEW_BLACK_IP AS (
    SELECT DISTINCT
        w.ACT_NO AS [警示帳號]
        , t.IP AS IP
        , t.TRNS_IN_BANK AS TRNS_IN_BANK
        , t.TRNS_IN_ACT AS TRNS_IN_ACT
        , w.[警示標記]
        , w.[警示日期]
    FROM T_WARNFLG w
    INNER JOIN T_ALL_TRANS t ON w.ACT_NO = t.ACT_NO
    WHERE t.IP <> '127.0.0.1' 
      AND NULLIF(LTRIM(RTRIM(t.IP)), '') IS NOT NULL
)

SELECT * FROM NEW_BLACK_IP


{% if is_incremental() %}
  -- 增量寫入：只塞入尚未存在於黑名單庫的紀錄
  WHERE NOT EXISTS (
      SELECT 1 FROM {{ this }} old
      WHERE NEW_BLACK_IP.[警示帳號] = old.[警示帳號]
        AND NEW_BLACK_IP.IP = old.IP
        AND NEW_BLACK_IP.TRNS_IN_BANK = old.TRNS_IN_BANK
        AND NEW_BLACK_IP.TRNS_IN_ACT = old.TRNS_IN_ACT
  )
{% endif %}