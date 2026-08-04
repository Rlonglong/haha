/*
Created: 2026-05-23
Description: 警示戶共用IP名單產出 (近30日同 IP 且同轉入帳戶之交易紀錄追蹤)
Change Log:
- 2026-05-23 [REFACT] Refactor from python script.
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_HIGH_RISK_IP', 
    tags=["HIGH_RISK_IP", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}
{% set start_date_30 = (modules.datetime.datetime.strptime(target_date, '%Y-%m-%d') - modules.datetime.timedelta(days=30)).strftime('%Y-%m-%d') %}

WITH T_ALL_TRANS AS (
    -- 聯集三個通路的成功轉出交易，並附上附言欄位
    SELECT ACT_NO, LOG_REQ_DATE, LOG_REQ_TIME, CAST(TXN_AMT AS BIGINT) AS TXN_AMT, IP, TRNS_IN_BANK, TRNS_IN_ACT, COMMENT_SELF, COMMENT_OUT 
    FROM {{ source('database', 'T_IWEBATM') }}
    WHERE LOG_REQ_DATE >= REPLACE('{{ start_date_30 }}', '-', '') AND RESULT_FLG = '0' AND CAST(TXN_AMT AS BIGINT) > 0
    UNION ALL
    SELECT ACT_NO, LOG_REQ_DATE, LOG_REQ_TIME, CAST(TXN_AMT AS BIGINT) AS TXN_AMT, IP, TRNS_IN_BANK, TRNS_IN_ACT, COMMENT_SELF, COMMENT_OUT 
    FROM {{ source('database', 'T_IPOST') }}
    WHERE LOG_REQ_DATE >= REPLACE('{{ start_date_30 }}', '-', '') AND RESULT_FLG = '0' AND CAST(TXN_AMT AS BIGINT) > 0
    UNION ALL
    SELECT ACT_NO, LOG_REQ_DATE, LOG_REQ_TIME, CAST(TXN_AMT AS BIGINT) AS TXN_AMT, IP, TRNS_IN_BANK, TRNS_IN_ACT, COMMENT_SELF, COMMENT_OUT 
    FROM {{ source('database', 'T_MPOST') }}
    WHERE LOG_REQ_DATE >= REPLACE('{{ start_date_30 }}', '-', '') AND RESULT_FLG = '0' AND CAST(TXN_AMT AS BIGINT) > 0
)

-- 取得我們已經建立好的黑名單表
, T_BLACK_IP_LIST AS (
    SELECT DISTINCT 
        IP
        , TRNS_IN_BANK
        , TRNS_IN_ACT
        , 警示帳號
        , 警示日期
    FROM {{ ref('mrt_BLACK_IP_LIST') }}
)

-- 進行比對 (交集)
, MATCHED_TRANS AS (
    SELECT 
        t.ACT_NO
        , t.LOG_REQ_DATE
        , t.LOG_REQ_TIME
        , t.TXN_AMT
        , t.IP
        , t.TRNS_IN_BANK
        , t.TRNS_IN_ACT
        , t.COMMENT_SELF
        , t.COMMENT_OUT
        , b.[警示帳號]
        , b.[警示日期]
    FROM T_ALL_TRANS t
    INNER JOIN T_BLACK_IP_LIST b 
        ON t.IP = b.IP 
       AND t.TRNS_IN_BANK = b.TRNS_IN_BANK 
       AND t.TRNS_IN_ACT = b.TRNS_IN_ACT
    WHERE NULLIF(LTRIM(RTRIM(t.ACT_NO)), '') IS NOT NULL
)

-- 計算共用該 (IP+轉出對象) 的不重複帳號數 (對應 Python: transform('nunique') >= 2)
, COUNT_MATCHED AS (
    SELECT 
        *
        , COUNT(DISTINCT ACT_NO) OVER(PARTITION BY IP, TRNS_IN_BANK, TRNS_IN_ACT) AS UNIQUE_ACT_CNT
    FROM MATCHED_TRANS
)

, FINAL_REPORT AS (
    SELECT 
        ACT_NO AS [交易帳號]
        , LOG_REQ_DATE AS [交易日期]
        , LOG_REQ_TIME AS [交易時間]
        , TXN_AMT AS [交易金額]
        , IP AS [IP]
        , TRNS_IN_BANK AS [轉入行庫]
        , TRNS_IN_ACT AS [轉入帳號]
        , ISNULL(COMMENT_SELF, '') AS [附言(給自己)]
        , ISNULL(COMMENT_OUT, '') AS [附言(給對方)]
        , [警示帳號]
        , [警示日期]
    FROM COUNT_MATCHED
    WHERE UNIQUE_ACT_CNT >= 2
)

-- ==================== 最終輸出 ====================
SELECT 
    [交易帳號]
    , [交易日期]
    , [交易時間]
    , [交易金額]
    , IP
    , [轉入行庫]
    , [轉入帳號]
    , [附言(給自己)]
    , [附言(給對方)]
    , [警示帳號]
    , [警示日期]
    , '{{ target_date }}' AS TABLE_DATE
FROM FINAL_REPORT
-- ORDER BY IP, [轉入帳號], [交易帳號], [交易日期], [交易時間];