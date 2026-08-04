/*
Created: 2026-05-29
Description: 追蹤風險IP使用帳戶 及 今日新增警示帳戶共享IP非警示帳號
Change Log:
- 2026-05-29 [REFACT] Refactor from python script, removing cancellation logic and twelve_to_fourteen conversion.
*/

{{ config(
    materialized='incremental',
    alias='mrt_RISKY_IP_TRACING', 
    tags=["RISK_IP", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}
{% set target_ym = target_date[:7] %}


-- 1. T_BASE 系列
WITH T_WARNING_BASE AS (
    SELECT 
        ACT_NO
        , STATUS_PBA_CODE AS [目前註記]
        , STATUS_ECTRL_FLG AS [衍伸管制]
        , [警示日期]
        , [警示標記]
    FROM {{ source('database', 'mrt_WARNINGFLG_BONUS') }}
    WHERE TABLE_DATE = {{ target_date }}
      AND IS_VALID_ACT_FLG = 'Y'
)

, T_CUST_PS_BASE AS (
    SELECT 
        ID
        , ACT_NO
    FROM (
        SELECT 
            ID
            , ACT_NO
            , ROW_NUMBER() OVER(PARTITION BY ID ORDER BY OPEN_DATE DESC) AS RN
        FROM {{ source('database', 'T_CUST_PS') }}
        WHERE TABLE_DATE = '{{ target_ym }}'
          AND NULLIF(LTRIM(RTRIM(ID)), '') IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
          AND STATUS_PBA_CODE NOT IN {{ get_config()['RISKY_IP_TRACING']['exclude_pba_codes'] }}
    ) t
    WHERE RN = 1
)

, T_MPOST_BASE AS (
    SELECT 
        IP
        , ACT_NO
        , ID
        , LOG_REQ_DATE
        , IS_VALID_ACT_FLG
    FROM {{ source('database', 'T_MPOST_LOG') }}
    WHERE LOG_REQ_DATE = DATEADD(DAY, -1, CAST('{{ target_date }}' AS DATE))
      AND RESULT_FLG = '0'
      AND FUNC_CODE IN {{ get_config()['RISKY_IP_TRACING']['mpost_func_codes'] }}
      AND NULLIF(LTRIM(RTRIM(IP)), '') IS NOT NULL
)

, T_IPOST_BASE AS (
    SELECT 
        IP
        , ACT_NO
        , ID
        , LOG_REQ_DATE
        , IS_VALID_ACT_FLG
    FROM {{ source('database', 'T_IPOST_LOG') }}
    WHERE LOG_REQ_DATE = {{ target_date }}
      AND RESULT_FLG = '0'
      AND FUNC_CODE IN {{ get_config()['RISKY_IP_TRACING']['ipost_func_codes'] }}
      AND NULLIF(LTRIM(RTRIM(IP)), '') IS NOT NULL
)


-- 2. T_TXN_PS_FULL
, T_LOGS_MAPPED AS (
    SELECT 
        IP
        , CASE 
            WHEN IS_VALID_ACT_FLG = 'Y' 
            THEN ACT_NO 
            ELSE c.ACT_NO END 
          AS ACT_NO
        , CASE 
            WHEN IS_VALID_ACT_FLG = 'Y' 
            THEN 'Y' 
            ELSE ISNULL(c.IS_VALID_ACT_FLG, 'N') END 
          AS FINAL_VALID_FLG
        , LOG_REQ_DATE AS [使用日]
        , 'ipost' AS SOURCE_TYPE
    FROM T_IPOST_BASE i LEFT JOIN T_CUST_PS_BASE c ON i.ID = c.ID
    UNION ALL
    SELECT 
        IP
        , CASE 
            WHEN IS_VALID_ACT_FLG = 'Y' 
            THEN ACT_NO 
            ELSE c.ACT_NO 
          END AS ACT_NO
        , CASE 
            WHEN IS_VALID_ACT_FLG = 'Y' 
            THEN 'Y' 
            ELSE ISNULL(c.IS_VALID_ACT_FLG, 'N') END 
          AS FINAL_VALID_FLG
        , LOG_REQ_DATE AS [使用日]
        , 'app' AS SOURCE_TYPE
    FROM T_MPOST_BASE m LEFT JOIN T_CUST_PS_BASE c ON m.ID = c.ID
)

, T_IP_TRACE AS (
    -- [LOGIC FIX] 完全還原 Python: 讀取「昨日舊表 (h)」的數字，並保留 right join 漏檔缺陷，去重時機也照舊
    SELECT 
        m.IP
        , MAX(CASE WHEN m.SOURCE_TYPE = 'ipost' THEN 'Y' ELSE h.[ipost] END) AS [ipost]
        , MAX(CASE WHEN m.SOURCE_TYPE = 'app' THEN 'Y' ELSE h.[app] END) AS [app]
        , h.[警示] AS [累計警示]
        , h.[目前警示] AS [先前警示]
        , h.[目前終止] AS [先前終止]
        , h.[目前凍結] AS [先前凍結]
        , h.[目前管制] AS [先前管制]
        , h.[目前衍管] AS [先前衍管]
        , h.[操作帳號總數] AS [先前操作帳號總數]
        , m.ACT_NO AS [今日使用帳號]
        , m.[使用日]
        , ISNULL(w.[目前註記], '') AS [目前註記]
        , ISNULL(w.[衍伸管制], '') AS [衍伸管制]
        , ISNULL(w.[警示日期], '') AS [警示日期]
        , ISNULL(w.[警示標記], '') AS [警示標記]
    FROM T_LOGS_MAPPED m
    INNER JOIN {{ source('database', 'mrt_RISKY_IP') }} h 
        ON m.IP = h.IP
    LEFT JOIN T_WARNING_BASE w 
        ON m.ACT_NO = w.ACT_NO
    WHERE m.ACT_NO IS NOT NULL
      AND m.FINAL_VALID_FLG = 'Y'
    GROUP BY m.IP, h.[警示], h.[目前警示], h.[目前終止], h.[目前凍結], h.[目前管制], h.[目前衍管], h.[操作帳號總數], m.ACT_NO, m.[使用日], w.[目前註記], w.[衍伸管制], w.[警示日期], w.[警示標記]
)

, T_NEW_NORMAL_ACT AS (
    -- 對應 Python 的 df_new1_normal
    SELECT 
        IP
        , ACT_NO
        , LAST_SHARE_DATE
        , ISNULL(w.[警示標記], '') AS [警示標記]
    FROM {{ source('database', 'mrt_WARNINGFLG_SHARE_IP') }} n
    LEFT JOIN T_WARNING_BASE w 
        ON n.ACT_NO = w.ACT_NO
    WHERE TABLE_DATE = {{ target_date }}
      AND ISNULL(w.[警示標記], '') != 'B'
)


-- 3. T_TXN_PS_FINAL
, T_TXN_PS_FINAL AS (
    SELECT 
        IP
        , [ipost]
        , [app]
        , [累計警示]
        , [先前警示]
        , [先前終止]
        , [先前凍結]
        , [先前管制]
        , [先前衍管]
        , [先前操作帳號總數]
        , [今日使用帳號]
        , [使用日]
        , [目前註記]
        , [衍伸管制]
        , [警示日期]
        , [警示標記]
        , 1 AS IS_TRACE_LIST
        , 0 AS IS_NEW_SHARE_NORMAL
    FROM T_IP_TRACE

    UNION ALL

    SELECT 
        IP
        , '' AS [ipost]
        , '' AS [app]
        , 0 AS [累計警示]
        , 0 AS [先前警示]
        , 0 AS [先前終止]
        , 0 AS [先前凍結]
        , 0 AS [先前管制]
        , 0 AS [先前衍管]
        , 0 AS [先前操作帳號總數]
        , ACT_NO AS [今日使用帳號]
        , LAST_SHARE_DATE AS [使用日]
        , '' AS [目前註記]
        , '' AS [衍伸管制]
        , '' AS [警示日期]
        , [警示標記]
        , 0 AS IS_TRACE_LIST
        , 1 AS IS_NEW_SHARE_NORMAL
    FROM T_NEW_NORMAL_ACT
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    IP
    , [ipost]
    , [app]
    , [累計警示]
    , [先前警示]
    , [先前終止]
    , [先前凍結]
    , [先前管制]
    , [先前衍管]
    , [先前操作帳號總數]
    , [今日使用帳號]
    , [使用日]
    , [目前註記]
    , [衍伸管制]
    , [警示日期]
    , [警示標記]
    , IS_TRACE_LIST
    , IS_NEW_SHARE_NORMAL
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL
-- ORDER BY [先前操作帳號總數] DESC, IP DESC;