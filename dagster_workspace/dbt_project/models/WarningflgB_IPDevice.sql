/*
Created: 2026-05-27
Description: 警示帳戶 90 日內 IP、設備溯源與同日共用追蹤 (統一產出寬表)
Change Log:
- 2026-05-27 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_WARNINGFLG_B_IPDEVICE', 
    tags=["RISK_TRACEBACK", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定 (90天區間: T-89 ~ T)
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}
{% set target_ym = target_date[:7] %}

-- 1. T_BASE 系列: 保持乾淨的 SELECT，進行初步過濾與型別轉換
WITH T_WARNING_BASE AS (
    -- 找出昨天剛變成警示戶的帳號
    SELECT 
        ACT_NO
        , IS_VALID_ACT_FLG
        , STATUS_PBA_CODE
        , WARN_MARK
    FROM {{ source('database', 'mrt_WARNINGFLG_BONUS') }}
    WHERE TABLE_DATE = {{ target_date}}
      AND [警示日期] = DATEADD(DAY, -1, CAST('{{ target_date }}' AS DATE))
      AND NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
      AND IS_VALID_ACT_FLG = 'Y'
)

, T_CUST_PS_BASE AS (
    -- ID 反查帳戶字典檔 (防堵笛卡爾積：確保 1 個 ID 只對應 1 個最新活躍帳戶)
    SELECT ID, ACT_NO
    FROM (
        SELECT 
            ID
            , ACT_NO
            , ROW_NUMBER() OVER(PARTITION BY ID ORDER BY LAST_TX_DATE DESC) AS RN
        FROM {{ source('database', 'T_CUST_PS') }}
        WHERE TABLE_DATE = '{{ target_ym }}'
          AND NULLIF(LTRIM(RTRIM(ID)), '') IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
          AND NOT (STATUS_PBA_CODE IN {{ get_config()['WARNINGFLG_B_IPDEVICE']['exclude_pba_codes'] }}
                                   AND LAST_TX_DATE <= DATEADD(DAY, -89, CAST('{{ target_date }}' AS DATE))
        ) t
    )
    WHERE RN = 1
)

, T_LOG_BASE AS (
    -- 聯集 90 天內的 MPOST 與 IPOST 成功日誌
    SELECT 
        LOG_REQ_DATE
        , ACT_NO
        , IS_VALID_ACT_FLG
        , ID
        , IP
        , DEVICE_ID
        , 'app' AS SOURCE
    FROM {{ source('database', 'T_MPOST_LOG') }}
    WHERE LOG_REQ_DATE BETWEEN DATEADD(DAY, -89, CAST('{{ target_date }}' AS DATE)) AND {{ target_date}}
      AND RESULT_FLG = '0' 
      AND FUNC_CODE IN {{ get_config()['WARNINGFLG_B_IPDEVICE']['mpost_func_codes'] }}

    UNION ALL

    SELECT 
        LOG_REQ_DATE
        , ACT_NO
        , IS_VALID_ACT_FLG
        , ID
        , IP
        , NULL AS DEVICE_ID
        , 'ipost' AS SOURCE
    FROM {{ source('database', 'T_IPOST_LOG') }}
    WHERE LOG_REQ_DATE BETWEEN DATEADD(DAY, -89, CAST('{{ target_date }}' AS DATE)) AND {{ target_date}}
      AND RESULT_FLG = '0' 
      AND FUNC_CODE IN {{ get_config()['WARNINGFLG_B_IPDEVICE']['ipost_func_codes'] }}
      AND NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
      AND IS_VALID_ACT_FLG = 'Y'
)


-- 2. T_TXN_PS_FULL: 算出所有特徵工程 (分拆為 目標IP、目標設備、共用IP)
, T_LOG_MAPPED AS (
    -- 補齊缺失的 ACT_NO
    SELECT 
        l.LOG_REQ_DATE
        , CASE
            WHEN l.IS_VALID_ACT_FLG = 'Y'
            THEN l.ACT_NO
            ELSE c.ACT_NO
          END AS ACT_NO
        , ISNULL(NULLIF(LTRIM(RTRIM(l.ACT_NO)), ''), c.ACT_NO) AS ACT_NO
        , l.IP
        , l.DEVICE_ID
        , l.SOURCE
    FROM T_LOG_BASE l
    LEFT JOIN T_CUST_PS_BASE c 
        ON l.ID = c.ID
)

, T_TARGET_B_LOGS AS (
    -- 篩選出「警示戶」本身的日誌軌跡
    SELECT 
        m.* 
    FROM T_LOG_MAPPED m
    INNER JOIN T_WARNING_BASE w 
        ON m.ACT_NO = w.ACT_NO
)

, T_B_IP AS (
    -- 特徵 A：警示戶使用的 IP (全面修復：支援完整 IP 與網段前綴比對，且防範 127.0.0.100 黏連漏洞)
    SELECT 
        l.ACT_NO
        , l.IP
        , MAX(l.LOG_REQ_DATE) AS LAST_USE_DATE
    FROM T_TARGET_B_LOGS l
    WHERE NOT EXISTS (
        SELECT 1 
        FROM {{ source('database', 'WHITE_IP') }} w
        WHERE l.IP = w.IP 
           OR l.IP LIKE w.IP + '.%'
    )
    GROUP BY l.ACT_NO, l.IP
)

, T_B_DEVICE AS (
    -- 特徵 B：警示戶使用的 Device (排除白名單，取最後使用日)
    SELECT 
        ACT_NO
        , DEVICE_ID
        , MAX(LOG_REQ_DATE) AS LAST_USE_DATE
    FROM T_TARGET_B_LOGS
    WHERE DEVICE_ID IS NOT NULL 
      AND DEVICE_ID NOT IN (SELECT DEVICE_ID FROM {{ source('database', 'WHITE_DEVICE') }})
    GROUP BY ACT_NO, DEVICE_ID
)

, T_SHARED_IP AS (
    -- 特徵 C：同日共用 IP 者 (任何帳戶，只要在「同一天」使用了警示戶當天用過的 IP)
    SELECT 
        m.IP
        , m.ACT_NO
        , MAX(m.LOG_REQ_DATE) AS LAST_SHARE_DATE
        , m.SOURCE
    FROM T_LOG_MAPPED m
    INNER JOIN T_TARGET_B_LOGS b 
        ON m.IP = b.IP 
       AND m.LOG_REQ_DATE = b.LOG_REQ_DATE -- 嚴格鎖定「同日」共用
    GROUP BY m.IP, m.ACT_NO, m.SOURCE
)


-- 3. T_TXN_PS_FINAL: 篩選組合清楚的 OR，並計算總額比 (統一整合三份報告為單一寬表)
, T_TXN_PS_FINAL AS (
    -- 產出 1: 警示戶 IP
    SELECT 
        'B_IP' AS REPORT_TYPE
        , ACT_NO
        , IP AS TRACE_VALUE
        , CAST(NULL AS VARCHAR(10)) AS SOURCE
        , LAST_USE_DATE AS TRACE_DATE 
    FROM T_B_IP

    UNION ALL

    -- 產出 2: 警示戶 Device
    SELECT 
        'B_DEVICE' AS REPORT_TYPE
        , ACT_NO
        , DEVICE_ID AS TRACE_VALUE
        , CAST(NULL AS VARCHAR(10)) AS SOURCE
        , LAST_USE_DATE AS TRACE_DATE 
    FROM T_B_DEVICE

    UNION ALL

    -- 產出 3: 同日共用 IP 者
    SELECT 
        'SHARED_IP' AS REPORT_TYPE
        , ACT_NO
        , IP AS TRACE_VALUE
        , SOURCE
        , LAST_SHARE_DATE AS TRACE_DATE 
    FROM T_SHARED_IP
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    REPORT_TYPE
    , ACT_NO
    , TRACE_VALUE
    , SOURCE
    , TRACE_DATE
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL
WHERE NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
-- ORDER BY REPORT_TYPE, TRACE_DATE DESC, ACT_NO;