/*
Created: 2026-05-27
Description: 90日內與警示帳戶共用 DEVICE 之帳戶清單
Change Log:
- 2026-05-27 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_WARNINGFLG_B_IPDEVICE_SHARE', 
    tags=["RISK_TRACEBACK", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定 (固定的 90 天極限區間公式)
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}
{% set target_ym = target_date[:7] %}

-- 1. T_BASE 系列: 保持乾淨的 SELECT 
WITH T_TARGET_DEVICE_BASE AS (
    SELECT DISTINCT TRACE_VALUE AS DEVICE_ID
    FROM {{ source('database', 'mrt_WARNINGFLG_B_IPDEVICE') }}
    WHERE TABLE_DATE = {{ target_date }}
      AND REPORT_TYPE = 'B_DEVICE'
)

, T_WARNING_BASE AS (
    SELECT 
        ACT_NO
        , IS_VALID_ACT_FLG
        , STATUS_PBA_CODE
        , WARN_MARK
        , [警示日期]
    FROM {{ source('database', 'mrt_WARNINGFLG_BONUS') }}
    WHERE TABLE_DATE = {{ target_date }}
      AND IS_VALID_ACT_FLG = 'Y'
)

, T_CUST_PS_BASE AS (
    -- ID 反查字典檔 (防堵雙重帳戶對應造成的笛卡爾積)
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
          AND NOT (
              STATUS_PBA_CODE IN {{ get_config()['WARNINGFLG_SHARE_DEVICE']['exclude_pba_codes'] }}
              AND LAST_TX_DATE <= DATEADD(DAY, -89, CAST('{{ target_date }}' AS DATE))
          )
    ) t
    WHERE RN = 1
)

, T_MPOST_BASE AS (
    SELECT 
        LOG_REQ_DATE
        , ACT_NO
        , ID
        , DEVICE_ID
    FROM {{ source('database', 'T_MPOST_LOG') }}
    WHERE LOG_REQ_DATE BETWEEN DATEADD(DAY, -89, CAST('{{ target_date }}' AS DATE)) AND {{ target_date }}
      AND RESULT_FLG = '0'
      AND FUNC_CODE IN {{ get_config()['WARNINGFLG_SHARE_DEVICE']['mpost_func_codes'] }}
      AND NULLIF(LTRIM(RTRIM(DEVICE_ID)), '') IS NOT NULL
)


-- 2. T_TXN_PS_FULL: 完美對齊 Python 的 Fallback 與分流關聯邏輯
, T_LOG_MAPPED AS (
    SELECT 
        m.LOG_REQ_DATE
        , m.DEVICE_ID
        , CASE 
            WHEN m.IS_VALID_ACT_FLG = 'Y' THEN m.ACT_NO
            ELSE c.ACT_NO
          END AS ACT_NO
    FROM T_MPOST_BASE m
    LEFT JOIN T_CUST_PS_BASE c 
        ON m.ID = c.ID
)

, T_SHARED_DEVICE_RAW AS (
    SELECT 
        m.DEVICE_ID
        , m.ACT_NO
        , m.IS_VALID_ACT_FLG
        , MAX(m.LOG_REQ_DATE) AS LAST_USE_DATE
    FROM T_LOG_MAPPED m
    INNER JOIN T_TARGET_DEVICE_BASE t 
        ON m.DEVICE_ID = t.DEVICE_ID
    WHERE m.IS_VALID_ACT_FLG = 'Y'
    GROUP BY m.DEVICE_ID, m.ACT_NO
)


-- 3. T_TXN_PS_FINAL: 關聯警示狀態與計算特徵 Flag
, T_SHARED_DEVICE_ENRICHED AS (
    SELECT 
        r.DEVICE_ID
        , r.ACT_NO
        , ISNULL(w.STATUS_PBA_CODE, '') AS [帳戶狀態]
        , w.WARN_MARK AS [警示標記]
        , w.WARN_DATE AS [警示日期]
        , r.LAST_USE_DATE
        , CASE 
            WHEN ISNULL(w.STATUS_PBA_CODE, '') NOT IN ('3', '4', '8', '9', 'G', 'B', 'C') THEN 1 
            ELSE 0 
          END AS IS_NORMAL_ACT
    FROM T_SHARED_DEVICE_RAW r
    LEFT JOIN T_WARNING_BASE w 
        ON r.ACT_NO = w.ACT_NO
)

, T_TXN_PS_FINAL AS (
    SELECT 
        DEVICE_ID
        , ACT_NO
        , [帳戶狀態]
        , [警示標記]
        , [警示日期]
        , LAST_USE_DATE
        -- 視窗函數：同一台 Device 只要存在任一活躍正常帳戶，全數標記為 'Y'
        , CASE 
            WHEN MAX(IS_NORMAL_ACT) OVER(PARTITION BY DEVICE_ID) = 1 THEN 'Y' 
            ELSE 'N' 
          END AS HAS_NORMAL_ACT_FLG
    FROM T_SHARED_DEVICE_ENRICHED
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    DEVICE_ID
    , ACT_NO
    , [帳戶狀態]
    , [警示標記]
    , [警示日期]
    , LAST_USE_DATE
    , HAS_NORMAL_ACT_FLG
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL
-- ORDER BY DEVICE_ID, [警示標記], ACT_NO;