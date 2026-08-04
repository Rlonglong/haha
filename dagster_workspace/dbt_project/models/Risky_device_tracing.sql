/*
Created: 2026-05-29
Description: 追蹤風險DEVICE使用帳戶 (HRD 高風險設備操作正常帳號清單)
Change Log:
- 2026-05-29 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
*/

{{ config(
    materialized='incremental',
    alias='mrt_RISKY_DEVICE_TRACING', 
    tags=["RISK_DEVICE", "daily_job"]
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
    FROM {{ source('database', 'mrt_WARNINGFLG_BONUS') }}
    WHERE TABLE_DATE = {{ target_date }}
      AND IS_VALID_ACT_FLG = 'Y'
)

, T_CUST_PS_BASE AS (
    SELECT 
        ID
        , ACT_NO
        , IS_VALID_ACT_FLG
    FROM (
        SELECT 
            ID
            , ACT_NO
            , ROW_NUMBER() OVER(PARTITION BY ID ORDER BY OPEN_DATE DESC) AS RN
        FROM {{ source('database', 'T_CUST_PS') }}
        WHERE TABLE_DATE = '{{ target_ym }}'
          AND NULLIF(LTRIM(RTRIM(ID)), '') IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
          AND STATUS_PBA_CODE NOT IN {{ get_config()['RISKY_DEVICE_TRACING']['exclude_pba_codes'] }}
    ) t
    WHERE RN = 1
)

, T_MPOST_BASE AS (
    SELECT 
        DEVICE_ID
        , ACT_NO
        , ID
        , LOG_REQ_DATE
        , IS_VALID_ACT_FLG
    FROM {{ source('database', 'T_MPOST_LOG') }}
    WHERE LOG_REQ_DATE = DATEADD(DAY, -1, CAST('{{ target_date }}' AS DATE))
      AND RESULT_FLG = '0'
      AND FUNC_CODE IN {{ get_config()['RISKY_DEVICE_TRACING']['mpost_func_codes'] }}
      AND NULLIF(LTRIM(RTRIM(DEVICE_ID)), '') IS NOT NULL
)


-- 2. T_TXN_PS_FULL
, T_MPOST_MAPPED AS (
    SELECT DISTINCT
        m.DEVICE_ID
        , CASE 
            WHEN m.IS_VALID_ACT_FLG = 'Y' THEN m.ACT_NO
            ELSE c.ACT_NO
          END AS ACT_NO
        , CASE 
            WHEN m.IS_VALID_ACT_FLG = 'Y' THEN 'Y'
            ELSE ISNULL(c.IS_VALID_ACT_FLG, 'N')
          END AS FINAL_VALID_FLG
        , m.LOG_REQ_DATE AS [最後使用日]
    FROM T_MPOST_BASE m
    LEFT JOIN T_CUST_PS_BASE c 
        ON m.ID = c.ID
)

, , T_DEVICE_TRACE AS (
    SELECT 
        h.DEVICE_ID
        , m.ACT_NO AS [今日使用帳號]
        , ISNULL(m.[最後使用日], h.[最後使用日]) AS [最後使用日]
        , h.[警示] AS [累計警示]
        , h.[目前警示] AS [先前警示]
        , h.[目前終止] AS [先前終止]
        , h.[目前凍結] AS [先前凍結]
        , h.[目前管制] AS [先前管制]
        , h.[目前衍管] AS [先前衍管]
        , h.[操作帳號總數] AS [先前操作帳號總數]
        , ISNULL(w.[目前註記], '') AS [目前註記]
    FROM {{ source('database', 'mrt_RISKY_DEVICE') }} h 
    LEFT JOIN T_MPOST_MAPPED m
        ON h.DEVICE_ID = m.DEVICE_ID
        AND m.ACT_NO IS NOT NULL 
        AND m.FINAL_VALID_FLG = 'Y'
    LEFT JOIN T_WARNING_BASE w 
        ON m.ACT_NO = w.ACT_NO
)


-- 3. T_TXN_PS_FINAL
, T_TXN_PS_FINAL AS (
    SELECT 
        [今日使用帳號] AS ACT_NO
        , DEVICE_ID
        , [累計警示] AS [DEVICE警示數]
        , [先前操作帳號總數] AS [DEVICE操作帳號數]
        
        , [先前警示]
        , [先前終止]
        , [先前凍結]
        , [先前管制]
        , [先前衍管]
        , [最後使用日]
        , [目前註記]

        , CASE 
            WHEN [累計警示] >= 5 
             AND [目前註記] IN {{ get_config()['RISKY_DEVICE_TRACING']['normal_pba_codes'] }}
            THEN 1 ELSE 0 
          END AS IS_HRD_5
        , CASE 
            WHEN [累計警示] >= 2 
             AND [目前註記] IN {{ get_config()['RISKY_DEVICE_TRACING']['normal_pba_codes'] }}
            THEN 1 ELSE 0 
          END AS IS_HRD_2
    FROM T_DEVICE_TRACE
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    ACT_NO
    , DEVICE_ID
    , [DEVICE警示數]
    , [DEVICE操作帳號數]
    , [先前警示]
    , [先前終止]
    , [先前凍結]
    , [先前管制]
    , [先前衍管]
    , [最後使用日]
    , IS_HRD_5
    , IS_HRD_2
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL
-- ORDER BY [DEVICE警示數] DESC;