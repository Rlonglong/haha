/*
Created: 2026-05-29
Description: 風險DEVICE清單更新 (滾動黑名單)
Change Log:
- 2026-05-29 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
*/

{{ config(
    materialized='incremental',
    unique_key='DEVICE_ID',
    alias='mrt_RISKY_DEVICE', 
    tags=["RISK_DEVICE", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}
{% set target_ym = target_date[:7] %}


-- 1. T_BASE 系列
WITH T_WARNING_BASE AS (
    SELECT ACT_NO, [目前註記], [衍伸管制], [警示標記]
    FROM (
        SELECT 
            ACT_NO
            , STATUS_PBA_CODE AS [目前註記]
            , STATUS_ECTRL_FLG AS [衍伸管制]
            , [警示標記]
            -- 加入去重機制，確保每個帳號只有一筆最新狀態
            , ROW_NUMBER() OVER(PARTITION BY ACT_NO ORDER BY [警示日期] DESC) AS RN
        FROM {{ source('database', 'mrt_WARNINGFLG_BONUS') }}
        WHERE TABLE_DATE = {{ target_date }}
          AND IS_VALID_ACT_FLG = 'Y'
    ) t
    WHERE RN = 1
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

, T_NEW_RISK01_BASE AS (
    SELECT 
        DEVICE_ID
        , ACT_NO
        , LAST_USE_DATE
    FROM {{ source('database', 'mrt_WARNINGFLG_SHARE_DEVICE') }}
    WHERE TABLE_DATE = {{ target_date }}
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
        , m.LOG_REQ_DATE AS LAST_USE_DATE
    FROM T_MPOST_BASE m
    LEFT JOIN T_CUST_PS_BASE c 
        ON m.ID = c.ID
)

, T_NEW_ACT AS (
    SELECT 
        m.DEVICE_ID
        , m.ACT_NO
        , m.LAST_USE_DATE
    FROM T_MPOST_MAPPED m
    {% if is_incremental() %}
    INNER JOIN {{ this }} h ON m.DEVICE_ID = h.DEVICE_ID
    {% else %}
    WHERE 1=0
    {% endif %}
    WHERE m.ACT_NO IS NOT NULL
      AND m.FINAL_VALID_FLG = 'Y'
)

{% if is_incremental() %}
, T_HISTORICAL_UNNESTED AS (
    SELECT 
        DEVICE_ID
        , value AS ACT_NO
    FROM {{ this }}
    CROSS APPLY STRING_SPLIT([帳號清單], ',')
)
{% endif %}

, T_COMBINED_ACCOUNTS AS (
    SELECT DEVICE_ID, ACT_NO FROM T_NEW_RISK01_BASE
    UNION ALL
    SELECT DEVICE_ID, ACT_NO FROM T_NEW_ACT
    {% if is_incremental() %}
    UNION ALL
    SELECT DEVICE_ID, ACT_NO FROM T_HISTORICAL_UNNESTED
    {% endif %}
)

, T_DEDUPLICATED_ACCOUNTS AS (
    SELECT DEVICE_ID, ACT_NO
    FROM T_COMBINED_ACCOUNTS
    WHERE NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
    GROUP BY DEVICE_ID, ACT_NO
)

, T_LAST_USE_DATES AS (
    SELECT DEVICE_ID, LAST_USE_DATE AS [最後使用日]
    FROM T_NEW_RISK01_BASE
    UNION ALL
    SELECT DEVICE_ID, LAST_USE_DATE AS [最後使用日]
    FROM T_NEW_ACT

    {% if is_incremental() %}
    UNION ALL
    SELECT DEVICE_ID, [最後使用日]
    FROM {{ this }}
    {% endif %}
)

, T_LAST_USE_DEDUP AS (
    SELECT DEVICE_ID, MAX([最後使用日]) AS [最後使用日]
    FROM T_LAST_USE_DATES
    GROUP BY DEVICE_ID
)

, T_MANAGEMENT_DATES AS (
    SELECT DEVICE_ID, '{{ target_date }}' AS [納管日]
    FROM T_NEW_RISK01_BASE
    {% if is_incremental() %}
    UNION ALL
    SELECT DEVICE_ID, [納管日]
    FROM {{ this }}
    {% endif %}
)

, T_MANAGEMENT_DEDUP AS (
    SELECT DEVICE_ID, MIN([納管日]) AS [納管日]
    FROM T_MANAGEMENT_DATES
    GROUP BY DEVICE_ID
)


-- 3. T_TXN_PS_FINAL
, T_TXN_PS_FINAL AS (
    SELECT 
        d.DEVICE_ID
        , COUNT(DISTINCT CASE WHEN w.[警示標記] = 'B' THEN d.ACT_NO END) AS [警示]
        , COUNT(DISTINCT CASE WHEN w.[目前註記] = 'B' THEN d.ACT_NO END) AS [目前警示]
        
        , COUNT(DISTINCT CASE WHEN w.[目前註記] IN {{ get_config()['RISKY_DEVICE_TRACING']['end_pba_codes'] }} THEN d.ACT_NO END) AS [目前終止]
        , COUNT(DISTINCT CASE WHEN w.[目前註記] IN {{ get_config()['RISKY_DEVICE_TRACING']['freeze_pba_codes'] }} THEN d.ACT_NO END) AS [目前凍結]
        , COUNT(DISTINCT CASE WHEN w.[目前註記] = 'C' THEN d.ACT_NO END) AS [目前管制]
        , COUNT(DISTINCT CASE WHEN w.[衍伸管制] = 'Y' THEN d.ACT_NO END) AS [目前衍管]
        
        , COUNT(DISTINCT d.ACT_NO) AS [操作帳號總數]
        
        , COUNT(DISTINCT d.ACT_NO) AS [操作帳號總數]
        , ISNULL(lu.[最後使用日], '') AS [最後使用日]
        , ISNULL(md.[納管日], '') AS [納管日]
        , STRING_AGG(d.ACT_NO, ',') WITHIN GROUP (ORDER BY d.ACT_NO) AS [帳號清單]
    FROM T_DEDUPLICATED_ACCOUNTS d
    LEFT JOIN T_WARNING_BASE w 
        ON d.ACT_NO = w.ACT_NO
    LEFT JOIN T_LAST_USE_DEDUP lu
        ON d.DEVICE_ID = lu.DEVICE_ID
    LEFT JOIN T_MANAGEMENT_DEDUP md
        ON d.DEVICE_ID = md.DEVICE_ID
    GROUP BY d.DEVICE_ID, lu.[最後使用日], md.[納管日]
)

-- , SUM(CASE WHEN w.[警示標記] = 'B' THEN 1 ELSE 0 END) AS [警示]
--         , SUM(CASE WHEN w.[目前註記] = 'B' THEN 1 ELSE 0 END) AS [目前警示]
        
--         , SUM(CASE WHEN w.[目前註記] 
--             IN {{ get_config()['RISKY_DEVICE_TRACING']['end_pba_codes'] }} 
--             THEN 1 ELSE 0 END) 
--           AS [目前終止]
--         , SUM(CASE WHEN w.[目前註記] 
--             IN {{ get_config()['RISKY_DEVICE_TRACING']['freeze_pba_codes'] }} 
--             THEN 1 ELSE 0 END) 
--           AS [目前凍結]
--         , SUM(CASE WHEN w.[目前註記] = 'C' THEN 1 ELSE 0 END) AS [目前管制]
--         , SUM(CASE WHEN w.[衍伸管制] = 'Y' THEN 1 ELSE 0 END) AS [目前衍管]


-- ==================== 4. 最終輸出 ====================
SELECT 
    DEVICE_ID
    , [警示]
    , [目前警示]
    , [目前終止]
    , [目前凍結]
    , [目前管制]
    , [目前衍管]
    , [操作帳號總數]
    , [最後使用日]
    , [納管日]
    , [帳號清單]
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL
-- ORDER BY [警示] DESC, [目前警示] DESC, [目前終止] DESC;