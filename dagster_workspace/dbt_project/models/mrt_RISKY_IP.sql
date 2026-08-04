/*
Created: 2026-05-29
Description: 風險IP清單更新 (滾動黑名單)
Change Log:
- 2026-05-29 [REFACT] Refactor from python script (Apply deduplication, logic fixes, and use native DATE types)
*/

{{ config(
    materialized='incremental',
    unique_key='IP',
    alias='mrt_RISKY_IP', 
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
        , [目前註記]
        , [衍伸管制]
        , [警示標記]
    FROM (
        SELECT 
            ACT_NO
            , STATUS_PBA_CODE AS [目前註記]
            , STATUS_ECTRL_FLG AS [衍伸管制]
            , [警示標記]
            -- [LOGIC FIX] 防堵笛卡爾積：確保每個帳號只有一筆最新狀態
            , ROW_NUMBER() OVER(PARTITION BY ACT_NO ORDER BY ISNULL([警示日期], '') DESC) AS RN
        FROM {{ source('database', 'mrt_WARNINGFLG_BONUS') }}
        WHERE TABLE_DATE = CAST('{{ target_date }}' AS DATE)
          AND IS_VALID_ACT_FLG = 'Y'
    ) t
    WHERE RN = 1
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
    WHERE LOG_REQ_DATE = CAST('{{ target_date }}' AS DATE)
      AND RESULT_FLG = '0'
      AND FUNC_CODE IN {{ get_config()['RISKY_IP_TRACING']['ipost_func_codes'] }}
      AND NULLIF(LTRIM(RTRIM(IP)), '') IS NOT NULL
)

, T_NEW_RISK02_BASE AS (
    SELECT 
        IP
        , ACT_NO
        , SOURCE
        , LAST_SHARE_DATE
    FROM {{ source('database', 'mrt_WARNINGFLG_SHARE_IP') }}
    WHERE TABLE_DATE = CAST('{{ target_date }}' AS DATE)
)


-- 2. T_TXN_PS_FULL
, T_LOGS_MAPPED AS (
    -- [LOGIC FIX] 加上 DISTINCT，完美還原 Python 的 df.drop_duplicates() 防堵重複帳號灌水
    SELECT DISTINCT
        IP
        , CASE 
            WHEN IS_VALID_ACT_FLG = 'Y' 
            THEN ACT_NO 
            ELSE c.ACT_NO 
            END 
          AS ACT_NO
        , CASE 
            WHEN IS_VALID_ACT_FLG = 'Y' 
            THEN 'Y' 
            ELSE ISNULL(c.IS_VALID_ACT_FLG, 'N') 
            END 
          AS FINAL_VALID_FLG
        , LOG_REQ_DATE AS [最後使用日]
        , 'ipost' AS SOURCE_TYPE
    FROM T_IPOST_BASE i LEFT JOIN T_CUST_PS_BASE c ON i.ID = c.ID
    
    UNION ALL
    
    SELECT DISTINCT
        IP
        , CASE 
            WHEN IS_VALID_ACT_FLG = 'Y' 
            THEN ACT_NO 
            ELSE c.ACT_NO 
            END 
          AS ACT_NO
        , CASE 
            WHEN IS_VALID_ACT_FLG = 'Y' 
            THEN 'Y' 
            ELSE ISNULL(c.IS_VALID_ACT_FLG, 'N') 
            END 
          AS FINAL_VALID_FLG
        , LOG_REQ_DATE AS [最後使用日]
        , 'app' AS SOURCE_TYPE
    FROM T_MPOST_BASE m LEFT JOIN T_CUST_PS_BASE c ON m.ID = c.ID
)

, T_TRACE_ACT AS (
    SELECT 
        m.IP
        , m.ACT_NO
        , m.[最後使用日]
    FROM T_LOGS_MAPPED m
    {% if is_incremental() %}
    INNER JOIN {{ this }} h ON m.IP = h.IP
    {% else %}
    WHERE 1=0
    {% endif %}
    WHERE m.ACT_NO IS NOT NULL
      AND m.FINAL_VALID_FLG = 'Y'
)

{% if is_incremental() %}
, T_HISTORICAL_UNNESTED AS (
    SELECT 
        IP
        , value AS ACT_NO
    FROM {{ this }}
    CROSS APPLY STRING_SPLIT([帳號清單], ',')
)
{% endif %}

, T_COMBINED_ACCOUNTS AS (
    SELECT IP, ACT_NO FROM T_NEW_RISK02_BASE
    UNION ALL
    SELECT IP, ACT_NO FROM T_TRACE_ACT
    {% if is_incremental() %}
    UNION ALL
    SELECT IP, ACT_NO FROM T_HISTORICAL_UNNESTED
    {% endif %}
)

, T_DEDUPLICATED_ACCOUNTS AS (
    SELECT IP, ACT_NO
    FROM T_COMBINED_ACCOUNTS
    WHERE NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
    GROUP BY IP, ACT_NO
)

-- [LOGIC FIX] 補上 Python 漏掉的 T_TRACE_ACT (今日有活動的老 IP 日期)，參與最新日期結算
, T_LAST_USE_DATES AS (
    SELECT IP, LAST_SHARE_DATE AS [最後使用日]
    FROM T_NEW_RISK02_BASE
    
    UNION ALL
    SELECT IP, [最後使用日]
    FROM T_TRACE_ACT
    
    {% if is_incremental() %}
    UNION ALL
    SELECT IP, [最後使用日]
    FROM {{ this }}
    {% endif %}
)

, T_LAST_USE_DEDUP AS (
    SELECT IP, MAX([最後使用日]) AS [最後使用日]
    FROM T_LAST_USE_DATES
    GROUP BY IP
)

, T_MANAGEMENT_DATES AS (
    SELECT IP, CAST('{{ target_date }}' AS DATE) AS [納管日]
    FROM T_NEW_RISK02_BASE
    {% if is_incremental() %}
    UNION ALL
    SELECT IP, [納管日]
    FROM {{ this }}
    {% endif %}
)

, T_MANAGEMENT_DEDUP AS (
    SELECT IP, MIN([納管日]) AS [納管日]
    FROM T_MANAGEMENT_DATES
    GROUP BY IP
)

, T_SOURCE_AGG AS (
    SELECT 
        IP
        , MAX(CASE WHEN SOURCE LIKE '%ipost%' THEN 'Y' ELSE '' END) AS IPOST_FLG
        , MAX(CASE WHEN SOURCE LIKE '%app%' THEN 'Y' ELSE '' END) AS APP_FLG
    FROM (
        SELECT IP, SOURCE FROM T_NEW_RISK02_BASE
        {% if is_incremental() %}
        UNION ALL
        SELECT 
            IP
            , CASE 
                WHEN ipost = 'Y' 
                THEN 'ipost' 
                ELSE '' 
                END 
            AS SOURCE 
        FROM {{ this }}

        UNION ALL

        SELECT 
            IP
            , CASE 
                WHEN app = 'Y' 
                THEN 'app' 
                ELSE '' 
                END 
              AS SOURCE 
        FROM {{ this }}
        {% endif %}
    ) t
    GROUP BY IP
)


-- 3. T_TXN_PS_FINAL
, T_TXN_PS_FINAL AS (
    SELECT 
        d.IP
        , ISNULL(s.IPOST_FLG, '') AS [ipost]
        , ISNULL(s.APP_FLG, '') AS [app]
        , md.[納管日]
        , lu.[最後使用日]
        
        -- [LOGIC FIX] 全面改用 COUNT(DISTINCT ...) 防堵重複帳號灌水，並精準還原 Python 原本不排除 B 的寫法
        , COUNT(DISTINCT CASE WHEN w.[警示標記] = 'B' THEN d.ACT_NO END) AS [警示]
        , COUNT(DISTINCT CASE WHEN w.[目前註記] = 'B' THEN d.ACT_NO END) AS [目前警示]
        
        , COUNT(DISTINCT CASE WHEN w.[目前註記] 
            IN {{ get_config()['RISKY_DEVICE_TRACING']['end_pba_codes'] }} 
            AND w.[警示標記] != 'B' THEN d.ACT_NO END) 
          AS [目前終止]
        , COUNT(DISTINCT CASE WHEN w.[目前註記] 
            IN {{ get_config()['RISKY_DEVICE_TRACING']['freeze_pba_codes'] }} 
            AND w.[警示標記] != 'B' THEN d.ACT_NO END) 
          AS [目前凍結]
        , COUNT(DISTINCT 
            CASE 
                WHEN w.[目前註記] = 'C' 
                AND w.[警示標記] != 'B'
            THEN d.ACT_NO END) 
          AS [目前管制]
        , COUNT(DISTINCT 
            CASE 
                WHEN w.[衍伸管制] = 'Y' 
            THEN d.ACT_NO END) 
          AS [目前衍管]
        
        , COUNT(DISTINCT d.ACT_NO) AS [操作帳號總數]
        , STRING_AGG(d.ACT_NO, ',') WITHIN GROUP (ORDER BY d.ACT_NO) AS [帳號清單]
    FROM T_DEDUPLICATED_ACCOUNTS d
    LEFT JOIN T_WARNING_BASE w 
        ON d.ACT_NO = w.ACT_NO
    LEFT JOIN T_LAST_USE_DEDUP lu
        ON d.IP = lu.IP
    LEFT JOIN T_MANAGEMENT_DEDUP md
        ON d.IP = md.IP
    LEFT JOIN T_SOURCE_AGG s
        ON d.IP = s.IP
    GROUP BY d.IP, lu.[最後使用日], md.[納管日], s.IPOST_FLG, s.APP_FLG
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    IP
    , [ipost]
    , [app]
    , [納管日]
    , [最後使用日]
    , [帳號清單]
    , [警示]
    , [目前警示]
    , [目前終止]
    , [目前凍結]
    , [目前管制]
    , [目前衍管]
    , [操作帳號總數]
    , CAST('{{ target_date }}' AS DATE) AS TABLE_DATE
FROM T_TXN_PS_FINAL
WHERE [最後使用日] >= DATEADD(DAY, -89, CAST('{{ target_date }}' AS DATE))
  AND [警示] > 0
-- ORDER BY 
--     [警示] DESC
--     , [目前警示] DESC
--     , [目前終止] DESC
--     , [目前凍結] DESC
--     , [目前管制] DESC
--     , [目前衍管] DESC
--     , [操作帳號總數] DESC;