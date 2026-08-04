/*
Created: 2026-05-27
Description: 警示帳戶個資與共用聯絡資料(電話、信箱、地址)群聚特徵分析大表
Change Log:
- 2026-05-27 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_WARNINGFLG_B_CONTACT', 
    tags=["RISK_CONTACT", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定
-- ======================================================================

{% set target_date = var("target_date", "2026-02-01") %}
{% set target_ym = target_date[:7] %}
{% set t_month_start = "CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, CAST('" ~ target_date ~ "' AS DATE)), 0) AS DATE)" %}


-- 1. T_BASE 系列: 讀取原始資料並進行格式清洗與初步過濾
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

, T_CUST_BASE AS (
    SELECT 
        ps.ACT_NO
        , ps.IS_VALID_ACT_FLG
        , ps.ID
        , ps.ID_DUP_NO
        , NULLIF(LTRIM(RTRIM(c.ADR_FUZZY)), '') AS CLEAN_ADR
        , CASE 
            WHEN 
                LTRIM(RTRIM(c.HAND_TEL)) IN ('', '          ', '0         ', '9999999999') 
            THEN NULL
            ELSE LOWER(LTRIM(RTRIM(c.HAND_TEL)))
          END AS CLEAN_TEL
        , CASE
            WHEN LTRIM(RTRIM(c.EMAIL)) = 'N' THEN NULL
            ELSE LOWER(LTRIM(RTRIM(c.EMAIL)))
          END AS CLEAN_EMAIL
    FROM {{ source('database', 'T_CUST_PS') }} ps
    LEFT JOIN {{ ref('snp_cust') }} c
        ON ps.ID = c.ID AND ps.ID_DUP_NO = c.ID_DUP_NO
    WHERE ps.TABLE_DATE = '{{ target_ym }}'
      AND c.TABLE_DATE = '{{ target_ym }}'
      AND NULLIF(LTRIM(RTRIM(ps.ID)), '') IS NOT NULL
      AND NULLIF(LTRIM(RTRIM(ps.ACT_NO)), '') IS NOT NULL
      AND ps.IS_VALID_ACT_FLG = 'Y'
)

, T_MONTHLY_CHG_TEL AS (
    SELECT ACT_NO, HAND_TEL
    FROM (
        SELECT 
            ACT_NO
            , CASE 
                WHEN 
                    LTRIM(RTRIM(HAND_TEL)) IN ('', '          ', '0         ', '9999999999') 
                THEN NULL
                ELSE LOWER(LTRIM(RTRIM(HAND_TEL)))
              END AS HAND_TEL
            , ROW_NUMBER() OVER(PARTITION BY ACT_NO ORDER BY TXN_DATE DESC) AS RN
        FROM {{ source('database', 'T_CUST_PS_CHG') }}
        WHERE TXN_DATE BETWEEN {{ t_month_start }} AND {{ target_date }}
          AND NULLIF(LTRIM(RTRIM(HAND_TEL)), '') IS NOT NULL
    ) t WHERE RN = 1
)

, T_MONTHLY_CHG_EMAIL AS (
    SELECT ACT_NO, EMAIL
    FROM (
        SELECT 
            ACT_NO
            , CASE 
                WHEN LTRIM(RTRIM(EMAIL)) = 'N' THEN NULL
                ELSE LOWER(LTRIM(RTRIM(EMAIL)))
              END AS EMAIL
            , ROW_NUMBER() OVER(PARTITION BY ACT_NO ORDER BY TXN_DATE DESC) AS RN
        FROM {{ source('database', 'T_CUST_PS_CHG') }}
        WHERE TXN_DATE BETWEEN {{ t_month_start }} AND {{ target_date }}
          AND NULLIF(LTRIM(RTRIM(EMAIL)), '') IS NOT NULL
    ) t WHERE RN = 1
)

, T_MONTHLY_WEBATM_TEL AS (
    SELECT ACT_NO, HAND_TEL
    FROM (
        SELECT 
            ACT_NO
            , CASE
                WHEN REPLACE(REPLACE(LTRIM(RTRIM(TRNS_ACC_TEL)), '-', ''), ' ', '') 
                    LIKE '09[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'
                THEN REPLACE(REPLACE(LTRIM(RTRIM(TRNS_ACC_TEL)), '-', ''), ' ', '')
                ELSE NULL
              END AS HAND_TEL
            , ROW_NUMBER() 
                OVER(PARTITION BY ACT_NO ORDER BY REPLACE(LOG_REQ_DATE, '-', '') DESC) 
                AS RN
        FROM {{ source('database', 'T_IPOST_LOG') }}
        WHERE REPLACE(LOG_REQ_DATE, '-', '') BETWEEN {{ t_month_start }} AND {{ target_date }}
    ) t WHERE RN = 1
)

, T_MONTHLY_WEBATM_EMAIL AS (
    SELECT ACT_NO, EMAIL
    FROM (
        SELECT 
            ACT_NO
            , LOWER(LTRIM(RTRIM(TRNS_ACC_EMAIL))) AS EMAIL
            , ROW_NUMBER() 
                OVER(PARTITION BY ACT_NO ORDER BY REPLACE(LOG_REQ_DATE, '-', '') DESC) 
                AS RN
        FROM {{ source('database', 'T_IPOST_LOG') }}
        WHERE REPLACE(LOG_REQ_DATE, '-', '') BETWEEN {{ t_month_start }} AND {{ target_date }}
          AND NULLIF(LTRIM(RTRIM(TRNS_ACC_EMAIL)), '') IS NOT NULL
    ) t WHERE RN = 1
)


-- 2. T_TXN_PS_FULL: 算出所有特徵工程 (動態 Fallback 覆蓋、計算三大共用計數)
, T_MASTER_PROFILE AS (
    SELECT 
        b.ACT_NO
        , b.IS_VALID_ACT_FLG
        , b.ID
        , b.ID_DUP_NO
        , b.CLEAN_ADR AS FINAL_ADR
        , COALESCE(ctel.HAND_TEL, wtel.HAND_TEL, b.CLEAN_TEL) AS FINAL_TEL
        , COALESCE(cemail.EMAIL, wemail.EMAIL, b.CLEAN_EMAIL) AS FINAL_EMAIL
    FROM T_CUST_BASE b
    LEFT JOIN T_MONTHLY_CHG_TEL ctel ON b.ACT_NO = ctel.ACT_NO
    LEFT JOIN T_MONTHLY_CHG_EMAIL cemail ON b.ACT_NO = cemail.ACT_NO
    LEFT JOIN T_MONTHLY_WEBATM_TEL wtel ON b.ACT_NO = wtel.ACT_NO
    LEFT JOIN T_MONTHLY_WEBATM_EMAIL wemail ON b.ACT_NO = wemail.ACT_NO
)

, T_ADR_COUNTS AS (
    SELECT
        m.FINAL_ADR
        , COUNT(DISTINCT m.ACT_NO) AS ADR_TOTAL_ACTS
        , COUNT(DISTINCT CASE WHEN w.[警示標記] = 'B' THEN m.ACT_NO END) 
            AS [共用地址警示戶數]
    FROM T_MASTER_PROFILE m
    LEFT JOIN T_WARNING_BASE w ON m.ACT_NO = w.ACT_NO
    WHERE NULLIF(LTRIM(RTRIM(m.FINAL_ADR)), '') IS NOT NULL
    GROUP BY m.FINAL_ADR
)

, T_TEL_COUNTS AS (
    SELECT
        m.FINAL_TEL
        , COUNT(DISTINCT m.ACT_NO) AS TEL_TOTAL_ACTS
        , COUNT(DISTINCT CASE WHEN w.[警示標記] = 'B' THEN m.ACT_NO END) 
            AS [共用電話警示戶數]
    FROM T_MASTER_PROFILE m
    LEFT JOIN T_WARNING_BASE w ON m.ACT_NO = w.ACT_NO
    WHERE NULLIF(LTRIM(RTRIM(m.FINAL_TEL)), '') IS NOT NULL
    GROUP BY m.FINAL_TEL
)

, T_EMAIL_COUNTS AS (
    SELECT
        m.FINAL_EMAIL
        , COUNT(DISTINCT m.ACT_NO) AS EMAIL_TOTAL_ACTS
        , COUNT(DISTINCT CASE WHEN w.[警示標記] = 'B' THEN m.ACT_NO END) 
            AS [共用郵件警示戶數]
    FROM T_MASTER_PROFILE m
    LEFT JOIN T_WARNING_BASE w ON m.ACT_NO = w.ACT_NO
    WHERE NULLIF(LTRIM(RTRIM(m.FINAL_EMAIL)), '') IS NOT NULL
    GROUP BY m.FINAL_EMAIL
)

, T_ID_WARNING_COUNTS AS (
    SELECT
        m.ID
        , COUNT(DISTINCT CASE WHEN w.[警示標記] = 'B' THEN m.ACT_NO END) 
            AS WARNING_ACT_CNT
    FROM T_MASTER_PROFILE m
    LEFT JOIN T_WARNING_BASE w ON m.ACT_NO = w.ACT_NO
    GROUP BY m.ID
)

, T_RISK_SHARING_COUNTS AS (
    SELECT 
        m.*
        , ISNULL(a.[共用地址警示戶數], 0) AS [共用地址警示戶數]
        , ISNULL(a.ADR_TOTAL_ACTS, 0) AS ADR_TOTAL_ACTS
        , ISNULL(t.[共用電話警示戶數], 0) AS [共用電話警示戶數]
        , ISNULL(t.TEL_TOTAL_ACTS, 0) AS TEL_TOTAL_ACTS
        , ISNULL(e.[共用郵件警示戶數], 0) AS [共用郵件警示戶數]
        , ISNULL(e.EMAIL_TOTAL_ACTS, 0) AS EMAIL_TOTAL_ACTS
        , CASE 
            WHEN ISNULL(i.WARNING_ACT_CNT, 0) > 0 
            THEN 1 ELSE 0 
          END AS IS_SAME_ID_WARNING
    FROM T_MASTER_PROFILE m
    LEFT JOIN T_ADR_COUNTS a ON m.FINAL_ADR = a.FINAL_ADR
    LEFT JOIN T_TEL_COUNTS t ON m.FINAL_TEL = t.FINAL_TEL
    LEFT JOIN T_EMAIL_COUNTS e ON m.FINAL_EMAIL = e.FINAL_EMAIL
    LEFT JOIN T_ID_WARNING_COUNTS i ON m.ID = i.ID
)


-- 3. T_TXN_PS_FINAL: 特徵標籤與分析維度整合
, T_TXN_PS_FINAL AS (
    SELECT 
        s.ACT_NO
        , s.IS_VALID_ACT_FLG
        , s.ID
        , s.ID_DUP_NO
        , CASE WHEN s.IS_SAME_ID_WARNING = 1 THEN 'Y' ELSE '' END AS [警示戶共同ID]
        , s.FINAL_TEL AS HAND_TEL
        , s.[共用電話警示戶數]
        , s.FINAL_EMAIL AS EMAIL
        , s.[共用郵件警示戶數]
        , s.FINAL_ADR AS ADR
        , s.[共用地址警示戶數]
        , ISNULL(w.[目前註記], '') AS [目前註記]
        , ISNULL(w.[衍伸管制], '') AS [衍伸管制]
        , ISNULL(w.[警示日期], '') AS [警示日期]
        , ISNULL(w.[警示標記], '') AS [警示標記]
        , s.ADR_TOTAL_ACTS
        , s.TEL_TOTAL_ACTS
        , s.EMAIL_TOTAL_ACTS
    FROM T_RISK_SHARING_COUNTS s
    LEFT JOIN T_WARNING_BASE w 
        ON s.ACT_NO = w.ACT_NO
    WHERE s.[共用地址警示戶數] > 0 
        OR s.[共用電話警示戶數] > 0 
        OR s.[共用郵件警示戶數] > 0
)

, T_REPORT_FLAGGING AS (
    SELECT 
        *
        , CASE 
            WHEN ADR_TOTAL_ACTS > 1 
            AND [共用地址警示戶數] > 1 
            THEN 1 ELSE 0 
          END AS IS_RISKY_ADR
        , CASE 
            WHEN TEL_TOTAL_ACTS > 1 
            AND [共用電話警示戶數] > 1 
            THEN 1 ELSE 0 
          END AS IS_RISKY_TEL
        , CASE 
            WHEN EMAIL_TOTAL_ACTS > 1 
            AND [共用郵件警示戶數] > 1 
            THEN 1 ELSE 0 
          END AS IS_RISKY_EMAIL
        , CASE
            WHEN [共用電話警示戶數] >= 3
             AND (CAST([共用電話警示戶數] AS FLOAT) / NULLIF(CAST(TEL_TOTAL_ACTS AS FLOAT), 0)) * 100.0 >= 10.0
             AND [目前註記] NOT IN {{ get_config()['WARNINGFLG_B_CONTACT']['comment_codes'] }}
             AND [衍伸管制] <> 'Y'
            THEN 1 ELSE 0 
          END AS IS_HR_TEL_NORMAL_ACT
        , CASE
            WHEN [共用郵件警示戶數] >= 3
             AND (CAST([共用郵件警示戶數] AS FLOAT) / NULLIF(CAST(EMAIL_TOTAL_ACTS AS FLOAT), 0)) * 100.0 >= 10.0
             AND [目前註記] NOT IN {{ get_config()['WARNINGFLG_B_CONTACT']['comment_codes'] }}
             AND [衍伸管制] <> 'Y'
            THEN 1 ELSE 0 
          END AS IS_HR_EMAIL_NORMAL_ACT
    FROM T_TXN_PS_FINAL
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    ACT_NO
    , ID
    , ID_DUP_NO
    , [警示戶共同ID]
    , HAND_TEL
    , [共用電話警示戶數]
    , EMAIL
    , [共用郵件警示戶數]
    , ADR
    , [共用地址警示戶數]
    , [目前註記]
    , [衍伸管制]
    , [警示日期]
    , [警示標記]
    , IS_RISKY_ADR
    , IS_RISKY_TEL
    , IS_RISKY_EMAIL
    , IS_HR_TEL_NORMAL_ACT
    , IS_HR_EMAIL_NORMAL_ACT
    , '{{ target_date }}' AS TABLE_DATE
FROM T_REPORT_FLAGGING
-- ORDER BY [共用電話警示戶數] DESC, HAND_TEL DESC, [共用郵件警示戶數] DESC;