/*
Created: 2026-05-23
Description: 解控加分計算統整表 (包含各項加分標記與設控紀錄)
Change Log:
- 2026-05-23 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_UNLOCK_SCORE_LIST', 
    tags=["UNLOCK_SCORE", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}
{% set target_ym = target_date[:7] %}

-- 1. T_BASE 系列: 保持乾淨的 SELECT，進行初步過濾與型別轉換
WITH T_CUST_CHG_BASE AS (
    SELECT 
        ACT_NO
        , FUNC_CODE
        , TXM_MEMO
        , TXN_BRH
        , APPLY_CODE
        , CAST(TXN_DATE AS DATE) AS TXN_DATE
        , CONVERT(
            DATETIME
            , CAST(TXN_DATE AS VARCHAR) + ' ' + STUFF(STUFF(TXN_TIME, 5, 0, ':'), 3, 0, ':')
          ) AS DATETIME
    FROM {{ source('database', 'T_CUST_CHG') }}
    WHERE NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
      AND CAST(TXN_DATE AS DATE) >= DATEADD(DAY, -29, CAST('{{ target_date }}' AS DATE))
      AND CAST(TXN_DATE AS DATE) <= CAST('{{ target_date }}' AS DATE)
)

, T_CUST_PS_BASE AS (
    SELECT 
        ACT_NO
        , ID
        , ID_DUP_NO
        , BRH_NO AS [開戶局]
    FROM {{ ref('snp_dim_cust_act') }}
    WHERE NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
    {% if target_date == run_started_at.strftime("%Y-%m-%d") %}
      AND dbt_valid_to IS NULL
    {% else %}
      AND CAST('{{ target_date }}' AS DATETIME) >= dbt_valid_from 
      AND CAST('{{ target_date }}' AS DATETIME) < ISNULL(dbt_valid_to, '9999-12-31')
    {% endif %}
)

, T_CUST_BASE AS (
    SELECT 
        ID
        , ID_DUP_NO
        , OCUP_CODE
    FROM {{ ref('snp_cust') }}
    WHERE NULLIF(LTRIM(RTRIM(ID)), '') IS NOT NULL
      AND TABLE_DATE = '{{ target_ym }}'
)

, T_SUBSIDY_BASE AS (
    SELECT DISTINCT ACCT_NBR_ORI AS ACT_NO
    FROM {{ source('database', 'T_SUBSIDY') }}
    WHERE TABLE_DATE = '{{ target_date }}'
)

, T_LOW_INCOME_BASE AS (
    SELECT DISTINCT ACCT_NBR_ORI AS ACT_NO
    FROM {{ source('database', 'T_LOW_INCOME') }}
    WHERE TABLE_DATE = '{{ target_date }}'
)

, T_FARPOST_BASE AS (
    SELECT DISTINCT SUBSTRING(BRH_NO, 1, 6) AS BRH_NO
    FROM {{ source('database', 'T_FARPOST') }} 
)

, T_CUST_PS_LASTCTRL_BASE AS (
    SELECT 
        ACT_NO
        , TXN_DATE AS [設控日期]
        , TXN_TIME
        , TXM_MEMO
    FROM {{ source('database', 'T_CUST_PS_LASTCTRL') }}
    WHERE TABLE_DATE = '{{ target_date }}'
)


-- 2. T_TXN_PS_FULL: 算出所有特徵工程 (聚合與特徵值計算)
, RELEASE_EVENTS AS (
    SELECT 
        ACT_NO
        , DATETIME AS RELEASE_DATETIME
        , TXN_DATE AS RELEASE_DATE
        , TXN_BRH AS RELEASE_BRH
    FROM (
        SELECT 
            ACT_NO
            , DATETIME
            , TXN_DATE
            , TXN_BRH
            , ROW_NUMBER() OVER(
                PARTITION BY ACT_NO 
                ORDER BY DATETIME DESC
              ) AS RN
        FROM T_CUST_CHG_BASE
        WHERE FUNC_CODE = '1679'
          AND TXM_MEMO = 'RELEASE'
          AND APPLY_CODE = '1'
    ) t
    WHERE RN = 1
)

, WARNING_EVENTS AS (
    SELECT 
        c.ACT_NO
        , c.TXN_DATE
        , c.TXN_BRH
        , r.RELEASE_DATETIME
        , r.RELEASE_DATE
        , r.RELEASE_BRH
    FROM T_CUST_CHG_BASE c
    INNER JOIN RELEASE_EVENTS r 
        ON c.ACT_NO = r.ACT_NO
    WHERE c.DATETIME >= r.RELEASE_DATETIME
      AND DATEDIFF(DAY, r.RELEASE_DATE, c.TXN_DATE) <= 30
      AND c.FUNC_CODE IN {{ get_config()['UNLOCK_SCORE']['warn_codes'] }}
      AND NOT (c.FUNC_CODE = '1603' AND ISNULL(c.TXM_MEMO, '') <> '')
)

, WARN_DAILY_MAX AS (
    SELECT 
        ACT_NO
        , MAX(DAILY_CNT) AS MAX_DAILY_CNT
        , MAX(TXN_DATE) AS LAST_WARN_DATE
    FROM (
        SELECT 
            ACT_NO
            , TXN_DATE
            , COUNT(*) AS DAILY_CNT
        FROM WARNING_EVENTS
        GROUP BY ACT_NO, TXN_DATE
    ) t
    GROUP BY ACT_NO
)

, WARNING_METRICS_AGG AS (
    SELECT 
        w.ACT_NO
        , w.RELEASE_DATE
        , w.RELEASE_BRH
        , COUNT(*) AS DANGER_COUNT
        , SUM(
            CASE 
                WHEN w.TXN_BRH = w.RELEASE_BRH 
                THEN 1 
                ELSE 0 
            END
          ) AS SAME_BRH_COUNT
        , SUM(
            CASE 
                WHEN w.TXN_BRH <> w.RELEASE_BRH 
                THEN 1 
                ELSE 0 
            END
          ) AS DIFF_BRH_COUNT
    FROM WARNING_EVENTS w
    GROUP BY w.ACT_NO, w.RELEASE_DATE, w.RELEASE_BRH
)

, ACT_FULL_DATA AS (
    SELECT 
        m.ACT_NO
        , m.RELEASE_DATE
        , m.RELEASE_BRH
        , m.DANGER_COUNT
        , m.SAME_BRH_COUNT
        , m.DIFF_BRH_COUNT
        , dm.LAST_WARN_DATE
        , ISNULL(dm.MAX_DAILY_CNT, 0) AS MAX_DAILY_CNT
        , CASE 
            WHEN f.BRH_NO IS NOT NULL
            THEN 'Y' ELSE '' END
            AS IS_FARPOST
        , CASE 
            WHEN c.OCUP_CODE IN {{ get_config()['UNLOCK_SCORE']['ocup_5'] }}
            THEN 'Y' ELSE '' END
            AS IS_OCUP_5
        , CASE 
            WHEN c.OCUP_CODE IN {{ get_config()['UNLOCK_SCORE']['ocup_10'] }}
            THEN 'Y'ELSE '' END
            AS IS_OCUP_10
        , CASE 
            WHEN sub.ACT_NO IS NOT NULL 
              OR low.ACT_NO IS NOT NULL 
            THEN 'Y' ELSE '' END
            AS IS_LOW_SUB
    FROM WARNING_METRICS_AGG m
    LEFT JOIN WARN_DAILY_MAX dm ON m.ACT_NO = dm.ACT_NO
    LEFT JOIN T_CUST_PS_BASE ps ON m.ACT_NO = ps.ACT_NO
    LEFT JOIN T_CUST_BASE c ON ps.ID = c.ID AND ps.ID_DUP_NO = c.ID_DUP_NO
    LEFT JOIN T_FARPOST_BASE f ON SUBSTRING(ps.[開戶局], 1, 6) = f.BRH_NO
    LEFT JOIN T_SUBSIDY_BASE sub ON m.ACT_NO = sub.ACT_NO
    LEFT JOIN T_LOW_INCOME_BASE low ON m.ACT_NO = low.ACT_NO
)

, T_TXN_PS_FULL AS (
    SELECT 
        a.LAST_WARN_DATE AS [TXN_DATE]
        , a.ACT_NO
        , a.RELEASE_BRH AS [解控局]
        , a.DANGER_COUNT AS [累計危險交易次數]
        , a.IS_LOW_SUB AS [低收或補助戶]
        , a.IS_FARPOST AS [偏鄉郵局]
        , a.IS_OCUP_10 AS [10分高危職業]
        , a.IS_OCUP_5 AS [5分高危職業]
        , ( 55 
            + (a.SAME_BRH_COUNT * 5)
            + (a.DIFF_BRH_COUNT * 10)
            + (CASE WHEN a.MAX_DAILY_CNT >= 2 THEN 5 ELSE 0 END)
            + (CASE WHEN a.IS_FARPOST = 'Y' THEN 5 ELSE 0 END)
            + (CASE WHEN a.IS_OCUP_5 = 'Y' THEN 5 ELSE 0 END)
            + (CASE WHEN a.IS_LOW_SUB = 'Y' THEN 10 ELSE 0 END)
            + (CASE WHEN a.IS_OCUP_10 = 'Y' THEN 10 ELSE 0 END)
          ) AS [總分]
        , REPLACE(CAST(a.RELEASE_DATE AS VARCHAR(10)), '-', '') AS [解控日]
    FROM ACT_FULL_DATA a
)


-- 3. T_TXN_PS_FINAL: 條件篩選
, T_TXN_PS_FINAL AS (
    SELECT 
        f.[TXN_DATE]
        , f.[ACT_NO]
        , f.[解控局]
        , f.[累計危險交易次數]
        , f.[低收或補助戶]
        , f.[偏鄉郵局]
        , f.[10分高危職業]
        , f.[5分高危職業]
        , f.[總分]
        , f.[解控日]
        , ctrl.[設控日期]
        , ctrl.[TXN_TIME]
        , ctrl.[TXM_MEMO]
    FROM T_TXN_PS_FULL f
    LEFT JOIN T_CUST_PS_LASTCTRL_BASE ctrl 
        ON f.ACT_NO = ctrl.ACT_NO
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    [TXN_DATE]
    , [ACT_NO]
    , [解控局]
    , [累計危險交易次數]
    , [低收或補助戶]
    , [偏鄉郵局]
    , [10分高危職業]
    , [5分高危職業]
    , [總分]
    , [解控日]
    , [設控日期]
    , [TXN_TIME]
    , [TXM_MEMO]
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL
-- ORDER BY [ACT_NO];