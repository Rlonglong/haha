/*
Created: 2026-05-23
Description: ATM_F (交叉比對 A, C, E 異常名單與解控高分清單)
Change Log:
- 2026-05-23 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_ATM_F', 
    tags=["ATM_F", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}

-- 1. T_BASE 系列: 保持乾淨的 SELECT，進行初步過濾與型別轉換
WITH T_ATM_A_BASE AS (
    SELECT [帳號] 
    FROM {{ ref('ATM_A2') }} 
    WHERE TABLE_DATE = '{{ target_date }}'
)

, T_ATM_C_BASE AS (
    SELECT [帳號] 
    FROM {{ ref('ATM_C2') }}
    WHERE TABLE_DATE = '{{ target_date }}'
)

, T_ATM_E_BASE AS (
    SELECT [帳號] 
    FROM {{ ref('ATM_E2') }}
    WHERE TABLE_DATE = '{{ target_date }}'
)

, T_WARNINGFLG_BASE AS (
    SELECT 
        ACT_NO
        , TABLE_DATE AS [狀態變更日]
        , STATUS_PBA_CODE AS [帳號狀態]
        , STATUS_ECTRL_FLG AS [衍生管制]
        , ISNULL(WARN_MARK, '') AS [警示標記]
    FROM {{ source('database', 'T_CUST_WARNINGFLG') }}
    WHERE TABLE_DATE = '{{ target_date }}'
)

, T_UNLOCK_SCORE_BASE AS (
    SELECT 
        ACT_NO
        , CAST(SUBSTRING([解控日], 1, 4) + '-' + SUBSTRING([解控日], 5, 2) + '-' + SUBSTRING([解控日], 7, 2) AS DATE) AS UNLOCK_DATE
        , CAST([總分] AS FLOAT) AS TOTAL_SCORE
        , ISNULL([TXM_MEMO], '') AS TXM_MEMO
    FROM {{ source('database', 'mrt_UNLOCK_SCORE_LIST') }}
    WHERE TABLE_DATE >= REPLACE(CAST(DATEADD(DAY, -30, CAST('{{ target_date }}' AS DATE)) AS VARCHAR(10)), '-', '')
      AND TABLE_DATE <= '{{ target_date }}'
)


-- 2. T_TXN_PS_FULL: 算出所有特徵工程 (聚合與特徵值計算)
-- A. 聯集 A, C, E 的名單
, ATM_UNION_LIST AS (
    SELECT [帳號] FROM T_ATM_A_BASE
    UNION 
    SELECT [帳號] FROM T_ATM_C_BASE
    UNION 
    SELECT [帳號] FROM T_ATM_E_BASE
)

-- B. 針對解控分數表，抓出每個帳號「最新」的一次解控紀錄
, LATEST_UNLOCK_SCORE AS (
    SELECT 
        ACT_NO
        , UNLOCK_DATE
        , TOTAL_SCORE
        , TXM_MEMO
    FROM (
        SELECT 
            ACT_NO
            , UNLOCK_DATE
            , TOTAL_SCORE
            , TXM_MEMO
            , ROW_NUMBER() OVER(PARTITION BY ACT_NO ORDER BY UNLOCK_DATE DESC, TABLE_DATE DESC) AS RN
        FROM T_UNLOCK_SCORE_BASE
    ) t
    WHERE RN = 1
)

-- C. 依據業務邏輯 (天數、分數、設控原因) 進行條件篩選
, FILTERED_UNLOCK_SCORE AS (
    SELECT 
        ACT_NO
        , UNLOCK_DATE
        , TOTAL_SCORE
        , TXM_MEMO
    FROM LATEST_UNLOCK_SCORE
    WHERE DATEDIFF(DAY, UNLOCK_DATE, CAST('{{ target_date }}' AS DATE)) <= 30
      AND (
          -- 情況 1：前次設控為「久未往來」，標準放寬到 >= 70 分
          (TXM_MEMO IN {{ get_config()['ATM_F']['long_time_memos'] }} AND TOTAL_SCORE >= 70)
          OR 
          -- 情況 2：前次設控「非久未往來」，標準嚴格到 >= 80 分
          (TXM_MEMO NOT IN {{ get_config()['ATM_F']['long_time_memos'] }} AND TOTAL_SCORE >= 80)
      )
)


-- 3. T_TXN_PS_FINAL: 條件篩選 (將 A/C/E 名單與高分清單做 Inner Join 收網)
, T_TXN_PS_FINAL AS (
    SELECT 
        s.ACT_NO AS [帳號]
        , REPLACE(CAST(s.UNLOCK_DATE AS VARCHAR(10)), '-', '') AS [解控日]
        , s.TOTAL_SCORE AS [總分]
        , s.TXM_MEMO AS [設控原因]
        , w.[狀態變更日]
        , w.[帳號狀態]
        , w.[衍生管制]
        , w.[警示標記]
    FROM FILTERED_UNLOCK_SCORE s
    -- 核心交集：必須在 A, C, E 任一名單中
    INNER JOIN ATM_UNION_LIST atm 
        ON s.ACT_NO = atm.[帳號]
    -- 左關聯：帶出最新的警示標籤
    LEFT JOIN T_WARNINGFLG_BASE w 
        ON s.ACT_NO = w.ACT_NO
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    f.[帳號]
    , f.[解控日]
    , f.[總分]
    , f.[設控原因]
    , f.[狀態變更日]
    , f.[帳號狀態]
    , f.[衍生管制]
    , f.[警示標記]
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL f
-- ORDER BY [帳號]
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ ref('ATM_F_earlyjob') }} e 
    WHERE e.[帳號] = f.[帳號]
);