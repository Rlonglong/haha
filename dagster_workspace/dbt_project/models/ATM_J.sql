/*
Created: 2026-05-27
Description: ATM_J 詐欺模型交叉比對名單 (結合 Fraud360 與解控名單，分流 Control/Email)
Change Log:
- 2026-05-27 [REFACT] Refactor from python script. 修復 Python 的隨機去重複漏洞，並整合雙產出檔案為單一模型表。
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_ATM_J', 
    tags=["ATM_J", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}
-- 依原邏輯，Fraud360 固定抓 T-2 日的資料
{% set fraud360_date = "REPLACE(CAST(DATEADD(DAY, -2, CAST('" ~ target_date ~ "' AS DATE)) AS VARCHAR(10)), '-', '')" %}

-- 1. T_BASE 系列: 保持乾淨的 SELECT，進行初步過濾與型別轉換
WITH T_FRAUD360_BASE AS (
    SELECT 
        ACT_NO
        , CAST(ACT_FRAUD_SCORE AS FLOAT) AS ACT_FRAUD_SCORE
        , CAST(OUT_AMT_ATM_SUM_DIV_7D_180D AS FLOAT) AS OUT_AMT_ATM_SUM_DIV_7D_180D
        , CAST(AMT_SUM_DIV_7D_180D AS FLOAT) AS AMT_SUM_DIV_7D_180D
        , CAST(IN_AMT_SUM_DIV_7D_180D AS FLOAT) AS IN_AMT_SUM_DIV_7D_180D
    FROM (
        SELECT 
            ACT_NO
            , ACT_FRAUD_SCORE
            , OUT_AMT_ATM_SUM_DIV_7D_180D
            , AMT_SUM_DIV_7D_180D
            , IN_AMT_SUM_DIV_7D_180D
            -- 若同一天有多筆，取最新的 OPER_DATE
            , ROW_NUMBER() OVER(PARTITION BY ACT_NO ORDER BY OPER_DATE DESC) AS RN
        FROM {{ source('database', 'T_FRAUD360') }}
        WHERE TABLE_DATE = {{ fraud360_date }}
    ) t
    WHERE RN = 1
)

, T_UNLOCK_SCORE_BASE AS (
    SELECT 
        ACT_NO
        , TXM_MEMO
    FROM (
        SELECT 
            ACT_NO
            , ISNULL(TXM_MEMO, '') AS TXM_MEMO
            -- 防堵原 Python 的隨機去重複漏洞：確保 30 天內有多筆時，取最新解控日的那一筆
            , ROW_NUMBER() OVER(PARTITION BY ACT_NO ORDER BY [解控日] DESC, TABLE_DATE DESC) AS RN
        {{ source('database', 'mrt_UNLOCK_SCORE_LIST') }}
        WHERE TABLE_DATE >= REPLACE(CAST(DATEADD(DAY, -30, CAST('{{ target_date }}' AS DATE)) AS VARCHAR(10)), '-', '')
          AND TABLE_DATE <= REPLACE(CAST('{{ target_date }}' AS DATE), '-', '')
    ) t
    WHERE RN = 1
)

, T_WARNFLG_BASE AS (
    SELECT 
        ACT_NO
        , ISNULL(STATUS_PBA_CODE, '') AS STATUS_PBA_CODE
    FROM {{ source('database', 'T_CUST_WARNINGFLG') }}
    WHERE TABLE_DATE = '{{ target_date }}'
)


-- 2. T_TXN_PS_FULL: 算出特徵工程與名單分類 (CASE WHEN 判定)
, T_COMBINED_CLASSIFICATION AS (
    SELECT 
        u.ACT_NO
        , f.ACT_FRAUD_SCORE
        , f.OUT_AMT_ATM_SUM_DIV_7D_180D
        , f.AMT_SUM_DIV_7D_180D
        , f.IN_AMT_SUM_DIV_7D_180D
        , u.TXM_MEMO
        , w.STATUS_PBA_CODE
        -- 核心分類邏輯
        , CASE 
            -- 【條件 1：Control (設控名單)】: 分數 >= 70 且 3大指標 >= 0.9
            WHEN f.ACT_FRAUD_SCORE >= 70 
             AND f.OUT_AMT_ATM_SUM_DIV_7D_180D >= 0.9 
             AND f.AMT_SUM_DIV_7D_180D >= 0.9 
             AND f.IN_AMT_SUM_DIV_7D_180D >= 0.9 
            THEN 'CONTROL'

            -- 【條件 2：Email (通知名單)】: 分數 < 70，指標 >= 0.9，且排除特定解控原因與警示狀態
            WHEN f.ACT_FRAUD_SCORE < 70 
             AND f.OUT_AMT_ATM_SUM_DIV_7D_180D >= 0.9 
             AND f.AMT_SUM_DIV_7D_180D >= 0.9 
             AND f.IN_AMT_SUM_DIV_7D_180D >= 0.9 
             AND u.TXM_MEMO NOT IN {{ get_config()['ATM_J']['no_control'] }}
             AND ISNULL(w.STATUS_PBA_CODE, '') NOT IN {{ get_config()['ATM_J']['nouse_code'] }}
            THEN 'EMAIL'

            ELSE NULL 
          END AS [名單類型]
    FROM T_UNLOCK_SCORE_BASE u
    -- 交集 Fraud360 名單
    INNER JOIN T_FRAUD360_BASE f 
        ON u.ACT_NO = f.ACT_NO
    -- 左關聯 取得最新的 PBA 狀態 (供 Email 條件判定用)
    LEFT JOIN T_WARNFLG_BASE w 
        ON u.ACT_NO = w.ACT_NO
)


-- 3. T_TXN_PS_FINAL: 條件篩選 (只保留符合任一名單資格的帳號)
, T_TXN_PS_FINAL AS (
    SELECT 
        ACT_NO
        , [名單類型]
        , ACT_FRAUD_SCORE
        , OUT_AMT_ATM_SUM_DIV_7D_180D
        , AMT_SUM_DIV_7D_180D
        , IN_AMT_SUM_DIV_7D_180D
        , TXM_MEMO
        , STATUS_PBA_CODE
    FROM T_COMBINED_CLASSIFICATION
    WHERE [名單類型] IS NOT NULL
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    ACT_NO
    , [名單類型]
    , ACT_FRAUD_SCORE
    , OUT_AMT_ATM_SUM_DIV_7D_180D
    , AMT_SUM_DIV_7D_180D
    , IN_AMT_SUM_DIV_7D_180D
    , TXM_MEMO
    , STATUS_PBA_CODE
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL
-- ORDER BY [名單類型], ACT_NO;