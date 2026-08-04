/*
Created: 2026-05-23
Description: ATM_H (小額沖銷異常清單)
Change Log:
- 2026-05-23 [REFACT] Refactor from python script. 取代 Pandas groupby last，改用 Window Function 取最後餘額，並利用 COUNT(DISTINCT) 計算對手帳戶數。
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_ATM_H', 
    tags=["ATM_H", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}

-- 1. T_BASE 系列: 保持乾淨的 SELECT，進行初步過濾與型別轉換
WITH T_TXN_PS_BASE AS (
    SELECT
        ACT_NO
        , CPU_DATE
        , TXN_TIME
        , FUNC_CODE
        , ISNULL(TXN_NAME, '') AS TXN_NAME
        , CAST(TXN_AMT AS BIGINT) AS TXN_AMT
        , CAST(PS_BAL AS BIGINT) AS PS_BAL
        , ISNULL(BNK_ID, '') AS BNK_ID
        , ISNULL(OWN_TRANS_ACCT, '') AS OWN_TRANS_ACCT
    FROM {{ source('database', 'T_TXN_PS') }}
    WHERE NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
      AND TABLE_DATE = '{{ target_date }}'
)


-- 2. T_TXN_PS_FULL: 算出所有特徵工程 (聚合與特徵值計算)
-- A. 抓取每個帳號當日的最後一筆餘額 (確保無序資料庫能抓對)
, T_LAST_BAL AS (
    SELECT 
        ACT_NO
        , CPU_DATE
        , TXN_TIME
        , PS_BAL AS [當日餘額]
    FROM (
        SELECT 
            ACT_NO
            , CPU_DATE
            , TXN_TIME
            , PS_BAL
            , ROW_NUMBER() OVER(PARTITION BY ACT_NO ORDER BY CPU_DATE DESC, TXN_TIME DESC) AS RN
        FROM T_TXN_PS_BASE
    ) t
    WHERE RN = 1
)

-- B. 抓出小額沖銷紀錄，並計算「不重複的對手帳號數」
, T_CANCEL_AGG AS (
    SELECT
        ACT_NO
        , COUNT(DISTINCT LTRIM(RTRIM(BNK_ID)) + '_' + LTRIM(RTRIM(OWN_TRANS_ACCT))) AS [小額沖銷帳號數]
    FROM T_TXN_PS_BASE
    WHERE FUNC_CODE IN {{ get_config()['ATM_H']['specific_func'] }}
      AND TXN_NAME IN {{ get_config()['ATM_H']['specific_name'] }}
      AND TXN_AMT < 100
    GROUP BY ACT_NO
)

-- C. 合併特徵
, T_TXN_PS_FULL AS (
    SELECT 
        c.ACT_NO AS [帳號]
        , c.[小額沖銷帳號數]
        , ISNULL(b.[當日餘額], 0) AS [當日餘額]
    FROM T_CANCEL_AGG c
    LEFT JOIN T_LAST_BAL b 
        ON c.ACT_NO = b.ACT_NO
)


-- 3. T_TXN_PS_FINAL: 條件篩選
, T_TXN_PS_FINAL AS (
    SELECT 
        [帳號]
        , [小額沖銷帳號數]
        , [當日餘額]
    FROM T_TXN_PS_FULL
    WHERE [當日餘額] < 5000
      AND [小額沖銷帳號數] >= 2
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    [帳號]
    , [小額沖銷帳號數]
    , [當日餘額]
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL
-- ORDER BY [小額沖銷帳號數] DESC, [帳號];