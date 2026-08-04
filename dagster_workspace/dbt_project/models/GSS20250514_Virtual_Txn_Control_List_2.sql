/*
Created: 2026-05-23
Description: 虛擬交易管控清單 2
Change Log:
- 2026-05-23 [REFACT] Refactor from python script, removing cancellation logic and twelve_to_fourteen conversion.
- 2026-07-16 [CHANGE] 805/102 改為帳號層級必要條件 (至少一筆即符合), 前置過濾後聚合不再逐筆檢查.
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_VIRTUAL_TRANSACT_2', 
    tags=["VIRTUAL_TRANSACT", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定 (使用 dbt var 以支援 Dagster 排程動態傳入)
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}

-- 1. T_BASE 系列: 保持乾淨的 SELECT，進行初步過濾與型別轉換
WITH T_TXN_PS_BASE AS (
    SELECT
        ACT_NO
        , CPU_DATE
        , TXN_TIME
        , Seq_ID
        , FUNC_CODE
        , DR_FLG
        , CAST(TXN_AMT AS BIGINT) AS TXN_AMT
        , CAST(PS_BAL AS BIGINT) AS PS_BAL
        , BNK_ID
        , IS_102_ACT
    FROM {{ ref('txn_ps_net') }}
    WHERE NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
      AND TABLE_DATE = '{{ target_date }}'
)


-- 2. T_QUALIFIED_ACT: 必要條件前置過濾 (當日至少一筆 805 + 102 交易即符合)
, T_QUALIFIED_ACT AS (
    SELECT DISTINCT
        ACT_NO
    FROM T_TXN_PS_BASE
    WHERE BNK_ID = '805'
      AND IS_102_ACT = '1'
)

, T_TXN_PS_FILTERED AS (
    SELECT
        B.ACT_NO
        , B.CPU_DATE
        , B.TXN_TIME
        , B.FUNC_CODE
        , B.DR_FLG
        , B.TXN_AMT
        , B.PS_BAL
    FROM T_TXN_PS_BASE AS B
    INNER JOIN T_QUALIFIED_ACT AS Q
        ON B.ACT_NO = Q.ACT_NO
)


-- 3. T_TXN_PS_FULL: 算出所有特徵工程 (聚合與特徵值計算)
, T_TXN_PS_AGG1 AS (
    SELECT 
        ACT_NO
        , CPU_DATE
        , TXN_TIME
        , FUNC_CODE
        , DR_FLG
        , TXN_AMT
        , PS_BAL
        , ROW_NUMBER() OVER(
            PARTITION BY ACT_NO 
            ORDER BY CPU_DATE DESC, TXN_TIME DESC
          ) AS RN_LAST
    FROM T_TXN_PS_FILTERED
)

, T_TXN_PS_AGG2 AS (
    SELECT 
        ACT_NO
        , SUM(
            CASE 
                WHEN TXN_AMT > 2000
                 AND DR_FLG = '1' 
                THEN TXN_AMT 
                ELSE 0 
            END
          ) AS [虛擬交易出帳總額]
        , SUM(
            CASE 
                WHEN TXN_AMT > 2000
                 AND DR_FLG = '0'
                 AND FUNC_CODE <> '1419' 
                THEN TXN_AMT 
                ELSE 0 
            END
          ) AS [非1419入帳總額]
        , MAX(
            CASE 
                WHEN RN_LAST = 1 
                THEN PS_BAL 
            END
          ) AS [當日餘額]
    FROM T_TXN_PS_AGG1
    GROUP BY ACT_NO
)

, T_TXN_PS_FULL AS (
    SELECT 
        ACT_NO
        , ISNULL([虛擬交易出帳總額], 0) AS [虛擬交易出帳總額]
        , ISNULL([非1419入帳總額], 0) AS [非1419入帳總額]
        , ISNULL([當日餘額], 0) AS [當日餘額]
        , ROUND(
            {{ safe_divide('[虛擬交易出帳總額]', '[非1419入帳總額]', '0') }}
            , 2
          ) AS [虛擬帳戶交易出帳總額佔當日入帳總額比]
    FROM T_TXN_PS_AGG2
)


-- 4. T_TXN_PS_FINAL: 條件篩選
, T_TXN_PS_FINAL AS (
    SELECT 
        ACT_NO AS [帳號]
        , [虛擬交易出帳總額]
        , [非1419入帳總額]
        , [當日餘額]
        , [虛擬帳戶交易出帳總額佔當日入帳總額比]
    FROM T_TXN_PS_FULL
    WHERE [當日餘額] <= 3000
      AND [虛擬帳戶交易出帳總額佔當日入帳總額比] >= 0.90
)


-- ==================== 5. 最終輸出 ====================
SELECT 
    [帳號]
    , [虛擬交易出帳總額]
    , [非1419入帳總額]
    , [當日餘額]
    , [虛擬帳戶交易出帳總額佔當日入帳總額比]
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL
-- ORDER BY [帳號];