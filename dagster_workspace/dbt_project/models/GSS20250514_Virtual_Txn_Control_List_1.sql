/*
Created: 2026-05-23
Description: 虛擬交易管控清單 1
Change Log:
- 2026-05-23 [REFACT] Refactor from python script, removing cancellation logic and twelve_to_fourteen conversion.
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_VIRTUAL_TRANSACT_1', 
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
        , FUNC_CODE
        , DR_FLG
        , CAST(TXN_AMT AS BIGINT) AS TXN_AMT
        , BNK_ID
        , IS_102_ACT
    FROM {{ ref('txn_ps_net') }}
    WHERE NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
      AND TABLE_DATE = '{{ target_date }}'
)


-- 2. T_TXN_PS_FULL: 算出所有特徵工程 (聚合與特徵值計算)
, T_TXN_PS_AGG1 AS (
    SELECT 
        ACT_NO
        , SUM(
            CASE 
                WHEN TXN_AMT > 2000
                 AND BNK_ID = '805'
                 AND IS_102_ACT = '1'
                THEN 1 
                ELSE 0 
            END
          ) AS [虛擬帳戶交易次數]
        , SUM(
            CASE 
                WHEN BNK_ID = '805'
                 AND IS_102_ACT = '1'
                 AND {{ is_outward_large('VIRTUAL_TRANSACT') }}
                THEN 1 
                ELSE 0 
            END
          ) AS [指定交易出帳次數]
        , SUM(
            CASE 
                WHEN BNK_ID = '805' 
                 AND IS_102_ACT = '1'
                 AND {{ is_outward_large('VIRTUAL_TRANSACT') }}
                THEN TXN_AMT 
                ELSE 0 
            END
          ) AS [指定交易出帳總額]
        , SUM(
            CASE 
                WHEN TXN_AMT > 2000
                 AND DR_FLG = '0'
                 AND FUNC_CODE <> '1419'
                THEN TXN_AMT 
                ELSE 0 
            END
          ) AS [非1419入帳總額]
    FROM T_TXN_PS_BASE
    GROUP BY ACT_NO
)

, T_TXN_PS_FULL AS (
    SELECT 
        ACT_NO
        , ISNULL([虛擬帳戶交易次數], 0) AS [虛擬帳戶交易次數]
        , ISNULL([指定交易出帳次數], 0) AS [指定交易出帳次數]
        , ISNULL([指定交易出帳總額], 0) AS [指定交易出帳總額]
        , ISNULL([非1419入帳總額], 0) AS [非1419入帳總額]
        , ROUND(
            {{ safe_divide('[指定交易出帳總額]', '[非1419入帳總額]', '0') }}
            , 2
          ) AS [ATM臨櫃虛擬帳戶交易出帳總額佔當日入帳總額比]
    FROM T_TXN_PS_AGG1
)


-- 3. T_TXN_PS_FINAL: 條件篩選
, T_TXN_PS_FINAL AS (
    SELECT 
        ACT_NO AS [帳號]
        , [虛擬帳戶交易次數]
        , [指定交易出帳次數]
        , [指定交易出帳總額]
        , [非1419入帳總額]
        , [ATM臨櫃虛擬帳戶交易出帳總額佔當日入帳總額比]
    FROM T_TXN_PS_FULL
    WHERE [指定交易出帳次數] >= 3
      AND [ATM臨櫃虛擬帳戶交易出帳總額佔當日入帳總額比] >= 0.90
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    [帳號]
    , [虛擬帳戶交易次數]
    , [指定交易出帳次數]
    , [指定交易出帳總額]
    , [非1419入帳總額]
    , [ATM臨櫃虛擬帳戶交易出帳總額佔當日入帳總額比]
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL
-- ORDER BY [帳號];