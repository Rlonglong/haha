/*
Created: 2026-03-18
Description: 特定條件異常交易清單產出 (含普發現金排除與 A/B/C 三種複雜情境篩選)
Change Log:
- 2026-05-22 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
- 2026-05-08 [REFACT] Refactor with the clear-logical select
- 2026-03-18 [REFACT] Refactor from python script, Optimized with Conditional Aggregation
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_ATM_C2_earlyjob', 
    tags=["ATM C2", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定 (使用 dbt var 以支援 Dagster 排程動態傳入)
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}


-- 1. T_TXN_PS_BASE: 把所有表需要用到的欄位先 select 進來
WITH T_TXN_PS_BASE AS (
    SELECT
        table_date
        , CPU_DATE
        , TXN_TIME
        , ACT_NO
        , FUNC_CODE
        , DR_FLG
        , CAST(TXN_AMT AS BIGINT) AS TXN_AMT
        , CAST(PS_BAL AS BIGINT)  AS PS_BAL
    FROM {{ ref('txn_ps_first_net') }}
    WHERE NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
    AND TABLE_DATE = '{{ target_date }}'
)


-- 2. T_TXN_PS_FULL: 算出所有特徵工程
, T_TXN_PS_AGG1 AS (
    SELECT 
        table_date
        , ACT_NO
        , CPU_DATE
        , TXN_TIME
        , FUNC_CODE
        , DR_FLG
        , TXN_AMT
        , PS_BAL
        -- 利用 ROW_NUMBER 標記出當天最後一筆交易，方便第 4 步直接抓取餘額
        , ROW_NUMBER() 
            OVER(
                PARTITION BY ACT_NO, table_date 
                ORDER BY CPU_DATE DESC, TXN_TIME DESC
            ) 
          AS RN_LAST
    FROM T_TXN_PS_BASE
)

, T_TXN_PS_AGG2 AS (
    SELECT 
        table_date
        , ACT_NO
        -- [當日結餘]：利用剛剛的 RN_LAST = 1 來抓取最後一筆餘額
        , MAX(CASE WHEN RN_LAST = 1 THEN PS_BAL ELSE 0 END) AS [當日結餘]

        -- [指定入帳次數(排除小額)] & [指定入帳總額(排除小額)]
        , SUM(CASE WHEN {{ is_inward_large('ATM_C2') }} THEN 1 ELSE 0 END) AS [指定入帳次數(排除小額)]
        , ISNULL(
            SUM(CASE WHEN {{ is_inward_large('ATM_C2') }} THEN TXN_AMT ELSE 0 END)
            , 0) 
          AS [指定入帳總額(排除小額)]

        -- [ATM出帳次數] & [ATM出帳總額]
        , SUM(CASE WHEN {{ is_atm_outward_all('ATM_C2') }} THEN 1 ELSE 0 END) AS [ATM出帳次數]
        , ISNULL(
            SUM(CASE WHEN {{ is_atm_outward_all('ATM_C2') }} THEN TXN_AMT ELSE 0 END)
            , 0) 
          AS [ATM出帳總額]

        -- [指定出帳總額]
        , ISNULL(
            SUM(CASE WHEN  {{ is_outward_all('ATM_C2') }} THEN TXN_AMT ELSE 0 END)
            , 0) 
          AS [指定出帳總額]
    FROM T_TXN_PS_AGG1
    GROUP BY table_date, ACT_NO
)

, T_TXN_PS_FULL AS (
    SELECT 
        *
        , CASE 
            WHEN [指定入帳總額(排除小額)] = 0 THEN 0
            ELSE ROUND(CAST([指定出帳總額] AS FLOAT) / CAST([指定入帳總額(排除小額)] AS FLOAT), 2)
          END AS [指定出入帳總額比]
    FROM T_TXN_PS_AGG2
)

-- 3. T_TXN_PS_FINAL: 篩選組合清楚的 OR，並計算總額比
, T_TXN_PS_FINAL AS (
    SELECT 
        table_date
        , ACT_NO AS [帳號]
        -- , dbo.TWELVE_TO_FOURTEEN(ACT_NO) AS [帳號]
        , [當日結餘]
        , [指定入帳次數(排除小額)]
        , [指定入帳總額(排除小額)]
        , [ATM出帳次數]
        , [ATM出帳總額]
        , [指定出帳總額]
        , [指定出入帳總額比]
        , '' AS [remark]
    FROM T_TXN_PS_FULL
    WHERE 
        -- Condition A
        (
            [ATM出帳次數] BETWEEN 2 AND 3
            AND [ATM出帳總額] BETWEEN 68840 AND 100120
            AND [指定出帳總額] BETWEEN 94000 AND 106000
            AND [指定入帳總額(排除小額)] BETWEEN 79682 AND 99999
            AND [指定入帳次數(排除小額)] = 2
            AND [指定出入帳總額比] BETWEEN 0.61 AND 1.22
            AND [當日結餘] <= 1000
        )
        OR 
        -- Condition B
        (
            [ATM出帳次數] BETWEEN 2 AND 3
            AND [ATM出帳總額] BETWEEN 68840 AND 100120
            AND [指定出帳總額] BETWEEN 94000 AND 106000
            AND [指定入帳總額(排除小額)] BETWEEN 79682 AND 123492
            AND [指定入帳次數(排除小額)] BETWEEN 3 AND 8
            AND [指定出入帳總額比] BETWEEN 0.61 AND 1.22
            AND [當日結餘] <= 1000
        )
        OR 
        -- Condition C
        (
            [ATM出帳次數] BETWEEN 4 AND 10
            AND [ATM出帳總額] BETWEEN 99776 AND 100045
            AND [指定入帳總額(排除小額)] BETWEEN 83058 AND 102600
            AND [指定入帳次數(排除小額)] BETWEEN 2 AND 3
            AND [指定出入帳總額比] BETWEEN 0.97 AND 1.55
            AND [當日結餘] <= 1000
        )
)

-- ==================== 4. 最終輸出 ====================
SELECT 
    [帳號]
    -- dbo.TWELVE_TO_FOURTEEN(ACT_NO) AS ACT_NO
    , [當日結餘]
    , [指定入帳次數(排除小額)]
    , [指定入帳總額(排除小額)]
    , [ATM出帳次數]
    , [ATM出帳總額]
    , [指定出帳總額]
    , 'earlyjob' AS remark
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL