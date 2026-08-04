/*
Created: 2026-03-11
Description: 特定條件異常交易清單產出 (E2 情境)
Change Log:
- 2026-05-22 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
- 2026-05-08 [REFACT] Refactor with the clear-logical select
- 2026-03-11 [REFACT] Refactor from python script
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_ATM_E2', 
    tags=["ATM E2", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定 (使用 dbt var 以支援 Dagster 排程動態傳入)
-- ======================================================================
{% set target_date = var("target_date", "2026-02-03") %}


-- 1. T_TXN_PS_BASE: 把所有表需要用到的欄位先 select 進來
WITH DATE_VARS AS (
    SELECT 
        CAST(DATEADD(DAY, -1, '{{ target_date }}') AS DATE) AS YESTERDAY_DT
        , CAST(DATEADD(DAY, -2, '{{ target_date }}') AS DATE) AS TWO_DAYS_AGO_DT
)

, T_TXN_PS_BASE AS (
    SELECT
        ACT_NO
        , CPU_DATE
        , TXN_TIME
        , FUNC_CODE
        , DR_FLG
        , CAST(TXN_AMT AS BIGINT) AS TXN_AMT
        , CAST(PS_BAL AS BIGINT)  AS PS_BAL
        , V.YESTERDAY_DT
        , V.TWO_DAYS_AGO_DT
    FROM {{ ref('txn_ps_net') }}
    CROSS JOIN DATE_VARS V
    WHERE CPU_DATE IN (
        V.YESTERDAY_DT
        , V.TWO_DAYS_AGO_DT
    )
    AND NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
)


-- 2. T_TXN_PS_FULL: 算出所有特徵工程
, T_TXN_PS_AGG AS (
    SELECT 
        ACT_NO
        , CPU_DATE
        , TXN_TIME
        , FUNC_CODE
        , DR_FLG
        , TXN_AMT
        , PS_BAL
        , TWO_DAYS_AGO_DT
        , YESTERDAY_DT
        -- 利用 ROW_NUMBER 標記出次日零點最後一筆交易，方便後續抓取餘額
        , ROW_NUMBER() OVER(
            PARTITION BY 
                ACT_NO
                , CASE 
                    WHEN CPU_DATE = YESTERDAY_DT 
                    AND (
                        RIGHT('000000' + CAST(TXN_TIME AS VARCHAR(6)), 6) LIKE '00%' 
                        OR TXN_TIME = '010000'
                    ) THEN 1 
                    ELSE 0 
                END
            ORDER BY CPU_DATE DESC, TXN_TIME DESC
        ) AS RN_D2_LAST
    FROM T_TXN_PS_BASE
    WHERE CPU_DATE = TWO_DAYS_AGO_DT
       OR (
           CPU_DATE = YESTERDAY_DT 
           AND (
               RIGHT('000000' + CAST(TXN_TIME AS VARCHAR(6)), 6) LIKE '00%' 
               OR TXN_TIME = '010000'
           )
       )
)

, T_TXN_PS_FULL AS (
    SELECT 
        ACT_NO
        , MAX(TWO_DAYS_AGO_DT) AS [首日日期]

        -- [首日ATM提領總額]
        , ISNULL(
            SUM(
                CASE 
                    WHEN CPU_DATE = TWO_DAYS_AGO_DT 
                    AND {{ is_atm_outward_all('ATM_E2') }} 
                    THEN TXN_AMT 
                    ELSE 0 
                END
            )
            , 0
          ) AS [首日ATM提領總額]

        -- [首日指定入帳總額]
        , ISNULL(
            SUM(
                CASE 
                    WHEN CPU_DATE = TWO_DAYS_AGO_DT 
                    AND {{ is_inward_all('ATM_E2') }} 
                    THEN TXN_AMT 
                    ELSE 0 
                END
            )
            , 0
          ) AS [首日指定入帳總額]

        -- [次日零點ATM提領總額]
        , ISNULL(
            SUM(
                CASE 
                    WHEN CPU_DATE = YESTERDAY_DT 
                    AND (
                        RIGHT('000000' + CAST(TXN_TIME AS VARCHAR(6)), 6) LIKE '00%' 
                        OR TXN_TIME = '010000'
                    ) 
                    AND {{ is_atm_outward_all('ATM_E2') }} 
                    THEN TXN_AMT 
                    ELSE 0 
                END
            )
            , 0
          ) AS [次日零點ATM提領總額]

        -- [次日零點ATM提領次數]
        , SUM(
            CASE 
                WHEN CPU_DATE = YESTERDAY_DT 
                AND (
                    RIGHT('000000' + CAST(TXN_TIME AS VARCHAR(6)), 6) LIKE '00%' 
                    OR TXN_TIME = '010000'
                ) 
                AND {{ is_atm_outward_all('ATM_E2') }} 
                THEN 1 
                ELSE 0 
            END
          ) AS [次日零點ATM提領次數]

        -- [次日零點帳戶餘額]：利用剛剛的 RN_D2_LAST = 1 來抓取最後一筆餘額
        , MAX(
            CASE 
                WHEN CPU_DATE = YESTERDAY_DT 
                AND (
                    RIGHT('000000' + CAST(TXN_TIME AS VARCHAR(6)), 6) LIKE '00%' 
                    OR TXN_TIME = '010000'
                ) 
                AND RN_D2_LAST = 1 
                THEN PS_BAL 
                ELSE 0 
            END
          ) AS [次日零點帳戶餘額]
    FROM T_TXN_PS_AGG
    GROUP BY ACT_NO
)


-- 3. T_TXN_PS_FINAL: 篩選組合清楚的條件
, T_TXN_PS_FINAL AS (
    SELECT 
        ACT_NO AS [帳號]
        , [首日日期]
        , [首日ATM提領總額]
        , [首日指定入帳總額]
        , [次日零點ATM提領總額]
        , [次日零點ATM提領次數]
        , [次日零點帳戶餘額]
        , '' AS [remark]
    FROM T_TXN_PS_FULL
    WHERE [次日零點ATM提領次數] BETWEEN 1 AND 7
      AND [次日零點ATM提領總額] BETWEEN 100 AND 100035
      AND [次日零點帳戶餘額] BETWEEN 0 AND 79773
      AND [首日ATM提領總額] BETWEEN 85000 AND 100050
      AND [首日指定入帳總額] BETWEEN 10000 AND 205625
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    f.[帳號]
    , f.[首日日期]
    , f.[首日ATM提領總額]
    , f.[首日指定入帳總額]
    , f.[次日零點ATM提領總額]
    , f.[次日零點ATM提領次數]
    , f.[次日零點帳戶餘額]
    , f.remark
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL f
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ ref('ATM_E2_earlyjob') }} e
    WHERE e.[帳號] = f.[帳號]
    AND e.TABLE_DATE = DATEADD(DAY, -1, '{{ target_date }}')
)