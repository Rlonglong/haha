/*
Created: 2026-05-23
Description: 警示帳戶金流對手帳號分析 (1車) - 完整資料表
Change Log:
- 2026-05-23 [REFACT] Refactor from python script. 修復原程式未套用 FLG=Y (涉案帳戶>=2) 的嚴重 Bug，並統一帳號為 14 碼格式不截斷。
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_ALERT_TRADE_CAR1', 
    tags=["ALERT_TRADE_CAR1", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}
{% set target_ym = target_date[:7] %}

-- 1. T_BASE 系列: 保持乾淨的 SELECT，進行初步過濾與型別轉換
WITH WARNINGFLG_NEW_BASE AS (
    SELECT 
        ACT_NO
        , STATUS_PBA_CODE
    FROM {{ source('database', 'T_CUST_WARNINGFLG') }}
    WHERE TABLE_DATE = '{{ target_date }}'
)

, T_TXN_PS_BASE AS (
    SELECT
        CPU_DATE
        , TXN_TIME
        , FUNC_CODE
        , ACT_NO AS [自身帳號]
        , TXN_TYPE
        , DR_FLG
        , ISNULL(TXN_NAME, '') AS TXN_NAME
        , OWN_TRANS_ACCT AS [對手帳號]
        , CONVERT(
            DATETIME
            , CPU_DATE + ' ' + STUFF(STUFF(TXN_TIME, 5, 0, ':'), 3, 0, ':')
          ) AS DATETIME
    FROM {{ ref('txn_ps_net') }}
    WHERE NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
      AND TABLE_DATE >= REPLACE(CAST(DATEADD(DAY, -30, CAST('{{ target_date }}' AS DATE)) AS VARCHAR(10)), '-', '')
      AND TABLE_DATE <= REPLACE(CAST('{{ target_date }}' AS DATE), '-', '')
)

, T_CUST_PS_BASE AS (
    SELECT 
        ACT_NO
        , ID
        , ID_DUP_NO
        , PBA_INT_CODE
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
        , NATION_CODE
        , OCUP_CODE
        , CASE 
            WHEN BIRTH_DATE LIKE '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]' 
            THEN YEAR(CAST('{{ target_date }}' AS DATE)) - CAST(SUBSTRING(BIRTH_DATE, 1, 4) AS INT)
                 - CASE 
                     WHEN MONTH(CAST('{{ target_date }}' AS DATE)) < CAST(SUBSTRING(BIRTH_DATE, 5, 2) AS INT) 
                       OR (MONTH(CAST('{{ target_date }}' AS DATE)) = CAST(SUBSTRING(BIRTH_DATE, 5, 2) AS INT) 
                           AND DAY(CAST('{{ target_date }}' AS DATE)) < CAST(SUBSTRING(BIRTH_DATE, 7, 2) AS INT)) 
                     THEN 1 
                     ELSE 0 
                   END
            ELSE NULL 
          END AS [年齡]
    FROM {{ source('database', 'T_CUST') }}
    WHERE NULLIF(LTRIM(RTRIM(ID)), '') IS NOT NULL
      AND TABLE_DATE = '{{ target_ym }}'
)

, T_LOW_INCOME_BASE AS (
    -- 依照指示，不對帳號做 Substring 截斷，直接使用 14 碼比對
    SELECT DISTINCT ACCT_NBR_ORI AS ACCT_14
    FROM {{ source('database', 'T_LOW_INCOME') }}
    WHERE TABLE_DATE = '{{ target_date }}'
)

, T_FARPOST_BASE AS (
    SELECT DISTINCT SUBSTRING(BRH_NO, 1, 6) AS BRH_NO
    FROM {{ source('database', 'T_FARPOST') }} 
)

, CTBC_ALERT_BASE AS (
    SELECT DISTINCT [帳號(1車)] AS ACT_NO
    FROM {{ source('database', 'T_CTBC_LIST') }}
    WHERE TABLE_DATE = '{{ target_date }}'
)


-- 2. T_TXN_PS_FULL: 算出所有特徵工程 (聚合與特徵值計算)
, NEW_ALERT_ACCTS AS (
    SELECT ACT_NO, STATUS_PBA_CODE
    FROM WARNINGFLG_NEW_BASE
    WHERE STATUS_PBA_CODE IN ('B', 'C')
)

, VALID_TXN AS (
    SELECT 
        t.[自身帳號]
        , t.[對手帳號]
        , t.DR_FLG
        , t.DATETIME
    FROM T_TXN_PS_BASE t
    WHERE t.FUNC_CODE IN {{ get_config()['ALERT_TRADE_CAR1']['target_func_codes'] }}
      AND t.TXN_TYPE <> '10'
      AND t.TXN_NAME <> N'轉存簿 '
      AND NULLIF(LTRIM(RTRIM(t.[對手帳號])), '') IS NOT NULL
      AND LEN(LTRIM(RTRIM(t.[對手帳號]))) = 12
      AND LTRIM(RTRIM(t.[對手帳號])) NOT LIKE '%[^0-9]%'
)

, ALERT_TXN_PAIRS AS (
    SELECT 
        v.[自身帳號]
        , v.[對手帳號] AS [對手帳號(1車)]
        , CASE WHEN v.DR_FLG = '1' THEN '<-' ELSE '->' END AS [交易類型]
        , v.DATETIME
        , a.STATUS_PBA_CODE AS [自身帳號狀態]
    FROM VALID_TXN v
    INNER JOIN NEW_ALERT_ACCTS a ON v.[自身帳號] = a.ACT_NO
)

, DEDUPLICATED_PAIRS AS (
    SELECT 
        [對手帳號(1車)]
        , [交易類型]
        , [自身帳號]
        , [自身帳號狀態]
        , CONCAT([自身帳號狀態], '_', [自身帳號]) AS [原警示帳號及狀態]
    FROM (
        SELECT 
            [對手帳號(1車)]
            , [交易類型]
            , [自身帳號]
            , [自身帳號狀態]
            , ROW_NUMBER() OVER(
                PARTITION BY [對手帳號(1車)], [交易類型], [自身帳號] 
                ORDER BY DATETIME DESC
              ) AS RN
        FROM ALERT_TXN_PAIRS
    ) t
    WHERE RN = 1
)

, AGGREGATED_PAIRS AS (
    SELECT 
        [對手帳號(1車)]
        , [交易類型]
        -- 計算此對手帳號接觸過幾個獨立的警示帳戶
        , COUNT(DISTINCT [自身帳號]) AS [涉案帳戶數]
        , SUBSTRING(
            STRING_AGG([自身帳號狀態], '') WITHIN GROUP (ORDER BY [自身帳號狀態] ASC)
            , 1, 2
          ) AS [前期金流分析]
        , STRING_AGG([原警示帳號及狀態], '/') WITHIN GROUP (ORDER BY [原警示帳號及狀態] ASC) AS [原警示帳號及狀態]
    FROM DEDUPLICATED_PAIRS
    GROUP BY [對手帳號(1車)], [交易類型]
)

, FULL_CAR1_DATA AS (
    SELECT 
        ap.[對手帳號(1車)]
        , ISNULL(ctbc.ACT_NO, w.STATUS_PBA_CODE) AS [帳戶狀態] 
        , ap.[交易類型]
        , ap.[涉案帳戶數]
        , CASE WHEN ctbc.ACT_NO IS NOT NULL THEN 'Y' ELSE '' END AS [是否為中信名單]
        , CASE WHEN low.ACCT_14 IS NOT NULL THEN 'Y' ELSE '' END AS [是否為低收入戶]
        , CASE WHEN c.[年齡] < 18 THEN 'Y' ELSE '' END AS [是否低於18歲以下]
        , CASE WHEN c.NATION_CODE NOT IN ('  ', 'TW') AND c.OCUP_CODE = '50' THEN 'Y' ELSE '' END AS [是否為外籍學生]
        , CASE WHEN c.OCUP_CODE = '73' THEN 'Y' ELSE '' END AS [是否為外籍移工]
        , CASE WHEN ps.PBA_INT_CODE IN ('3', '4', '6', '8') THEN 'Y' ELSE '' END AS [是否為薪資戶]
        , CASE WHEN f.BRH_NO IS NOT NULL THEN 'Y' ELSE '' END AS [是否為偏鄉]
        , ap.[前期金流分析]
        , ap.[原警示帳號及狀態]
        , c.OCUP_CODE
        , c.NATION_CODE
        , c.[年齡]
        , SUBSTRING(ap.[對手帳號(1車)], 1, 6) AS [局號]
        , ps.PBA_INT_CODE
    FROM AGGREGATED_PAIRS ap
    LEFT JOIN T_CUST_PS_BASE ps ON ap.[對手帳號(1車)] = ps.ACT_NO
    LEFT JOIN T_CUST_BASE c ON ps.ID = c.ID AND ps.ID_DUP_NO = c.ID_DUP_NO
    LEFT JOIN NEW_ALERT_ACCTS w ON ap.[對手帳號(1車)] = w.ACT_NO
    LEFT JOIN CTBC_ALERT_BASE ctbc ON ap.[對手帳號(1車)] = ctbc.ACT_NO
    LEFT JOIN T_LOW_INCOME_BASE low ON ap.[對手帳號(1車)] = low.ACCT_14
)


-- 3. T_TXN_PS_FINAL: 條件篩選
, T_TXN_PS_FINAL AS (
    SELECT 
        [對手帳號(1車)] AS [帳號(1車)]
        , ISNULL([帳戶狀態], '') AS [帳戶狀態]
        , [交易類型]
        , [前期金流分析]
        , [原警示帳號及狀態]
        , [是否為低收入戶]
        , [年齡]
        , [是否低於18歲以下]
        , ISNULL([OCUP_CODE], '') AS [OCUP_CODE]
        , ISNULL([NATION_CODE], '') AS [NATION_CODE]
        , [是否為外籍學生]
        , [是否為外籍移工]
        , ISNULL([PBA_INT_CODE], '') AS [PBA_INT_CODE]
        , [是否為薪資戶]
        , [局號]
        , [是否為偏鄉]
    FROM FULL_CAR1_DATA
    WHERE ([涉案帳戶數] >= 2 AND [前期金流分析] IN ('BB', 'CC'))
       OR [是否為中信名單] = 'Y'
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    [帳號(1車)]
    , [帳戶狀態]
    , [交易類型]
    , [前期金流分析]
    , [原警示帳號及狀態]
    , [是否為低收入戶]
    , [年齡]
    , [是否低於18歲以下]
    , [OCUP_CODE]
    , [NATION_CODE]
    , [是否為外籍學生]
    , [是否為外籍移工]
    , [PBA_INT_CODE]
    , [是否為薪資戶]
    , [局號]
    , [是否為偏鄉]
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL
-- ORDER BY [帳號(1車)], [交易類型];