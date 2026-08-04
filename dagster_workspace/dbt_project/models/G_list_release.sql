/*
Created: 2026-05-27
Description: G_list 異常帳戶清單 (跨條件提現與轉出篩選)
Change Log:
- 2026-05-27 [REFACT] 導入出入帳巨集函數，利用條件聚合單次掃描，並修復 Python 原邏輯漏洞。
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_G_LIST', 
    tags=["G_LIST", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定 (cond1_date = target_date - 1)
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}
{% set t_minus_1 = "CAST(DATEADD(DAY, -1, CAST('" ~ target_date ~ "' AS DATE)) AS DATE)" %}

-- 1. T_BASE 系列: 保持乾淨的 SELECT，進行初步過濾與型別轉換
WITH T_TXN_PS_BASE AS (
    SELECT 
        ACCT_NBR_ORI AS ACT_NO
        , FUNC_CODE
        , CAST(TXN_AMT AS BIGINT) AS TXN_AMT
        , DR_FLG
        , CAST(CPU_DATE AS DATE) AS CPU_DATE
        , CONVERT(
            DATETIME, 
            CPU_DATE + ' ' + STUFF(STUFF(RIGHT('000000' + TXN_TIME, 6), 5, 0, ':'), 3, 0, ':')
          ) AS DATETIME
    FROM {{ ref('txn_ps_net') }}
    -- 讀取近 3 天的資料
    WHERE CPU_DATE >= CAST(DATEADD(DAY, -2, CAST('{{ target_date }}' AS DATE)) AS DATE)
      AND CPU_DATE <= CAST('{{ target_date }}' AS DATE)
      AND NULLIF(LTRIM(RTRIM(ACCT_NBR_ORI)), '') IS NOT NULL
)

, T_CUST_CHG_BASE AS (
    SELECT 
        ACT_NO
        , CAST(TXN_DATE AS DATE) AS TXN_DATE
    FROM {{ source('database', 'T_CUST_CHG') }}
    WHERE FUNC_CODE = '1666'
      -- 讀取近 1 年的資料
      AND CAST(TXN_DATE AS DATE) >= DATEADD(DAY, -365, CAST('{{ target_date }}' AS DATE))
)

, T_WARNFLG_BASE AS (
    SELECT 
        ACT_NO
        , STATUS_PBA_CODE
    FROM {{ source('database', 'T_CUST_WARNINGFLG') }}
    WHERE TABLE_DATE = '{{ target_date }}'
)


-- 2. T_TXN_PS_FULL: 算出所有特徵工程 (導入 Macro，單次掃描條件聚合)
, T_TXN_PS_AGG AS (
    SELECT 
        ACT_NO
        -- 【條件1】: 當日入帳總額
, SUM(CASE WHEN CPU_DATE = {{ t_minus_1 }} 
                    AND DR_FLG = '0' 
                    AND FUNC_CODE IN {{ get_config()['G_LIST']['in_codes'] }} 
                   THEN TXN_AMT ELSE 0 END) AS C1_IN_SUM

        -- 【條件2】: 昨天提現次數與金額
        , SUM(CASE WHEN CPU_DATE = {{ t_minus_1 }} 
                    AND FUNC_CODE IN {{ get_config()['G_LIST']['withdrawal'] }} 
                   THEN 1 ELSE 0 END) AS C2_WD_CNT
        , SUM(CASE WHEN CPU_DATE = {{ t_minus_1 }} 
                    AND FUNC_CODE IN {{ get_config()['G_LIST']['withdrawal'] }} 
                   THEN TXN_AMT ELSE 0 END) AS C2_WD_SUM

        -- 【條件4】: 昨天第一筆入帳與最後一筆出帳時間
        , MIN(CASE WHEN CPU_DATE = {{ t_minus_1 }} 
                    AND DR_FLG = '0' 
                    AND FUNC_CODE IN {{ get_config()['G_LIST']['txn_in'] }} 
                   THEN DATETIME END) AS C4_FIRST_IN_DT
        , MAX(CASE WHEN CPU_DATE = {{ t_minus_1 }} 
                    AND DR_FLG = '1' 
                    AND FUNC_CODE IN {{ get_config()['G_LIST']['txn_out'] }} 
                   THEN DATETIME END) AS C4_LAST_OUT_DT

        -- 【條件5】: 近三天內特定交易總筆數
        , SUM(CASE WHEN DR_FLG IN ('0', '1') 
                    AND FUNC_CODE IN {{ get_config()['G_LIST']['txn_code'] }} 
                   THEN 1 ELSE 0 END) AS C5_TXN_CNT

        -- 【條件6】: 昨天轉出次數 (比較基準是上面的 C2_WD_CNT)
        , SUM(CASE WHEN CPU_DATE = {{ t_minus_1 }} 
                    AND DR_FLG = '1' 
                    AND FUNC_CODE IN {{ get_config()['G_LIST']['transfer_out'] }} 
                   THEN 1 ELSE 0 END) AS C6_TROUT_CNT

        -- 【條件3】(供顯示用): T-2日 22:00 ~ T-1日 03:00 提現次數
        , SUM(CASE WHEN FUNC_CODE IN {{ get_config()['G_LIST']['withdrawal'] }} 
                    AND DATETIME >= DATEADD(HOUR, 22, CAST(DATEADD(DAY, -2, CAST('{{ target_date }}' AS DATE)) AS DATETIME))
                    AND DATETIME <= DATEADD(HOUR, 3, CAST(DATEADD(DAY, -1, CAST('{{ target_date }}' AS DATE)) AS DATETIME))
                   THEN 1 ELSE 0 END) AS C3_WD_CNT_2200_0300

    FROM T_TXN_PS_BASE
    GROUP BY ACT_NO
)

-- 找出一年代碼 1666 最新日期 (給最終報表用)
, CHG_1666_LATEST AS (
    SELECT 
        ACT_NO
        , TXN_DATE
    FROM (
        SELECT 
            ACT_NO
            , TXN_DATE
            , ROW_NUMBER() OVER(PARTITION BY ACT_NO ORDER BY TXN_DATE DESC) AS RN
        FROM T_CUST_CHG_BASE
    ) t
    WHERE RN = 1
)


-- 3. T_TXN_PS_FINAL: 條件篩選 
, T_TXN_PS_FINAL AS (
    SELECT 
        a.ACT_NO
        , REPLACE(CAST({{ t_minus_1 }} AS VARCHAR(10)), '-', '') AS [名單日期]
        , ISNULL(a.C3_WD_CNT_2200_0300, 0) AS WD_CNT_2200_0300
        , w.STATUS_PBA_CODE
        , REPLACE(CAST(chg.TXN_DATE AS VARCHAR(10)), '-', '') AS [警示帳戶設定日期]
    FROM T_TXN_PS_AGG a
    LEFT JOIN CHG_1666_LATEST chg 
        ON a.ACT_NO = chg.ACT_NO
    LEFT JOIN T_WARNFLG_BASE w 
        ON a.ACT_NO = w.ACT_NO
    WHERE a.C1_IN_SUM >= 20000                                                        -- 條件1
      AND (a.C2_WD_CNT = 2 OR CAST(a.C2_WD_SUM AS FLOAT) >= CAST(a.C1_IN_SUM AS FLOAT) * 0.8) -- 條件2
      AND DATEDIFF(MINUTE, a.C4_FIRST_IN_DT, a.C4_LAST_OUT_DT) BETWEEN 0 AND 30       -- 條件4
      AND a.C5_TXN_CNT < 30                                                           -- 條件5
      AND a.C2_WD_CNT > a.C6_TROUT_CNT                                                -- 條件6
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    [名單日期]
    , ACT_NO
    , WD_CNT_2200_0300
    , STATUS_PBA_CODE
    , [警示帳戶設定日期]
    , REPLACE(CAST(CAST('{{ target_date }}' AS DATE) AS VARCHAR(10)), '-', '') + SPACE(96) + ACT_NO AS FORMATTED_TXT
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL
-- ORDER BY ACT_NO;