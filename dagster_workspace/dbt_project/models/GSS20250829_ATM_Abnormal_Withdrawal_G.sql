/*
Created: 2026-04-10
Description: ATM_G (特定條件異常交易清單產出)
Change Log:
- 2026-05-23 [REFACT] Refactor from python script, removing cancellation logic and twelve_to_fourteen conversion.
- 2026-04-10 [REFACT] Refactor from python script
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_ATM_G', 
    tags=["ATM_G", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定 (使用 dbt var 以支援 Dagster 排程動態傳入)
-- ======================================================================
{% set target_date = var("target_date", "2026-05-23") %}
{% set target_ym = target_date[:7] %}

-- 1. T_BASE 系列: 保持乾淨的 SELECT，進行初步過濾與型別轉換
WITH T_CUST_PS_BASE AS (
    SELECT 
        ACT_NO
        , ID
        , ID_DUP_NO
    FROM {{ ref('snp_dim_cust_act') }}
    WHERE NULLIF(LTRIM(RTRIM(ID)), '') IS NOT NULL
      AND NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
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
        , NATION_CODE
        , CASE 
            WHEN BIRTH_DATE LIKE '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]' 
            THEN YEAR(CAST('{{ target_date }}' AS DATE)) - CAST(SUBSTRING(BIRTH_DATE, 1, 4) AS INT)
            ELSE NULL 
          END AS [AGE]
    FROM {{ ref('snp_cust') }}
    WHERE NULLIF(LTRIM(RTRIM(ID)), '') IS NOT NULL
      AND TABLE_DATE = '{{ target_ym }}'
)

, T_CUST_WARNINGFLG_BASE AS (
    SELECT 
        ACT_NO
        , STATUS_PBA_CODE
    FROM {{ source('database', 'T_CUST_WARNINGFLG') }}
    WHERE TABLE_DATE = '{{ target_date }}'
)

, T_TXN_PS_BASE AS (
    SELECT
        ACT_NO
        , CPU_DATE
        , TXN_TIME
        , Seq_ID
        , FUNC_CODE
        , DR_FLG
        , CAST(TXN_AMT AS BIGINT) AS TXN_AMT
        , CAST(PS_BAL AS BIGINT) AS PS_BAL
        , CONVERT(
            DATETIME
            , CPU_DATE + ' ' + STUFF(STUFF(TXN_TIME, 5, 0, ':'), 3, 0, ':')
          ) AS DATETIME
    FROM {{ ref('txn_ps_net') }}
    WHERE NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
      AND ISNULL(TXN_NAME, '') <> '行政院發'
      AND CPU_DATE = '{{ target_date }}'
)

, T_EARLYJOB_BASE AS (
    SELECT 
        [帳號]
    FROM {{ source('database', 'T_EARLYJOB_LIST') }}
    WHERE TABLE_DATE = '{{ target_date }}'
)


-- 2. T_AGG 系列: 算出所有特徵工程 (聚合與特徵值計算)
, T_ACT_BASE AS (
    SELECT 
        ps.ACT_NO
        , c.OCUP_CODE AS [職業代號]
        , c.NATION_CODE AS [國籍]
        , c.AGE AS [年齡]
        , w.STATUS_PBA_CODE AS [帳戶狀態]
    FROM T_CUST_PS_BASE ps
    LEFT JOIN T_CUST_BASE c 
        ON ps.ID = c.ID 
        AND ps.ID_DUP_NO = c.ID_DUP_NO
    LEFT JOIN T_CUST_WARNINGFLG_BASE w 
        ON ps.ACT_NO = w.ACT_NO
)

, T_TXN_PS_BAL_AGG AS (
    SELECT 
        ACT_NO
        , CPU_DATE
        , TXN_TIME
        , PS_BAL AS [當日餘額]
    FROM (
        SELECT 
            ACT_NO
            , PS_BAL
            , CPU_DATE
            , TXN_TIME
            , ROW_NUMBER() OVER(
                PARTITION BY ACT_NO 
                ORDER BY CPU_DATE DESC, TXN_TIME DESC
              ) AS RN
        FROM T_TXN_PS_BASE
    ) t
    WHERE RN = 1
)

, T_TXN_PS_LARGE_BASE AS (
    SELECT 
        ACT_NO
        , CPU_DATE
        , TXN_TIME
        , Seq_ID
        , FUNC_CODE
        , DR_FLG
        , TXN_AMT
        , DATETIME
    FROM T_TXN_PS_BASE
    WHERE TXN_AMT > 2000
)

, T_TXN_PS_METRICS_AGG AS (
    SELECT 
        ACT_NO
        , SUM(
            CASE 
                WHEN {{ is_inward_all('ATM_G') }}
                THEN 1 
                ELSE 0 
            END
          ) AS [當日入帳次數]
        , SUM(
            CASE 
                WHEN {{ is_inward_all('ATM_G') }} 
                 AND TXN_AMT % 1000 <> 0 
                 AND RIGHT(CAST(TXN_AMT AS VARCHAR(20)), 2) <> '85'
                THEN 1 
                ELSE 0 
            END
          ) AS [當日尾數不齊之入帳次數]
        , SUM(
            CASE 
                WHEN {{ is_inward_all('ATM_G') }}
                THEN TXN_AMT 
                ELSE 0 
            END
          ) AS [當日入帳額]
        , SUM(
            CASE 
                WHEN {{ is_atm_outward_all('ATM_G') }}
                THEN TXN_AMT 
                ELSE 0 
            END
          ) AS [當日ATM出帳額]
    FROM T_TXN_PS_LARGE_BASE
    GROUP BY ACT_NO
)

-- 使用排序序號抓取下一筆交易狀態
, T_TXN_PS_PAIR_LEAD AS (
    SELECT 
        ACT_NO
        , CPU_DATE
        , TXN_TIME
        , DR_FLG
        , DATETIME AS Datetime_in
        , LEAD(DR_FLG) OVER(
            PARTITION BY ACT_NO 
            ORDER BY CPU_DATE ASC, TXN_TIME ASC
          ) AS NEXT_DR_FLG
        , LEAD(TXN_AMT) OVER(
            PARTITION BY ACT_NO 
            ORDER BY CPU_DATE ASC, TXN_TIME ASC
          ) AS AMT_out
        , LEAD(DATETIME) OVER(
            PARTITION BY ACT_NO 
            ORDER BY CPU_DATE ASC, TXN_TIME ASC
          ) AS Datetime_out
    FROM T_TXN_PS_LARGE_BASE
    WHERE {{ is_inward_all('ATM_G') }}
       OR {{ is_atm_outward_all('ATM_G') }}
)

, T_TXN_PS_PAIR_AGG AS (
    SELECT 
        ACT_NO
        , COUNT(*) AS [連續入出帳組數]
        , SUM(AMT_out) AS TOTAL_AMT_out
    FROM T_TXN_PS_PAIR_LEAD
    WHERE DR_FLG = '0' 
      AND NEXT_DR_FLG = '1'
      AND CAST(DATEDIFF(SECOND, Datetime_in, Datetime_out) AS FLOAT) / 60.0 <= 20
    GROUP BY ACT_NO
)


-- 3. T_TXN_PS_FULL: 組合所有特徵
, T_TXN_PS_FULL AS (
    SELECT 
        a.ACT_NO
        , a.[職業代號]
        , a.[國籍]
        , a.[年齡]
        , a.[帳戶狀態]
        , ISNULL(lb.[當日餘額], 0) AS [當日餘額]
        , ISNULL(m.[當日入帳次數], 0) AS [當日入帳次數]
        , ISNULL(m.[當日尾數不齊之入帳次數], 0) AS [當日尾數不齊之入帳次數]
        , ISNULL(m.[當日入帳額], 0) AS [當日入帳額]
        , ISNULL(m.[當日ATM出帳額], 0) AS [當日ATM出帳額]
        , ISNULL(ps.[連續入出帳組數], 0) AS [連續入出帳組數]
        , CASE 
            WHEN ISNULL(ps.[連續入出帳組數], 0) >= 3 
            THEN 'Y' 
            ELSE '' 
          END AS [3組以上連續入出帳]
        , CASE 
            WHEN ISNULL(ps.[連續入出帳組數], 0) = 2 
             AND ISNULL(ps.TOTAL_AMT_out, 0) >= 90000 
            THEN 'Y' 
            ELSE '' 
          END AS [2組連續入出帳且出帳額>=90000]
    FROM T_ACT_BASE a
    LEFT JOIN T_TXN_PS_BAL_AGG lb 
        ON a.ACT_NO = lb.ACT_NO
    LEFT JOIN T_TXN_PS_METRICS_AGG m 
        ON a.ACT_NO = m.ACT_NO
    LEFT JOIN T_TXN_PS_PAIR_AGG ps 
        ON a.ACT_NO = ps.ACT_NO
)


-- 4. T_TXN_PS_FINAL: 條件篩選
, T_TXN_PS_FINAL AS (
    SELECT 
        f.ACT_NO AS [帳號]
        , f.[職業代號]
        , f.[國籍]
        , f.[年齡]
        , f.[帳戶狀態]
        , f.[當日餘額]
        , f.[當日入帳次數]
        , f.[當日尾數不齊之入帳次數]
        , f.[當日入帳額]
        , f.[當日ATM出帳額]
        , f.[3組以上連續入出帳]
        , f.[2組連續入出帳且出帳額>=90000]
        , f.[連續入出帳組數]
    FROM T_TXN_PS_FULL f
    WHERE f.[當日入帳次數] >= 3
      AND f.[當日尾數不齊之入帳次數] >= 2
      AND (f.[3組以上連續入出帳] = 'Y' OR f.[2組連續入出帳且出帳額>=90000] = 'Y')
      AND f.[當日餘額] <= 10000
      AND NOT EXISTS (
          SELECT 1 
          FROM T_EARLYJOB_BASE e 
          WHERE e.[帳號] = f.ACT_NO
      )
)


-- ==================== 5. 最終輸出 ====================
SELECT 
    f.[帳號]
    , f.[職業代號]
    , f.[國籍]
    , f.[年齡]
    , f.[帳戶狀態]
    , f.[當日餘額]
    , f.[當日入帳次數]
    , f.[當日尾數不齊之入帳次數]
    , f.[當日入帳額]
    , f.[當日ATM出帳額]
    , f.[3組以上連續入出帳]
    , f.[2組連續入出帳且出帳額>=90000]
    , f.[連續入出帳組數]
    , '' AS remark
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL f
WHERE NOT EXISTS (
    SELECT 1 
    FROM {{ ref('GSS20250829_ATM_Abnormal_Withdrawal_G_earlyjob') }} e 
    WHERE e.[帳號] = f.[帳號]
);

