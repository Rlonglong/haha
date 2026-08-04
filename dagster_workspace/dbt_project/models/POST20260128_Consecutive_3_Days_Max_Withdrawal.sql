/*
Created: 2026-03-11
Description: 
Change Log:
- 2026-05-23 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
- 2026-03-11 [REFACT] Refactor from python script
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_3DS_SAVING', 
    tags=["3DS_SAVING", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定 (使用 dbt var 以支援 Dagster 排程動態傳入)
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}
{% set target_ym = target_date[:7] %}

-- 1. T_BASE 系列: 保持乾淨的 SELECT，進行初步過濾與型別轉換
WITH T_CUST_PS_BASE AS (
    SELECT 
        ACT_NO
        , ID
        , ID_DUP_NO
    FROM {{ ref('snp_dim_cust_act') }}
    WHERE NULLIF(LTRIM(RTRIM(ID)), '') IS NOT NULL
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
        , NULLIF(LTRIM(RTRIM(BIRTH_DATE)), '') AS BIRTH_DATE
        , NULLIF(LTRIM(RTRIM(HAND_TEL)), '') AS HAND_TEL
        , NULLIF(LTRIM(RTRIM(EMAIL)), '') AS EMAIL
    FROM {{ ref('snp_cust') }}
    WHERE TABLE_DATE = '{{ target_ym }}'
      AND (
          (LEN(ID) = 10 AND ID LIKE '[A-Z][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]')
          OR (LEN(ID) = 10 AND ID LIKE '[A-Z][A-Z][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]')
      )
)

, T_CUST_PS_CHG_BASE AS (
    SELECT
        ACT_NO
        , NULLIF(LTRIM(RTRIM(HAND_TEL)), '') AS HAND_TEL
        , NULLIF(LTRIM(RTRIM(EMAIL)), '') AS EMAIL
        , TRY_CONVERT(DATE, TXN_DATE) AS TXN_DATE
    FROM {{ source('database', 'T_CUST_PS_CHG') }}
    WHERE ACT_NO IS NOT NULL
      AND LEN(ACT_NO) = 12
      AND IS_VALID_ACT_FLG = 'Y'
      AND TABLE_DATE = '{{ target_ym }}'
)

, T_TXN_PS_BASE AS (
    SELECT
        CPU_DATE
        , TXN_TIME
        , Seq_ID
        , ACT_NO
        , TXN_AMT
        , DR_FLG
        , FUNC_CODE
        , PS_BAL
    FROM {{ ref('txn_ps_net') }}
    WHERE CPU_DATE >= DATEADD(DAY, -3, CAST('{{ target_date }}' AS DATE)) 
      AND CPU_DATE <= DATEADD(DAY, -1, CAST('{{ target_date }}' AS DATE))
)

, T_MAX_WITHDRAWALS_FOR_3DS_SAVING_BASE AS (
    SELECT
        [帳號]
    FROM {{ source('database', 'T_MAX_WITHDRAWALS_FOR_3DS_SAVING') }}
    WHERE CPU_DATE = DATEADD(DAY, -2, CAST('{{ target_date }}' AS DATE)) 
       OR CPU_DATE = DATEADD(DAY, -1, CAST('{{ target_date }}' AS DATE))
)


-- 2. T_TXN_PS_FULL: 算出所有特徵工程 (此處先做 JOIN 並建立排序特徵)
, T_CUST_AGG AS (
    SELECT
        ID
        , ID_DUP_NO
        , HAND_TEL
        , EMAIL
        , TRY_CONVERT(DATE, BIRTH_DATE) AS BIRTH_DATE
        , YEAR(CAST('{{ target_date }}' AS DATE)) - YEAR(TRY_CONVERT(DATE, BIRTH_DATE)) AS AGE
        , FORMAT(TRY_CONVERT(DATE, BIRTH_DATE), 'yyyyMMdd') AS BIRTH_DATE_STR
    FROM T_CUST_BASE
    WHERE BIRTH_DATE IS NOT NULL
)

, CHG_HANDTEL_AGG AS (
    SELECT 
        ACT_NO
        , HAND_TEL
    FROM (
        SELECT 
            ACT_NO
            , HAND_TEL
            , ROW_NUMBER() OVER(
                PARTITION BY ACT_NO 
                ORDER BY TXN_DATE DESC
              ) AS RN
        FROM T_CUST_PS_CHG_BASE
        WHERE HAND_TEL IS NOT NULL
    ) t
    WHERE RN = 1
)

, CHG_EMAIL_AGG AS (
    SELECT 
        ACT_NO
        , EMAIL
    FROM (
        SELECT 
            ACT_NO
            , EMAIL
            , ROW_NUMBER() OVER(
                PARTITION BY ACT_NO 
                ORDER BY TXN_DATE DESC
              ) AS RN
        FROM T_CUST_PS_CHG_BASE
        WHERE EMAIL IS NOT NULL
    ) t
    WHERE RN = 1
)

, T_TXN_PS_ATM_AGG AS (
    SELECT 
        ACT_NO
        , COUNT(CPU_DATE) AS ATM_DAYS
    FROM (
        SELECT 
            ACT_NO
            , CPU_DATE
            , SUM(TXN_AMT) AS DAILY_WITHDRAWAL
        FROM T_TXN_PS_BASE
        WHERE {{ is_atm_outward_all('3DS_SAVING') }}
        GROUP BY ACT_NO, CPU_DATE
    ) t
    WHERE DAILY_WITHDRAWAL >= 100000
    GROUP BY ACT_NO
)

, T_TXN_PS_BAL_AGG AS (
    SELECT 
        ACT_NO
        , CPU_DATE
        , TXN_TIME
        , PS_BAL
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
        WHERE CPU_DATE = DATEADD(DAY, -1, CAST('{{ target_date }}' AS DATE))
    ) t
    WHERE RN = 1 
)

, T_TXN_PS_FULL AS (
    SELECT 
        ps.ACT_NO AS [帳號]
        , c.BIRTH_DATE_STR AS [生日]
        , c.AGE
        , COALESCE(ht.HAND_TEL, c.HAND_TEL) AS [手機號碼]
        , COALESCE(em.EMAIL, c.EMAIL) AS [電子信箱]
        , ISNULL(a.ATM_DAYS, 0) AS ATM_DAYS
        , ISNULL(b.PS_BAL, 0) AS YESTERDAY_BAL
    FROM T_CUST_PS_BASE ps
    INNER JOIN T_CUST_AGG c
        ON ps.ID = c.ID 
        AND ps.ID_DUP_NO = c.ID_DUP_NO
    LEFT JOIN T_TXN_PS_BAL_AGG b 
        ON ps.ACT_NO = b.ACT_NO
    LEFT JOIN T_TXN_PS_ATM_AGG a 
        ON ps.ACT_NO = a.ACT_NO
    LEFT JOIN EXNP_HANDTEL_AGG ht 
        ON ps.ACT_NO = ht.ACT_NO
    LEFT JOIN EXNP_EMAIL_AGG em 
        ON ps.ACT_NO = em.ACT_NO
)


-- 3. T_TXN_PS_FINAL: 條件篩選
, T_TXN_PS_FINAL AS (
    SELECT 
        f.[帳號]
        , f.[生日]
        , f.[手機號碼]
        , f.[電子信箱]
        , f.[手機號碼] + REPLICATE(' ', 10) AS [concat]
    FROM T_TXN_PS_FULL f
    WHERE f.AGE BETWEEN 60 AND 75
      AND f.ATM_DAYS >= 3
      AND f.YESTERDAY_BAL >= 3000000
      AND NOT EXISTS (
          SELECT 1 
          FROM T_MAX_WITHDRAWALS_FOR_3DS_SAVING_BASE h 
          WHERE h.[帳號] = f.[帳號]
      )
)

-- 若需要 TXT 輸出給電子科，可用 FINAL_DIG_IMF
, DIG_IMF_FINAL AS (
    SELECT HAND_TEL + REPLICATE(' ', 10) AS concat
    FROM FINAL_LIST
    WHERE HAND_TEL IS NOT NULL
    ORDER BY 帳號
)


-- ==================== 4. 最終輸出 ====================
SELECT 
    [帳號]
    , [生日]
    , [手機號碼]
    , [電子信箱]
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL
-- ORDER BY [帳號];

-- -- 最終輸出（SELECT 對應 Excel / TXT）
-- SELECT 
--     concat
--     '{{ target_date }}' AS TABLE_DATE
-- FROM DIG_IMF_FINAL;