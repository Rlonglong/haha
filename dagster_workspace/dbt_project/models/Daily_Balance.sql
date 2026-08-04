/*
Created: 2026-05-27
Description: 每日餘額與漲跌幅快照表
Change Log:
- 2026-05-27 [REFACT] 尊重原業務邏輯：每月 1 號自 T_CUST_PS 讀取並過濾黑名單，非 1 號自 T_TXN_PS 更新餘額。並保留新開戶防漏與除零防護。
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_DAILY_BALANCE', 
    tags=["DAILY_BALANCE", "daily_job"]
) }}

-- depends_on: {{ source('database', 'T_CUST_PS') }}

-- ======================================================================
-- 日期變數與分流設定
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}
-- 判斷是否為每月 1 號 (對應 Python: if today.day == 1)
{% set is_first_day = (target_date[-2:] == '01') %}
{% set t_minus_1 = "REPLACE(CAST(DATEADD(DAY, -1, CAST('" ~ target_date ~ "' AS DATE)) AS VARCHAR(10)), '-', '')" %}
{% set target_ym = target_date[:7] %}

-- 1. 共通區塊: 讀取昨日餘額 (df_pre)
WITH T_PREV_BALANCE_BASE AS (
    {% if is_incremental() %}
        SELECT 
            ACT_NO
            , LAST_TX_DATE
            , CAST(AVL_BAL AS FLOAT) AS AVL_BAL_PRE
        FROM {{ this }}
        WHERE TABLE_DATE = {{ t_minus_1 }}
    {% else %}
        -- 首次執行防呆
        SELECT 
            CAST('' AS VARCHAR(14)) AS ACT_NO
            , CAST('' AS VARCHAR(8)) AS LAST_TX_DATE
            , CAST(0 AS FLOAT) AS AVL_BAL_PRE
        WHERE 1=0
    {% endif %}
)


{% if is_first_day %}
-- ======================================================================
-- 【分支 A: 每月 1 號】自 T_CUST_PS 抓取基準餘額並過濾狀態
-- ======================================================================
, T_NEW_BALANCE AS (
    SELECT 
        ACT_NO
        , CAST(AVL_BAL AS FLOAT) AS AVL_BAL
        , LAST_TX_DATE
    FROM {{ source('database', 'T_CUST_PS') }}
    WHERE TABLE_DATE = '{{ target_ym }}'
      AND NULLIF(LTRIM(RTRIM(ID)), '') IS NOT NULL
      AND NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
      -- 依照原設計：只有 1 號會做黑名單狀態過濾
      AND ISNULL(STATUS_PBA_CODE, '') NOT IN {{ get_config()['DAILY_BALANCE']['exclude_pba_codes'] }}
)

{% else %}
-- ======================================================================
-- 【分支 B: 非 1 號】自 T_TXN_PS 抓取當日明細更新餘額
-- ======================================================================
, T_TXN_PS_BASE AS (
    SELECT 
        ACT_NO
        , CAST(PS_BAL AS FLOAT) AS PS_BAL
        , CPU_DATE
        , TXN_TIME
        , Seq_ID
    FROM {{ ref('txn_ps_net') }}
    WHERE CPU_DATE = REPLACE(CAST('{{ target_date }}' AS DATE), '-', '')
      AND NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
)

-- 抓取今日有交易帳戶的最後一筆餘額
, T_TODAY_LAST_TXN AS (
    SELECT 
        ACT_NO
        , PS_BAL AS AVL_BAL
        , CPU_DATE AS LAST_TX_DATE
    FROM (
        SELECT 
            ACT_NO
            , PS_BAL
            , CPU_DATE
            , TXN_TIME
            , ROW_NUMBER() OVER(PARTITION BY ACT_NO ORDER BY CPU_DATE DESC, TXN_TIME DESC) AS RN
        FROM T_TXN_PS_BASE
    ) t
    WHERE RN = 1
)

-- 聯集昨日名單與今日名單 (保留 LEFT JOIN 精神修復漏掉新開戶的問題)
, T_NEW_BALANCE AS (
    SELECT 
        a.ACT_NO
        , COALESCE(t.LAST_TX_DATE, p.LAST_TX_DATE) AS LAST_TX_DATE
        , COALESCE(t.AVL_BAL, p.AVL_BAL_PRE, 0) AS AVL_BAL
    FROM (
        SELECT ACT_NO FROM T_PREV_BALANCE_BASE
        UNION
        SELECT ACT_NO FROM T_TODAY_LAST_TXN
    ) a
    LEFT JOIN T_TODAY_LAST_TXN t ON a.ACT_NO = t.ACT_NO
    LEFT JOIN T_PREV_BALANCE_BASE p ON a.ACT_NO = p.ACT_NO
)
{% endif %}


-- ======================================================================
-- 2. 最終計算區塊: 結算今日餘額與漲跌幅
-- ======================================================================
, T_TXN_PS_FINAL AS (
    SELECT 
        n.ACT_NO
        , n.LAST_TX_DATE
        , n.AVL_BAL
        -- 確保昨日無餘額的新開戶能安全帶入 0
        , ISNULL(p.AVL_BAL_PRE, 0) AS AVL_BAL_PRE
        -- 套用巨集，修復除以零的錯誤
        , ROUND({{ safe_divide('n.AVL_BAL - ISNULL(p.AVL_BAL_PRE, 0)', 'ISNULL(p.AVL_BAL_PRE, 0)', 0) }} * 100, 2) AS [up(%)]
    FROM T_NEW_BALANCE n
    -- 與昨日餘額表關聯以計算漲跌幅
    LEFT JOIN T_PREV_BALANCE_BASE p 
        ON n.ACT_NO = p.ACT_NO
)


-- ==================== 3. 最終輸出 ====================
SELECT 
    ACT_NO
    , LAST_TX_DATE
    , AVL_BAL
    , [up(%)]
    , '{{ target_date }}' AS TABLE_DATE
FROM T_TXN_PS_FINAL
-- ORDER BY ACT_NO;