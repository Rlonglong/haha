/*
Created: 2026-05-23
Description: 解控加分計算統整表 (包含各項加分標記與設控紀錄)
Change Log:
- 2026-05-23 [REFACT] Refactor with the clear-logical select and macros, remove cancellation logic
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_UNLOCK_SCORE_LIST_85', 
    tags=["UNLOCK_SCORE", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}
{% set target_ym = target_date[:7] %}


SELECT 
    ACT_NO
    , [解控日] 
FROM {{ source('database', 'mrt_UNLOCK_SCORE_LIST') }}
WHERE [總分] >= 85
AND TABLE_DATE = '{{ target_date }}'