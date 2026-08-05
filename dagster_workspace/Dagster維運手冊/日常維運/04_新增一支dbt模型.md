# 04 · 新增一支 dbt 模型(SQL)

**做完這篇,一支新的 SQL 就會在上游資料到齊後自動跑出名單。**

以下用 `NEW_MODEL` 當範例,產出的資料庫實體表叫 `mrt_NEW_MODEL`。

---

## 開始前:三個名字要先分清楚

| 名字 | 是什麼 | 用在哪 |
|---|---|---|
| **檔名** `NEW_MODEL.sql` | 決定 Dagster 上的資產名稱 | `ref()`、`depends_on_dbt_model` |
| **alias** `mrt_NEW_MODEL` | 資料庫裡實際的表名 | `source()`、匯出的 `source_table`、SSMS 查詢 |
| **tag** `daily_job` | 決定被哪個排程涵蓋 | **沒填就永遠不會執行** |

**檔名取好之後不要隨便改**,改了等於換一個新資產,歷史紀錄會斷掉。

---

## Step 1 · 建立 SQL 檔

路徑:`dagster_workspace/dbt_project/models/NEW_MODEL.sql`

(中介表放 `models/intermediate/`,快照放 `snapshots/`,見本篇最後一節)

```sql
/*
Created: 2026-08-05
Description: 這支模型在做什麼（一句話講清楚業務目的）
Change Log:
- 2026-08-05 [員編] 初版
*/

-- dbt config
{{ config(
    materialized='incremental',
    alias='mrt_NEW_MODEL',
    tags=["NEW_MODEL", "daily_job"]
) }}

-- ======================================================================
-- 日期變數設定（由 Dagster 以 --vars 帶入分區日期）
-- ======================================================================
{% set target_date = var("target_date", "2026-02-01") %}


-- 1. 先把要用的欄位撈進來
WITH T_BASE AS (
    SELECT
        ACT_NO
        , TXN_AMT
        , CPU_DATE
    FROM {{ ref('txn_ps_net') }}                    -- 引用別的模型用 ref()
    WHERE TABLE_DATE = '{{ target_date }}'
      AND NULLIF(LTRIM(RTRIM(ACT_NO)), '') IS NOT NULL
)


-- 2. 關聯其他資料
, T_JOINED AS (
    SELECT
        b.ACT_NO
        , b.TXN_AMT
        , ISNULL(c.STATUS_PBA_CODE, '') AS STATUS_PBA_CODE
    FROM T_BASE b
    LEFT JOIN {{ source('database', 'T_CUST_WARNINGFLG') }} c   -- 引用來源表用 source()
        ON b.ACT_NO = c.ACT_NO
       AND c.TABLE_DATE = '{{ target_date }}'
)


-- 3. 最終輸出
SELECT
    ACT_NO
    , TXN_AMT
    , STATUS_PBA_CODE
    , '{{ target_date }}' AS TABLE_DATE        -- ★一定要有，下游與匯出都靠它篩資料★
FROM T_JOINED
WHERE TXN_AMT >= 10000
```

### 必守規則

| 規則 | 不遵守會怎樣 |
|---|---|
| `tags` 一定要有 `daily_job` 或 `monthly_job` | **模型永遠不會被執行**(Dagster 是用 `tag:daily_job` 去選的) |
| 日期一律用 `var("target_date")` | 拿不到 Dagster 傳進來的分區日期 |
| 輸出要有 `TABLE_DATE` 欄位 | 下游模型與匯出都篩不到資料 |
| 引用模型用 `ref('檔名')`,引用來源表用 `source('database', '實體表名')` | 寫死表名的話 Dagster 看不到相依關係,**執行順序會亂掉** |
| `alias` 建議用 `mrt_` 開頭 | 不給的話實體表名就等於檔名 |
| 第二個 tag 放業務代號 | 方便之後用 `dbt build --select tag:XXX` 挑著跑 |

### 關於增量(incremental)

全專案的模型都是 `materialized='incremental'`。

| 寫法 | 行為 | 重跑同一天會怎樣 |
|---|---|---|
| 只寫 `materialized='incremental'` | 走預設 append | **資料會重複** |
| 加 `unique_key` + `incremental_strategy='delete+insert'` | 先刪後插 | 安全,可重複跑 |

**會需要重跑的模型,強烈建議用第二種**:

```sql
{{ config(
    materialized='incremental',
    unique_key='ACT_NO',
    incremental_strategy='delete+insert',
    alias='mrt_NEW_MODEL',
    tags=["NEW_MODEL", "daily_job"]
) }}
```

---

## Step 2 · 如果用到新的來源表,先登記

編輯 `dbt_project/models/sources.yml`,在 `database` 底下加:

```yaml
      - name: T_SOMETHING_NEW
```

**沒登記就 `source()` 會直接編譯失敗。**

---

## Step 3 · 重新產生 manifest ★最容易漏的一步★

Dagster 是讀 `target/manifest.json` 來產生 dbt 資產的。**manifest 沒更新,你的新模型在 UI 上根本不存在。**

在 VM4 上執行:

```bash
docker run --rm \
  -v /data/deploy/workspace/dagster_workspace/dbt_project:/app/workspace/dbt_project \
  -w /app/workspace/dbt_project \
  --network docker-compose_vm4-network \
  dai/dagster:v2.6 \
  dbt parse --profiles-dir .
```

> 用 `dbt compile` 也可以,而且會順便幫你檢查 SQL 語法、產出編譯後的 SQL 可以直接看。

---

## Step 4 · 先看編譯後的 SQL 對不對

```bash
docker run --rm \
  -v /data/deploy/workspace/dagster_workspace/dbt_project:/app/workspace/dbt_project \
  -w /app/workspace/dbt_project \
  --network docker-compose_vm4-network \
  dai/dagster:v2.6 \
  dbt compile --select NEW_MODEL --vars '{"target_date": "2026-08-01"}' --profiles-dir .
```

然後看:

```bash
cat /data/deploy/workspace/dagster_workspace/dbt_project/target/compiled/post_office_dbt/models/NEW_MODEL.sql
```

**確認三件事**:
1. `{{ target_date }}` 都變成 `2026-08-01` 了
2. `{{ ref('txn_ps_net') }}` 變成 `"DDEQDTAI"."dbo"."T_TXN_PS_NET"`
3. `{{ source(...) }}` 變成正確的實體表名

**把這段 SQL 複製到 SSMS 手動跑一次**,確認筆數合理、欄位正確。這是最快的除錯方式。

---

## Step 5 · Reload code location

Dagster UI → **Deployment → Code locations → Reload**。

新資產 `NEW_MODEL` 會出現在 `fraud_detection_models` 群組。

---

## Step 6 · 驗證

1. **Assets** 頁面找到 `NEW_MODEL`,確認:
   - 群組是 `fraud_detection_models`
   - 分區是日檔(每天一格)
   - 相依圖上游有 `txn_ps_net` 跟你用到的來源表
2. 手動 **Materialize** 一個分區
3. 看 Run log 裡的 `[拓撲分層]`,確認它被排在正確的層
   (上游先跑完才輪到它)
4. 查資料庫:`SELECT COUNT(*) FROM dbo.mrt_NEW_MODEL WHERE TABLE_DATE = '2026-08-01'`
5. 隔天確認自動觸發有跑起來

### 自動觸發是怎麼發生的

dbt 模型的自動條件是「**上游任一分區更新就跑**」(`AutomationCondition.eager()`),
由 `default_automation_condition_sensor` 每 30 秒評估一次。

所以只要 `database/T_XXX` 或上游模型跑完,你的新模型 30 秒內就會自己排隊。**不需要另外設排程。**

---

## 中介表(intermediate)

放在 `models/intermediate/`,規則跟一般模型一樣,差別只有:

| 差別 | 說明 |
|---|---|
| 群組 | 自動歸到 `intermediate_tables`(`dbt_project.yml` 已設定) |
| alias 慣例 | 不加 `mrt_` 前綴,例如 `T_TXN_PS_NET` |
| 增量策略 | 建議一定要設 `unique_key` + `delete+insert`,因為常被重跑 |

---

## 快照(snapshot)

放在 `snapshots/`,語法不同:

```sql
{% snapshot snp_new %}

{{
    config(
      target_schema='snapshots',
      unique_key='SOME_PK',
      strategy='check',
      updated_at='TABLE_DATE',
      check_cols=['COL_A', 'COL_B'],
      invalidate_hard_deletes=True,
      tags=["snapshot", "monthly_job"]      -- ★monthly_job 必加★
    )
}}

{% set target_date = var("target_date", "") %}

SELECT *
FROM {{ source('database', 'T_XXX') }}
WHERE 1=1
{% if target_date != "" %}
    AND TABLE_DATE = '{{ target_date }}'
{% else %}
    AND TABLE_DATE = (SELECT MAX(TABLE_DATE) FROM {{ source('database', 'T_XXX') }})
{% endif %}

{% endsnapshot %}
```

快照一律是月頻。

---

## ⚠️ 目前的限制:不能新增「月頻的一般模型」

月線執行的指令固定是 `dbt snapshot`,只會跑快照。
**如果你新增一支 `tags=["monthly_job"]` 的一般模型,Dagster 會顯示成功,但實際上什麼都沒做。**

要支援月頻模型必須先改 `assets.py`,詳見 [進階調整/13_dbt執行行為](../進階調整/13_dbt執行行為.md)。

---

## 完成檢查表

```
[ ] SQL 檔建好，檔頭有 Created / Description / Change Log
[ ] config 有 alias 與 tags（含 daily_job 或 monthly_job）
[ ] 日期用 var("target_date")
[ ] 輸出有 TABLE_DATE 欄位
[ ] 用到的來源表都在 sources.yml 登記了
[ ] 重新產生 manifest（dbt parse）
[ ] 看過 target/compiled 的 SQL，變數都正確帶入
[ ] 編譯後的 SQL 在 SSMS 跑過，筆數合理
[ ] Reload code location 成功
[ ] 手動 materialize 一個分區成功
[ ] 資料庫裡查得到資料
[ ] 隔天確認自動觸發正常
```
