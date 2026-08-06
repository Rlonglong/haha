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

## Step 1-B · 善用 macros,不要自己重打一長串代碼

寫 SQL 之前先看一下 `dbt_project/macros/`,裡面已經把常用的東西包好了。

### 為什麼要有這些 macro

詐欺偵測的條件裡有大量的**交易代碼清單**。以「入帳」為例,它是這樣一串:

```sql
FUNC_CODE IN ('A401','A403','A503','B403','B415','B425','B437','BB02','1520','5502'
             ,'5506','5509','1226','1462','1463','1446','1448','E446','E448','E457'
             ,'E459','E460','E462','E472','E473','E438','14A1','14A3','14AL','14H1'
             ,'14H8','14X5','E476','E480','E482','E484')
```

**三十幾個代碼,每寫一支新模型就要重打一次。** 會發生兩件事:

1. **漏打**——少一個代碼,那一類交易就整批被漏掉,而且**不會報錯**,名單少了你也不知道
2. **改起來要命**——業務端說「新增一個入帳代碼 `E490`」,你得把用到的每一支模型都翻出來改,漏一支就不一致

所以這些清單集中放在 **`macros/anti_fraud/config.sql`** 的 `get_config()` 裡,
再由 **`macros/anti_fraud/logic.sql`** 包成語意化的判斷式。

**新增或調整一個代碼,只要改 `config.sql` 一個地方,所有用到的模型下次執行就一起生效。**

### `get_config()`:代碼與門檻的註冊表

它回傳一個以**模型代號**為 key 的字典:

```sql
{% set cfg = get_config()['ATM_C2'] %}
```

目前已註冊的代號有 `ATM_C2`、`ATM_E2`、`ATM_D`、`ATM_A2`、`ATM_B3`、`3DS_SAVING`、
`FOREIGN_SMALL`、`VIRTUAL_TRANSACT`、`ATM_G`、`UNLOCK_SCORE`、`ALERT_TRADE_CAR1`、
`ATM_F`、`ATM_H`、`CTBC_OUTPUT`、`G_LIST`、`ATM_J`、`DAILY_BALANCE`、
`WARNINGFLG_B_IPDEVICE`、`RISKY_DEVICE_TRACING`、`RISKY_IP_TRACING`、
`WARNINGFLG_B_COUNTERPARTY`。

常見的 key:

| key | 內容 |
|---|---|
| `inward_codes` / `outward_codes` | 入帳 / 出帳的交易代碼清單 |
| `atm_outward_codes` | ATM 出帳代碼 |
| `inward_threshold` / `outwardthreshold` | 小額門檻金額 |
| `exclude_pba_codes` / `nouse_code` | 要排除的帳戶狀態代碼 |
| `no_control` / `long_time_memos` | 不設控的備註文字(中文,前面要加 `N`) |

直接在 SQL 裡取用:

```sql
WHERE ISNULL(STATUS_PBA_CODE, '') NOT IN {{ get_config()['ATM_J']['nouse_code'] }}
```

> 注意值是**已經包好括號的字串**(例如 `"('A401','A403')"`),
> 所以 `IN` 後面**不要再自己加括號**。

### `logic.sql`:語意化的判斷式

比起自己拼 `DR_FLG` 加一串代碼,直接用包好的:

| macro | 判斷 |
|---|---|
| `is_inward_all(model_name)` | 入帳,不排小額 |
| `is_inward_large(model_name)` | 入帳,且金額 > `inward_threshold` |
| `is_outward_all(model_name)` | 出帳,不排小額 |
| `is_outward_large(model_name)` | 出帳,且金額 > `outwardthreshold` |
| `is_atm_outward_all(model_name)` | ATM 出帳 |

用起來像這樣:

```sql
SELECT *
FROM {{ ref('txn_ps_net') }}
WHERE TABLE_DATE = '{{ target_date }}'
  AND {{ is_inward_large('ATM_C2') }}
```

**讀起來就是「ATM_C2 定義的大額入帳」**,不用去看那三十幾個代碼是什麼。

> ⚠️ **兩個要注意的地方**
>
> 1. 傳進去的是 **`get_config()` 裡的代號**,不是模型檔名。多數情況一樣,但像
>    `GSS20250514_Virtual_Txn_Control_List_1` 這支模型用的代號是 `VIRTUAL_TRANSACT`。
> 2. `is_outward_large` 讀的 key 是 **`outwardthreshold`**(沒有底線),
>    而 `is_inward_large` 讀的是 **`inward_threshold`**(有底線)。目前只有
>    `VIRTUAL_TRANSACT` 定義了 `outwardthreshold`,**對其他代號呼叫
>    `is_outward_large` 會在編譯時出錯**。要用之前先確認 `config.sql` 裡有那個 key。

### `global/` 底下的通用工具

| macro | 用途 |
|---|---|
| `safe_divide(分子, 分母, 預設值=0)` | 除法防呆。分母是 0 或 NULL 時回傳預設值,不會炸掉。**算比率一律用它** |
| `make_temp_relation(...)` | 覆寫 dbt 產生暫存表名稱的方式,把 `invocation_id` 加進表名。**這是為了讓多個 Run 同時跑時暫存表不會撞名**,不需要你手動呼叫 |

```sql
SELECT {{ safe_divide('OUT_AMT_7D', 'OUT_AMT_180D') }} AS OUT_RATIO
```

### 什麼時候該新增到 `config.sql`

| 情況 | 做法 |
|---|---|
| 業務端要調整某個代碼清單 | 改 `config.sql` 對應的 key,**一次改完全部** |
| 新模型要用現有的代碼清單 | 直接引用現有代號,**不要複製一份** |
| 新模型有自己專屬的清單 | 在 `config.sql` 新增一個代號區塊 |
| 兩支模型的清單完全一樣 | 共用同一個代號,不要各存一份 |

**判斷原則:同一個清單只應該存在於一個地方。** 一旦你發現自己在複製貼上代碼清單,就是該進 `config.sql` 的時候了。

---

## Step 2 · 如果用到新的來源表,先登記

編輯 `dbt_project/models/sources.yml`,在 `database` 底下加:

```yaml
      - name: T_SOMETHING_NEW
```

**沒登記就 `source()` 會直接編譯失敗。**

---

## Step 3 · 推上 GitLab,讓 CI/CD 同步

Dagster 是讀 `target/manifest.json` 來產生 dbt 資產的——**manifest 沒更新,你的新模型在 UI 上根本不存在**。

**這件事已經納入 GitLab CI/CD**:程式碼同步到 VM4 的時候,pipeline 會順便重新產生 manifest,你不需要自己下指令。

所以正常流程就是:

```
git add models/NEW_MODEL.sql
git commit -m "新增 NEW_MODEL 模型"
git push
      ↓
GitLab CI/CD 同步程式碼到 VM4 + 重新產生 manifest
      ↓
到 Dagster UI 做 Reload（Step 4）
```

**push 之後請確認 pipeline 有跑成功再往下走。** 如果 pipeline 紅了,manifest 就沒更新,後面 Reload 也不會看到新模型。

> 需要手動產生 manifest 的情況(一般不會走到)請看本篇最後的
> [附註 · 手動產生 manifest 與編譯 SQL](#附註--手動產生-manifest-與編譯-sql)。

---

## Step 4 · Reload code location

Dagster UI → **Catalog 右上角的 Reload definitions**(或 Deployment → Code locations → Reload)。

新資產 `NEW_MODEL` 會出現在 `fraud_detection_models` 群組。

---

## Step 5 · 驗證

1. **Catalog** 找到 `NEW_MODEL`,確認:
   - 群組是 `fraud_detection_models`
   - 分區是日檔(每天一格)
2. 點進去 → **Lineage** 分頁 → 切 **Upstream**,
   確認上游有 `txn_ps_net` 跟你用到的來源表
   (**少一條線通常代表 SQL 裡把表名寫死了**,沒用 `ref()` / `source()`)
3. 看編譯後的 SQL:
   `target/compiled/post_office_dbt/models/NEW_MODEL.sql`
   確認 `{{ target_date }}` 已代成日期、`ref` / `source` 已變成實體表名。
   **複製到 SSMS 手跑一次**,確認筆數合理——這是最快的除錯方式
4. 手動 **Materialize** 一個分區
5. 看 Run log 裡的 `[拓撲分層]`,確認它被排在正確的層(上游先跑完才輪到它)
6. 查資料庫:`SELECT COUNT(*) FROM dbo.mrt_NEW_MODEL WHERE TABLE_DATE = '2026-08-01'`
7. 隔天確認自動觸發有跑起來

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

## earlyjob 模型:下游要吃「前一天」的上游

這是**日常會遇到的操作**,不是特例,所以寫在這裡。

### 情境

有些來源會**提早在前一天晚上先送一批資料**,我們叫它 earlyjob:

```
8/1 晚上   earlyjob 檔案先到    →  ATM_C2_earlyjob  跑 partition 2026-08-01
8/2 早上   正常檔案才到          →  ATM_C2           跑 partition 2026-08-02
                                     ↑ 它要跟「8/1 晚上那批 earlyjob 的結果」比對
```

也就是說,`ATM_C2` 在跑 `2026-08-02` 這一格時,要的是 `ATM_C2_earlyjob` 的
**`2026-08-01`** 那一格,不是同一天。

**如果不特別設定,Dagster 預設是「同一天對同一天」**,`ATM_C2` 的 8/2 會去等
`ATM_C2_earlyjob` 的 8/2——但那批資料要到 8/2 晚上才會有,於是下游就卡住了。

### 怎麼設定

在 `dagster_code/assets.py` 的 `PREV_DAY_DEPS` 加一行就好:

```python
PREV_DAY_DEPS = {
    ("ATM_C2", "ATM_C2_earlyjob"),
    ("ATM_E2", "ATM_E2_earlyjob"),
    ("你的下游模型", "你的上游 earlyjob 模型"),   # ← 加這行
}
```

**格式是 `(下游模型, 上游模型)`,名稱一律用 model 檔名**(不是 alias)。

登記之後,這組配對會自動套用:

```python
TimeWindowPartitionMapping(
    start_offset=-1,                              # 上游往前挪一天
    end_offset=-1,
    allow_nonexistent_upstream_partitions=True,   # 那天沒有 earlyjob 也不擋住下游
)
```

`allow_nonexistent_upstream_partitions=True` 很重要:**earlyjob 不一定每天都有**,
沒有的日子不應該讓正常的模型跟著停擺。

### 新增一組 earlyjob 的完整步驟

```
1. 建 XXX_earlyjob.sql          ← 處理提早到的那批資料
2. 建 XXX.sql                   ← 正常模型，用 {{ ref('XXX_earlyjob') }} 引用它
3. assets.py 的 PREV_DAY_DEPS 加一行 ("XXX", "XXX_earlyjob")
4. （通常還會）在 SQL_TO_CSV_MAPPING 加兩個匯出：
      XXX_EXPORT           depends_on_dbt_model: "XXX"
      XXX_earlyjob_EXPORT  depends_on_dbt_model: "XXX_earlyjob"
5. push → CI/CD → Reload definitions
```

### 驗證有沒有生效

Reload 之後到 **Catalog → 下游模型 → Lineage 分頁 → Upstream**,
點上游的 earlyjob 節點,確認它指到的是**前一天**那一格。

更直接的驗證方式:手動 materialize 下游模型的某一天,看它會不會因為
「同一天的 earlyjob 還沒跑」而卡住——正確設定的話**不會卡**。

> ⚠️ 這是**日常維運裡少數需要動 `assets.py` 的地方**。改完記得語法檢查:
> ```bash
> python3 -m py_compile /app/workspace/dagster_code/assets.py
> ```
> 更完整的分區調整說明見 [11_Partition與日期區間](../進階調整/11_Partition與日期區間.md)。

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
[ ] 代碼清單走 get_config()，沒有自己複製貼上一長串 FUNC_CODE
[ ] 用到的來源表都在 sources.yml 登記了
[ ] push 到 GitLab，CI/CD pipeline 綠燈（manifest 已更新）
[ ] Reload definitions 成功
[ ] Lineage 分頁確認上游有接對
[ ] 看過 target/compiled 的 SQL，變數都正確帶入
[ ] 編譯後的 SQL 在 SSMS 跑過，筆數合理
[ ] 手動 materialize 一個分區成功
[ ] 資料庫裡查得到資料
[ ] 隔天確認自動觸發正常
```

---

## 附註 · 手動產生 manifest 與編譯 SQL

**一般不會走這個路徑**——manifest 由 GitLab CI/CD 在同步時自動產生(Step 3)。

以下情況才需要手動下指令:

- CI/CD pipeline 掛了,但你急著先讓 Dagster 看到新模型
- 你在 VM4 上直接改了檔案沒有走 git(**不建議,下次同步會被蓋掉**)
- 想在 push 之前先看編譯後的 SQL 長什麼樣

### 只更新 manifest

```bash
docker run --rm \
  -v /data/deploy/workspace/dagster_workspace/dbt_project:/app/workspace/dbt_project \
  -w /app/workspace/dbt_project \
  --network docker-compose_vm4-network \
  dai/dagster:v2.6 \
  dbt parse --profiles-dir .
```

### 編譯指定模型與日期(順便檢查 SQL 語法)

```bash
docker run --rm \
  -v /data/deploy/workspace/dagster_workspace/dbt_project:/app/workspace/dbt_project \
  -w /app/workspace/dbt_project \
  --network docker-compose_vm4-network \
  dai/dagster:v2.6 \
  dbt compile --select NEW_MODEL --vars '{"target_date": "2026-08-01"}' --profiles-dir .
```

`dbt compile` 會**順便更新 manifest**,所以想一次做完兩件事就用這個。

然後看產出:

```bash
cat /data/deploy/workspace/dagster_workspace/dbt_project/target/compiled/post_office_dbt/models/NEW_MODEL.sql
```

**確認三件事**:
1. `{{ target_date }}` 都變成 `2026-08-01` 了
2. `{{ ref('txn_ps_net') }}` 變成 `"DDEQDTAI"."dbo"."T_TXN_PS_NET"`
3. `{{ get_config()[...] }}` 展開成完整的代碼清單

> ⚠️ 手動跑完之後,**記得還是要把程式碼 push 上 GitLab**。
> 只在 VM4 上改的檔案,下一次 CI/CD 同步就會被覆蓋掉。
