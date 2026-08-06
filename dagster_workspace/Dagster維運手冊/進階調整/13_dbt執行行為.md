# 13 · dbt 執行行為

**檔案**:`dagster_code/assets.py`(`_run_dbt_levels`、`CustomDbtTranslator`、兩支 `@dbt_assets`)

---

## 一、現在的邏輯是什麼

### 1-1 dbt 不是一次全跑,是「分層跑」

Dagster 不直接下 `dbt build --select A B C`,而是先算出相依層級,一層一層跑:

```python
def get_topological_levels(models, manifest):
    ...  # 從 manifest 讀相依關係，做拓撲排序，回傳 [[第1層], [第2層], ...]
```

執行時 log 會看到:
```
[拓撲分層] 共 3 層: [['txn_ps_net'], ['ATM_C2', 'ATM_E2'], ['ATM_J']]
▶ 執行第 1 層: ['txn_ps_net']
```

**每一層是一次獨立的 `docker run`**,同一層的模型放在同一個 `--select` 裡並行。

### 1-2 實際下的指令

```python
                dbt_cmd = "snapshot" if is_snapshot else "build"
                dbt_build_args = [dbt_cmd]
                dbt_build_args.append("--no-fail-fast")
                dbt_build_args.extend(["--select", " ".join(level_models)])
                dbt_build_args.append("--debug")

                if partition_date:
                    dbt_vars = {"target_date": partition_date}
                    dbt_build_args.extend(["--vars", json.dumps(dbt_vars)])

                full_command = ["dbt"] + dbt_build_args + ["--profiles-dir", "."]
```

組出來長這樣:
```
dbt build --no-fail-fast --select ATM_C2 ATM_E2 --debug --vars {"target_date": "2026-08-01"} --profiles-dir .
```

| 參數 | 作用 |
|---|---|
| `--no-fail-fast` | 同一層裡有模型失敗時,其他的繼續跑完 |
| `--debug` | 把實際 SQL 印進 log(**這是你在 UI 上看得到 SQL 的原因**) |
| `--vars` | 把 Dagster 的分區日期傳給 `var("target_date")` |
| `--profiles-dir .` | 用專案目錄下的 `profiles.yml` |

### 1-3 執行環境

```python
                pipes.run(
                    command=full_command,
                    image="dai/dagster:v2.6",
                    context=context,
                    extras={"stream_logs": True},
                    container_kwargs={
                        "working_dir": "/app/workspace/dbt_project",
                        "network": "docker-compose_vm4-network",
                        "volumes": {
                            "/data/deploy/workspace/dagster_workspace/dbt_project": {
                                "bind": "/app/workspace/dbt_project",
                                "mode": "rw",
                            }
                        }
                    }
                )
```

**每執行一層就啟動一個新容器**,跑完就丟。`dbt_project` 是掛載進去的(`rw`),
所以 `target/` 的產出會直接寫回 VM4 的磁碟。

資料庫帳密由 `docker_pipes` 的 `env` 傳入:

```python
docker_pipes = PipesDockerClient(
    env={
        "DBT_PROFILES_DIR": "/app/workspace/dbt_project",
        "DBT_DB_USER": DB_USER or "",
        "DBT_DB_PASSWORD": DB_PASS or "",
    }
)
```

### 1-4 日線與月線是兩個不同的資產

```python
@dbt_assets(
    manifest=post_office_dbt.manifest_path,
    partitions_def=daily_partitions,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=1),
    dagster_dbt_translator=CustomDbtTranslator(),
    select="tag:daily_job",                       # ← 只選日檔
)
def post_office_dbt_assets(...):
    ...
    yield from _run_dbt_levels(context, pipes, manifest, selected_models, partition_date)


@dbt_assets(
    ...
    select="tag:monthly_job",                     # ← 只選月檔
)
def post_office_dbt_monthly_assets(...):
    ...
    yield from _run_dbt_levels(..., is_snapshot=True)     # ← 注意這個 True
```

**沒有 `daily_job` 或 `monthly_job` tag 的模型,不會被任何一個資產涵蓋,永遠不會執行。**

### 1-5 自動觸發條件

```python
class CustomDbtTranslator(DagsterDbtTranslator):
    def get_automation_condition(self, dbt_resource_props):
        return AutomationCondition.eager().without(
            AutomationCondition.in_latest_time_window()
        )
```

意思是「上游一好就急著跑,而且**不限定只跑最新的時間窗**」——
所以補跑舊分區時,下游也會跟著自動補。

---

## 二、想調什麼,改哪裡

### 調整 A:換 dbt 映像檔版本

```python
                    image="dai/dagster:v2.6",       # ← 改這裡
```

⚠️ 換版本前先確認新映像裡有:`dbt-sqlserver` adapter、`ODBC Driver 18 for SQL Server`。
**建議先手動 `docker run` 跑一次 `dbt compile` 驗證,再改程式。**

---

### 調整 B:讓某一層失敗時整層立刻停

拿掉 `--no-fail-fast`:

```python
                dbt_build_args.append("--no-fail-fast")     # ← 刪掉或註解掉這行
```

**不建議。** 目前的設計是「一層裡壞一個,其他照跑」,這樣一次 Run 可以看到所有問題,
而不是修一個發現下一個。

---

### 調整 C:關掉 `--debug` 讓 log 變乾淨

```python
                dbt_build_args.append("--debug")            # ← 刪掉這行
```

⚠️ **刪掉之後,UI 的 log 裡就看不到實際 SQL 了**,只能去 `target/compiled/` 撈。
除非 log 量真的大到有問題,否則建議保留。

---

### 調整 D:傳更多變數給 dbt

```python
                if partition_date:
                    dbt_vars = {"target_date": partition_date}
                    dbt_build_args.extend(["--vars", json.dumps(dbt_vars)])
```

要多傳變數就往 `dbt_vars` 加:

```python
                    dbt_vars = {
                        "target_date": partition_date,
                        "env": "prod",
                    }
```

模型裡用 `{{ var("env", "dev") }}` 取用(第二個參數是預設值)。

---

### 調整 E:★支援月頻的一般模型(目前的限制)★

**現況**:月線固定下 `dbt snapshot`,只會執行快照。
如果新增一支 `tags=["monthly_job"]` 的**一般模型**,
**Dagster 會標成成功,但實際上什麼都沒做。**

原因在這裡:

```python
def post_office_dbt_monthly_assets(context, pipes):
    ...
    yield from _run_dbt_levels(context, pipes, manifest, selected_models, partition_date, is_snapshot=True)
```

而 `_run_dbt_levels` 裡:

```python
                dbt_cmd = "snapshot" if is_snapshot else "build"
```

**要修正的話**,把「是否為快照」的判斷從整支資產下放到每個節點。概念如下:

```python
def _run_dbt_levels(context, pipes, manifest, selected_models, partition_date):
    nodes = manifest.get("nodes", {})
    # 建一份「模型名 → 是不是 snapshot」的對照
    is_snapshot_map = {
        node["name"]: node.get("resource_type") == "snapshot"
        for node in nodes.values()
    }

    levels = get_topological_levels(selected_models, manifest)
    for level_idx, level_models in enumerate(levels):
        # 同一層裡把 snapshot 跟一般 model 分開，各下各的指令
        snapshots = [m for m in level_models if is_snapshot_map.get(m)]
        models    = [m for m in level_models if not is_snapshot_map.get(m)]

        for cmd, targets in (("snapshot", snapshots), ("build", models)):
            if not targets:
                continue
            # ... 用 cmd 與 targets 組指令並執行
```

⚠️ 這是**架構層級的改動**,會影響現有的兩支快照。改之前:
1. 先在測試環境驗證
2. 確認 `dbt snapshot --select` 對快照的選取語法沒變
3. 保留原本的重試與 `yield MaterializeResult` 邏輯

**在沒有這個需求之前,不要動它。** 需要月頻計算的話,
現階段的替代方案是「寫成日檔模型,但 SQL 裡自己判斷只在每月 1 號算」。

---

### 調整 F:改自動觸發條件

```python
    def get_automation_condition(self, dbt_resource_props):
        return AutomationCondition.eager().without(
            AutomationCondition.in_latest_time_window()
        )
```

| 想要的行為 | 改成 |
|---|---|
| 只自動跑最新分區,舊的要手動 | `return AutomationCondition.eager()` |
| 某些模型完全不自動跑 | 依 `dbt_resource_props` 的 tag 判斷後回傳 `None` |
| 全部改成手動 | `return None` |

只讓特定 tag 不自動跑的寫法:

```python
    def get_automation_condition(self, dbt_resource_props):
        if "manual_only" in dbt_resource_props.get("tags", []):
            return None
        return AutomationCondition.eager().without(
            AutomationCondition.in_latest_time_window()
        )
```

---

### 調整 G:改 backfill 一次跑幾個分區

```python
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=1),
```

改大的話一個 Run 會處理多個分區,**但 dbt 的 `--vars` 只會帶一個日期**
(程式是 `partition_date = keys[0]`),所以**改大會算錯資料**。

**不要動這個,除非你同時改掉 `_run_dbt_levels` 的日期處理邏輯。**

---

## 三、改完怎麼驗證

1. 語法檢查 → Reload code location
2. 手動 materialize 一支簡單的模型,看 log:
   - `[拓撲分層] 共 N 層` 分層合理嗎
   - `交給 Docker 的指令:` 這行的指令對嗎
   - `--vars` 的日期對嗎
3. 到 `target/compiled/` 確認編譯結果
4. 查資料庫確認資料真的有寫進去(**特別是改過 `dbt_cmd` 相關邏輯時**,
   Dagster 顯示成功不代表資料有進去)

---

## 四、注意事項

| 事項 | 說明 |
|---|---|
| **Dagster 說成功 ≠ 資料有進去** | `_run_dbt_levels` 是「指令沒有非零結束就 yield 成功」。指令本身沒做事(例如對 model 下 snapshot)也會算成功 |
| **manifest 是相依關係的唯一來源** | 分層、選取、資產產生全靠它。改完模型沒重產 manifest,行為會跟你想的不一樣 |
| **每層一個容器** | 層數多的時候容器啟動成本會累積。相依鏈很深的話考慮把中介層合併 |
| **`target/` 被多個 Run 共用** | 同時跑兩個 dbt Run 會互相覆寫 `target/`。目前靠 pool 限制避免,不要放寬 dbt 資產的併發 |
