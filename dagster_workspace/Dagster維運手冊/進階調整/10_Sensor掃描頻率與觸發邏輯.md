# 10 · Sensor 掃描頻率與觸發邏輯

**檔案**:`dagster_code/sensors.py`、`dagster_code/db_sync/sensors.py`

---

## 一、現在的邏輯是什麼

### 1-1 有哪幾支 sensor

| Sensor | 檔案 | 管什麼 |
|---|---|---|
| `daily_file_watcher_sensor` | `sensors.py` | `freq="daily"` 的表 |
| `monthly_file_watcher_sensor` | `sensors.py` | `freq="monthly"` 的表 |
| `db_sync_watcher_sensor` | `db_sync/sensors.py` | DB 直連同步表 |
| `automation_sensor` | `sensors.py` | dbt 模型與匯出的自動觸發 |
| `slack_failure_alert` | `sensors.py` | 失敗時寫 error log |

### 1-2 掃描頻率(最常被問的)

檔案 watcher 有**兩層節流**:

```
第一層：Dagster 本身的 tick 間隔
    @sensor(minimum_interval_seconds=OFF_PEAK_INTERVAL_SECONDS)  →  30 秒

第二層：程式裡自己判斷時段
    台灣時間 00:00–05:59（離峰）→ 每次 tick 都真的去掃
    其他時段              → 距上次實際掃描未滿 300 秒就直接 skip
```

**所以實際行為是**:

| 時段(台灣時間) | 多久掃一次 |
|---|---|
| **00:00 – 05:59** | **30 秒** |
| **06:00 – 23:59** | **5 分鐘(300 秒)** |

> 注意第二層不是「不 tick」,而是 tick 了但立刻 skip,所以 UI 上會看到很多
> `一般時段，距上次執行僅 N 秒，略過。` 的 tick——**這是正常的**。

相關程式(`sensors.py` 第 28–31 行):

```python
FILE_STABLE_SECONDS = 60
TW_TZ = timezone(timedelta(hours=8))
OFF_PEAK_INTERVAL_SECONDS = 30
PEAK_INTERVAL_SECONDS = 300
```

以及每支 sensor 裡的:

```python
    is_off_peak = (0 <= current_hour < 6)
    if not is_off_peak:
        elapsed = now_tw.timestamp() - last_run_ts
        if elapsed < PEAK_INTERVAL_SECONDS:
            yield SkipReason(f"一般時段，距上次執行僅 {int(elapsed)} 秒，略過。")
            return
```

### 1-3 掃到檔案之後的完整判斷流程

```
對每一張表：
  ① manual_only=True？          → 跳過這張表
  ② freq 跟這支 sensor 不符？    → 跳過這張表
  ③ 決定監控目錄：
        use_ftp_fetch=True  → 叫 VM1 執行 list_ftp_remote.py 列 FTP 目錄
        use_ftp_fetch=False → SSH 到 VM1 ls 本地目錄
  ④ 決定副檔名：use_ftp_zip=True 看 .zip，其他看 .csv

  對目錄裡每個檔案：
  ⑤ 副檔名不符？                 → 跳過
  ⑥ 本次已派滿 3 個？            → 停止，剩下的留給下一輪
  ⑦ 檔案 mtime 距現在 < 60 秒？   → 判定「還在寫入中」，跳過
  ⑧ 檔名抓不到日期？             → 跳過
        daily   用 (\d{8}) → 2026-08-01
        monthly 用 (\d{6}) → 2026-08-01（固定 1 號）
  ⑨ 這個「表名_日期」已處理過？   → 跳過
  ⑩ 通過 → 產生一個 RunRequest，涵蓋這張表所有有開的節點
```

### 1-4 去重機制(cursor)

Sensor 用 cursor 記住做過什麼,格式:

```
<上次實際掃描的時間戳>|<已處理清單的 JSON>
```

例如:`1754380800.0|["T_TXN_PS_2026-08-01", "T_CUST_2026-08-01"]`

**同一個「表名_日期」永遠只會派一次工。** 要重跑就到 UI 上 Reset cursor,或直接手動 materialize。

### 1-5 單次上限與「連續處理」機制

一個 tick 最多派 3 個 Run(`max_files = 3`)。

有一段容易看漏的邏輯:

```python
    if len(requests) >= max_files:
        next_ts = last_run_ts        # ★ 故意不推進時間戳
    else:
        next_ts = now_tw.timestamp()
```

**達到上限時故意不更新時間戳**,這樣下一個 tick(30 秒後)不會被 300 秒的節流擋下來,
可以立刻接著處理剩下的檔案。等到某次沒派滿 3 個(代表清空了)才重新開始倒數。

### 1-6 db_sync sensor 的邏輯

DB 沒有檔案 mtime,所以改看**筆數穩定**:

```
每 300 秒（@sensor 的 minimum_interval_seconds）查一次來源 DB
   ↓
以 sensor_watch_col 前 8 碼（daily）或 6 碼（monthly）分組，取得各批次筆數
   ↓
筆數跟上次不同？ → 更新記錄，重新計時
   ↓ 相同
穩定時間 >= DB_SYNC_STABLE_SECONDS(300)？ → 否 → 繼續等
   ↓ 是
（若首次看到至今 > DB_SYNC_STALL_WARN_SECONDS(7200) → 發 warning）
   ↓
觸發該分區
```

---

## 二、想調什麼,改哪裡

### 調整 A:改掃描頻率

**改離峰的頻率**(目前 30 秒):

```python
# sensors.py 第 30 行
OFF_PEAK_INTERVAL_SECONDS = 30      # ← 改這個
```

⚠️ 這個值同時是 `@sensor(minimum_interval_seconds=...)`,也就是 **tick 的下限**。
把它調大,離峰跟一般時段的實際掃描間隔都會受影響(因為一般時段也不可能掃得比 tick 還密)。

**改一般時段的頻率**(目前 300 秒 = 5 分鐘):

```python
# sensors.py 第 31 行
PEAK_INTERVAL_SECONDS = 300         # ← 改這個。想改成 10 分鐘就填 600
```

這個最單純,只影響非離峰時段,改完 Reload 即可。

---

### 調整 B:改離峰時段的範圍

目前是台灣時間 00:00–05:59。程式在**兩支 sensor 裡各有一份**,要一起改:

```python
    is_off_peak = (0 <= current_hour < 6)      # ← daily_file_watcher_sensor 裡
    ...
    is_off_peak = (0 <= current_hour < 6)      # ← monthly_file_watcher_sensor 裡
```

| 想要的時段 | 改成 |
|---|---|
| 00:00–07:59 | `(0 <= current_hour < 8)` |
| 22:00–05:59(跨午夜) | `(current_hour >= 22 or current_hour < 6)` |
| 全天都用高頻 | `is_off_peak = True` |

> **建議**:改之前先想清楚為什麼要密集掃。掃描本身要 SSH 到 VM1 列目錄,
> 表數量多的時候每次 tick 都有成本。

---

### 調整 C:改「檔案還在寫入中」的判定

目前:mtime 距現在不到 60 秒就視為還在寫,先跳過。

```python
# sensors.py 第 28 行
FILE_STABLE_SECONDS = 60            # ← 改這個
```

| 情況 | 建議值 |
|---|---|
| 對方傳很大的檔、常常抓到半成品 | 調大到 `120` ~ `300` |
| 檔案很小、想快點處理 | 可以降到 `30`,但**不建議低於 30** |

⚠️ 調太小的風險:抓到寫到一半的檔案 → 清洗或 BCP 失敗,而且該日期已進 cursor,
**不會自動重試**,要人工介入。

---

### 調整 D:改一次派幾個 Run

目前一個 tick 最多 3 個。**兩支 sensor 裡各有一份**:

```python
    max_files = 3          # ← daily 裡
    ...
    max_files = 3          # ← monthly 裡
```

| 想要 | 改成 | 副作用 |
|---|---|---|
| 積壓時消化快一點 | `5` ~ `10` | 佇列會塞更多 Run,但同時執行數仍受 `max_concurrent_runs: 5` 限制 |
| 更保守 | `1` | 一次只處理一個檔案,追進度會很慢 |

> 想真正提升吞吐量,應該調的是 `dagster.yaml` 的併發設定,
> 見 [12_併發_重試_資源池](./12_併發_重試_資源池.md)。

---

### 調整 E:改檔名的日期解析規則

目前:

```python
    # daily
    date_regex=r'(\d{8})'                       # 抓 8 碼數字
    def _daily_partition(match):
        raw_date = match.group(1)
        return f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"

    # monthly
    date_regex=r'(\d{6})'                       # 抓 6 碼數字
    def _monthly_partition(match):
        raw_date = match.group(1)
        return f"{raw_date[:4]}-{raw_date[4:6]}-01"
```

**已知風險**:`re.search` 抓的是檔名裡**第一段**符合的數字。
如果檔名長得像 `T_2024_REPORT_20260801.csv`,daily 的 `(\d{8})` 不會誤抓(因為 `2024` 只有 4 碼),
但像 `T_20240101_20260801.csv` 這種就會抓到**前面那個**日期。

要改成只抓結尾的日期:

```python
    date_regex=r'(\d{8})(?!.*\d{8})'       # 抓最後一段 8 碼數字
```

改完務必用實際檔名測過。

---

### 調整 F:讓某張表完全不自動觸發

不用改程式,在 `table_mapping.py` 該表加一行:

```python
    "manual_only": True,
```

效果:sensor 跳過、兩個批次 job 也不涵蓋,只能手動 materialize。
**`freq` 還是要照實填**——不填會讓分區變成月檔、檔名日期變成 `YYYYMM`。

---

### 調整 G:改 db_sync 的穩定判定

```python
# db_sync/config.py 第 2、4 行
DB_SYNC_STABLE_SECONDS = 300        # 筆數要穩定多久才算到齊
DB_SYNC_STALL_WARN_SECONDS = 7200   # 超過多久還沒穩定就發警告
```

輪詢頻率則是:

```python
# db_sync/sensors.py
@sensor(
    target=_build_db_sync_selection(),
    minimum_interval_seconds=300,       # ← 改這個
)
```

---

### 調整 H:把失敗告警真的送出去

目前 `slack_failure_alert` **只寫 log,沒有真的送 Slack**:

```python
@run_failure_sensor
def slack_failure_alert(context):
    error_msg = context.failure_event.message
    job_name = context.dagster_run.job_name
    context.log.error(f"🚨 [警報] 任務 {job_name} 徹底失敗！通知相關人員檢查。\n原因: {error_msg}")
```

要真的送出通知,在這個函式裡加上實際的發送邏輯(webhook、SMTP 等)。
注意:**錯誤訊息可能含有路徑或資料內容,送到外部前請先過濾。**

---

## 三、改完怎麼驗證

1. **語法檢查**
   ```bash
   python3 -m py_compile /app/workspace/dagster_code/sensors.py
   ```
2. **Reload code location**,確認狀態綠色
3. **Automation** → 點進該 sensor → 看 **Tick history**
   - 頻率對不對:看相鄰兩筆「真的有掃描」的 tick 間隔
   - 內容對不對:點開 tick log,應該看到每張表的
     `[表名] 監控目錄: ... 找到: [...]`
4. **想立刻驗證而不等排程**:sensor 詳細頁通常有 **Test / Evaluate** 功能,
   可以手動觸發一次 tick 看結果(不會真的送出 Run,依版本而異)

---

## 四、改動時的注意事項

| 事項 | 說明 |
|---|---|
| **兩支 sensor 各有一份** | `is_off_peak` 與 `max_files` 是複製貼上的兩份,只改一邊會不一致 |
| **cursor 不會因為改設定而重置** | 改完頻率不影響已處理清單。要重跑舊檔案得另外 Reset cursor |
| **調頻率不等於調吞吐量** | 真正的瓶頸通常是 `max_concurrent_runs` 與每個資產的 pool 上限 |
| **離峰時段是寫死的台灣時間** | `TW_TZ = timezone(timedelta(hours=8))`,不會跟著系統時區跑 |
