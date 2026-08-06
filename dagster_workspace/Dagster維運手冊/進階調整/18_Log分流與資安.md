# 18 · Log 分流與資安

**檔案**:`dagster_code/log_utils.py`、`dagster_home/dagster.yaml`

---

## 一、現在的邏輯是什麼

### 1-1 為什麼要分流

**Dagster UI 上的 Run log,只要能開啟 Dagster 網頁的人都看得到,沒有再細分權限。**

而 log 裡原本會出現這些東西:

| 內容 | 例子 |
|---|---|
| 完整檔案路徑 | `/run/media/root/D/data/fromFPP/T_TXN_PS/T_TXN_PS_20260801_ENCRYPTED.csv` |
| 完整遠端指令 | `python3.11 /home/bcp_runner/scripts/bcp_remote.py --input ... --target ...` |
| 遠端 stdout 逐行回放 | `50000 rows sent to SQL Server. Total sent: 17550000` |
| 檔名清單 | `找到: ['T_TXN_PS_20260801012819.zip', ...]` |
| 批次筆數 | `[T_ACCOUNT_ALERT] 2026-08-01 筆數 152340 已穩定` |

這些單獨看不算機密,但**合起來等於把整個系統的檔案佈局、腳本位置、資料規模攤開**。
所以現在改成兩條路:

```
context.log.info / warning / error   →  Dagster 事件 log  →  UI 上看得到（只放狀態）
log_detail / log_detail_error        →  明細 logger      →  rsyslog（有權限管控）
```

### 1-2 分流原則

| 放哪裡 | 內容 |
|---|---|
| **UI(`context.log`)** | 這一步**成功了沒**、**要不要有人介入**。不含路徑、指令、檔名、筆數 |
| **明細(`log_detail`)** | 路徑、遠端指令全文、遠端 stdout/stderr、檔名清單、筆數、完整 stack trace |

UI 上的錯誤訊息會寫「**詳見明細 log**」,提示查的人要去有權限的地方看。

### 1-3 明細寫到哪裡

由環境變數決定,優先序:

| 順位 | 環境變數 | 去向 |
|---|---|---|
| 1 | `DAGSTER_DETAIL_SYSLOG`(`host:port`) | 直接送 rsyslog(**最終形態**) |
| 2 | `DAGSTER_DETAIL_LOG_PATH` | 指定檔案,由 rsyslog 的 `imfile` 讀走 |
| 3 | (都沒設) | 預設檔案 `/app/workspace/dagster_home/logs/pipeline_detail.log` |
| 4 | (連檔案都寫不了) | 退回 stderr — ⚠️ **這個 fallback 會出現在 UI 的 stderr 分頁**,只是保命用 |

檔案模式用 `RotatingFileHandler`,單檔 50MB、保留 5 份。

### 1-4 兩個關鍵設計

**① `propagate = False`**

```python
logger.propagate = False
```

不設的話,明細會往上冒泡到 dagster 的 root logger,被寫進 process 的 stdout/stderr,
**然後被 Dagster 的 compute log 收走,又出現在 UI 的 stdout 分頁上**——分流就失效了。

**② logger 名稱不能放進 `managed_python_loggers`**

`dagster.yaml` 裡的 `managed_python_loggers` 會把指定 logger 的輸出收進**事件 log**。
`dagster_pipeline.detail` **刻意不在那個清單裡**,加進去等於前功盡棄。

### 1-5 明細訊息長什麼樣

每一則都帶 run / 資產 / 分區,方便從 rsyslog 那邊比對回 Dagster 的 Run:

```
2026-08-05 14:22:31 INFO [run=ee18f327-2e01-45a4-9665-746b941eb689 asset=file_system/T_TXN_PS_clean partition=2026-08-01] 清洗: /run/media/.../T_TXN_PS_20260801_ENCRYPTED.csv -> /run/media/.../T_TXN_PS_20260801_CLEAN.csv
```

---

## 二、想調什麼,改哪裡

### 調整 A:接上 rsyslog(正式做法)

在跑 code server 的容器環境裡加:

```yaml
# docker-compose 的 environment
environment:
  - DAGSTER_DETAIL_SYSLOG=10.10.x.x:514
```

改完**重啟容器**。設定有問題時會自動退回檔案模式,不會讓 code location 起不來。

`dagster.yaml` 裡另外還有一段被註解掉的 `dagster_handler_config`,那是把
**Dagster 自己的 WARNING 以上**也送一份到 rsyslog,跟這裡的明細 logger 是兩件事,
可以同時開。

---

### 調整 B:改明細檔案的位置或大小

```python
# log_utils.py
_DEFAULT_LOG_PATH = "/app/workspace/dagster_home/logs/pipeline_detail.log"
_MAX_BYTES = 50 * 1024 * 1024
_BACKUP_COUNT = 5
```

臨時換位置不用改程式,設 `DAGSTER_DETAIL_LOG_PATH` 即可。

---

### 調整 C:某一則訊息要在 UI 上看得到 / 看不到

改用另一個函式就好:

```python
# 要 UI 看得到（只能放狀態，不要放路徑）
context.log.info("✅ 清洗完成")

# 只進明細
log_detail(context, f"清洗: {src} -> {dst}")

# UI 一句話 + 明細留完整內容
context.log.error("❌ 入庫階段執行失敗，詳見明細 log")
log_detail_error(context, f"入庫階段失敗: {e}")
```

**新增訊息時的判斷題**:
> 「這句話印出來,能不能讓一個只看得到 UI 的人推敲出檔案放哪、腳本叫什麼、資料多大?」
> 會的話就用 `log_detail`。

---

### 調整 D:UI 上想再少一點

目前 UI 上還有 16 則 INFO(每個節點的「✅ 完成」與少數狀態)。
覺得還是太吵的話,把不需要的改成 `log_detail` 即可——
**但至少保留每個節點的成功訊息**,否則維運人員無從判斷卡在哪一步。

> 維運人員在 Run 詳細頁其實已經有時間軸顏色可以判斷成功失敗,
> INFO 只是輔助。真正不能拿掉的是 **WARNING 與 ERROR**。

---

## 三、改完怎麼驗證

1. Reload code location
2. 手動 materialize 一個節點
3. **UI 檢查**:Run 詳細頁 → Events / stdout / stderr 三個分頁
   - 不應該出現完整路徑、`python3.11 /home/bcp_runner/scripts/...`、遠端逐行輸出
   - 應該只看到「✅ xxx 完成」這類狀態
4. **明細檢查**:到容器裡看檔案有沒有寫進去
   ```bash
   tail -f /app/workspace/dagster_home/logs/pipeline_detail.log
   ```
   應該看得到路徑、指令、遠端輸出,而且每則都帶 `run=` / `asset=` / `partition=`
5. 接了 rsyslog 之後,到 rsyslog 那邊確認收得到

### 快速自我檢查

```bash
# UI 訊息裡不應該再出現這些變數
grep -rn "context\.log\." /app/workspace/dagster_code --include="*.py" \
  | grep -Ei "path|command|filename|stdout|stderr|_dir"
```

有命中就代表有訊息把明細寫回 UI 了。

---

## 四、注意事項

| 事項 | 說明 |
|---|---|
| **明細沒設好會靜靜掉到 stderr** | 那個 fallback 會出現在 UI,等於分流失效。上線前務必確認檔案或 syslog 寫得進去 |
| **`propagate = False` 不要拿掉** | 拿掉就會冒泡到 compute log,又回到 UI |
| **不要把 `dagster_pipeline.detail` 加進 `managed_python_loggers`** | 同上,加了就直接進事件 log |
| **VM1 腳本自己的輸出也要看** | 遠端腳本印在自己 stdout 的東西會經由 Pipes 回來;目前只進明細,但腳本那邊如果直接寫檔或送 syslog,要另外評估 |
| **錯誤訊息送外部前要過濾** | `slack_failure_alert` 之後如果真的接上通知管道,原始訊息含路徑,不要整段送出去 |
