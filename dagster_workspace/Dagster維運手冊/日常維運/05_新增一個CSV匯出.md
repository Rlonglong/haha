# 05 · 新增一個 CSV 匯出

**做完這篇,dbt 模型跑完之後會自動把名單匯成 CSV(可選送到 FTP)。**

---

## ⚠️ 先講最重要的一件事:匯出的是「解密後的明文」

匯出流程會把加密欄位(帳號、身分證)還原成明文寫進 CSV。
**所以能不落地就不要落地** ——設定要送 FTP 的話,就不要再留一份在 VM1 磁碟上。

系統目前的行為:

| 設定 | 行為 |
|---|---|
| 只給 `output_folder` | 寫檔到 VM1 |
| 只給 `ftp_remote_path` | 直接送 FTP,不落地 |
| **兩個都給** | **只送 FTP,不落地**(自動忽略 `output_folder`,log 會註明) |
| 兩個都給 + `keep_local_copy: True` | 兩邊都做,**log 會出現警告** |

`keep_local_copy` 是給測試用的逃生門。**上線前務必移除。**

---

## Step 1 · 確認上游模型已經存在

匯出是掛在 dbt 模型後面的,所以要先有模型。
你需要知道兩個名字:

| 要填的欄位 | 用哪個名字 | 範例 |
|---|---|---|
| `depends_on_dbt_model` | 模型的**檔名** | `ATM_C2` |
| `source_table` | 資料庫的**實體表名(alias)** | `mrt_ATM_C2` |

搞錯的話:填錯 `depends_on_dbt_model` → 永遠不會自動觸發;填錯 `source_table` → 執行時查無此表。

---

## Step 2 · 在 `table_mapping.py` 的 `SQL_TO_CSV_MAPPING` 新增設定

檔案位置:`dagster_workspace/dagster_code/table_mapping.py`(在檔案下半部)

### 情境 A:只在 VM1 產生檔案(內部使用)

```python
"mrt_NEW_MODEL_EXPORT": {
    # 1. 資料來源
    "source_table": "mrt_NEW_MODEL",          # 資料庫實體表名
    "date_column": "TABLE_DATE",

    # 2. 輸出
    "output_folder": "/run/media/root/D/data/PostBS/Project_output/99_new_model/",
    "template": "NEW_MODEL_EXPORT_{date}.csv",
    "freq": "daily",
    "delimiter": ",",
    "encoding": "utf-8-sig",                  # 給 Excel 開的 BOM

    # 3. 容錯
    "retries": 3,
    "retry_delay_sec": 60,

    # 4. 等哪支模型跑完（用檔名）
    "depends_on_dbt_model": "NEW_MODEL",

    # 5. 要還原成明文的欄位
    "decrypt_fields": ["帳號"],
},
```

### 情境 B:直接送 FTP 給對方(建議的正式做法)

```python
"mrt_NEW_MODEL_EXPORT": {
    "source_table": "mrt_NEW_MODEL",
    "date_column": "TABLE_DATE",

    # 只給 ftp_remote_path，不落地
    "ftp_remote_path": "/data/PostBS/Project_output/99_new_model/",
    "template": "NEW_MODEL_EXPORT_{date}.csv",
    "freq": "daily",
    "delimiter": ",",
    "encoding": "utf-8-sig",

    "retries": 3,
    "retry_delay_sec": 60,
    "depends_on_dbt_model": "NEW_MODEL",
    "decrypt_fields": ["帳號"],
},
```

> `ftp_remote_path` 結尾是 `/` 的話,系統會自動把 `template` 算出來的檔名接上去。
> 也可以在路徑裡用 `{date}`,例如 `/data/out/{date}/`。

### 情境 C:測試期間想同時留一份在本機

```python
    "output_folder": "/run/media/root/D/data/PostBS/Project_output/99_new_model/",
    "ftp_remote_path": "/data/PostBS/Project_output/99_new_model/",
    "keep_local_copy": True,      # ★測試用，上線前刪掉這行★
```

執行時 log 會出現:
```
⚠️ keep_local_copy=True：解密後的明文 CSV 會同時留在 VM1（...），僅供測試，上線前請移除此設定
```

**上線檢查時請搜尋 log 有沒有這行警告。**

### 其他可用選項

| 設定 | 用途 |
|---|---|
| `source_sql` | 直接給整段 SQL,優先於 `source_table`。可用 `{start_date}` / `{end_date}` 佔位 |
| `one_more_day_flg` | 檔名的日期 +1 天(對方要求用隔日日期時) |
| `freq: "monthly"` | 月頻匯出,日期格式變 `YYYYMM` |

### 日期區間怎麼算

系統會傳 `--start-date` / `--end-date` 給 VM1 的腳本:

| freq | 區間 |
|---|---|
| `daily` | `[分區日, 分區日 + 1 天)` |
| `monthly` | `[該月 1 號, 次月 1 號)` |

---

## Step 3 · VM1 準備

- [ ] 確認 `decrypt_fields` 裡的欄位名稱**跟資料庫欄位完全一致**(含中文欄位名)
- [ ] 若走 `ftp_remote_path`:確認 VM1 對該 FTP 路徑有寫入權限

> 輸出目錄**不用自己建**,`export_remote.py` 會自己 `makedirs`。

---

## Step 4 · Reload code location

新資產會出現在 **`export/mrt_NEW_MODEL_EXPORT`**,群組 `export_to_csv`。

---

## Step 5 · 驗證

1. 手動 **Materialize** 一個分區
2. 檢查產出:
   - 落地模式 → 到 VM1 看檔案,確認欄位、筆數、編碼(用 Excel 開不會亂碼)
   - FTP 模式 → 確認對方目錄收到檔案
3. **確認解密欄位真的是明文**(而不是密文或空白)
4. 確認自動觸發:上游模型跑完後 30 秒內,這個匯出應該自己排隊

### 自動觸發原理

匯出資產的自動條件是 `AutomationCondition.eager()`,由 `default_automation_condition_sensor`
每 30 秒評估。只要 `depends_on_dbt_model` 指定的模型完成該分區,匯出就會自動跑。

**如果沒有自動觸發**,九成是 `depends_on_dbt_model` 填成 alias(`mrt_XXX`)而不是檔名。

---

## 完成檢查表

```
[ ] source_table 用實體表名（alias）
[ ] depends_on_dbt_model 用模型檔名
[ ] 正式上線用 ftp_remote_path，沒有 keep_local_copy
[ ] decrypt_fields 欄位名稱與資料庫一致
[ ] 若走 FTP：VM1 對該路徑有寫入權限
[ ] Reload 成功，資產出現在 export_to_csv 群組
[ ] 手動跑過一次，檔案內容與編碼正確
[ ] 解密欄位確認是明文
[ ] 上游模型跑完後會自動觸發
[ ] log 裡沒有 keep_local_copy 的警告
```
