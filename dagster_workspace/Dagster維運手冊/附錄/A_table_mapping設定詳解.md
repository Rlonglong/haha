# 附錄 A · `table_mapping.py` 設定詳解

**檔案位置**:`dagster_workspace/dagster_code/table_mapping.py`

這份附錄逐一說明 `TABLE_CSV_MAPPING`(檔案線)每一個可以填的設定值:**它是什麼、不填會怎樣、有什麼限制、以及當初為什麼要這樣設計**。

實際操作步驟請看 [03 · 新增一張資料表](../日常維運/03_新增一張資料表.md);這裡是查詢用的。

> 匯出設定(`SQL_TO_CSV_MAPPING`)請看 [05 · 新增一個 CSV 匯出](../日常維運/05_新增一個CSV匯出.md),
> DB 直連設定(`DB_SYNC_MAPPING`)請看 [06 · 新增一張 DB 直連同步表](../日常維運/06_新增一張DB直連同步表.md)。

---

## 這個設定檔的設計理念

在動手改之前,先理解三件事,後面所有規則就都說得通了:

### ① 一份設定 = 一整條處理鏈

你不是在寫程式,而是在**描述這張表長什麼樣**。Dagster 會依照你填的內容,**動態產生 1～6 個處理節點**。

```
use_ftp_fetch  → 產生 XXX_fetch_ftp 節點
use_data_rule  → 產生 XXX_named 節點
encrypt_fields → 產生 XXX_encrypted 節點
has_clean_func → 產生 XXX_clean 節點
（一定會有）    → 產生 database/XXX 節點
use_ftp_fetch  → 產生 XXX_archive_cleanup 節點
```

沒開的節點不會產生,而且**鏈子不會斷**——每個節點的上游是「往前找第一個有開的節點」。所以你可以自由組合,不用擔心中間空一格。

### ② 設定錯誤要在「載入時」就爆掉,不是半夜才爆

很多欄位有嚴格的檢查,而且是在 **Reload code location 的當下**就驗證。這是刻意的:

> 寧可你按 Reload 時看到紅字,也不要等到凌晨三點檔案進來才失敗。

所以看到「設定值不合法」的錯誤請不要繞過它,那是保護機制。

### ③ 白名單不是刁難,是擋 SQL Injection

表名、索引名、分區函式名這些會被**直接字串拼進 SQL**,因為 T-SQL 的識別字**沒辦法用參數化查詢帶入**:

```python
# 這樣是不行的，SQL Server 不接受
cursor.execute("ALTER INDEX %s ON dbo.%s DISABLE", (index_name, table_name))

# 只能這樣，所以拼進去之前一定要驗證
run_sql(f"ALTER INDEX {index_name} ON dbo.{table_name} DISABLE")
```

同理,路徑片段會被組進 SSH 指令,不驗證就可能被路徑穿越或指令注入。

**所以請不要為了讓某個名字通過而放寬規則**——要改的是名字,不是規則。

---

## 0 · FTP 下載設定

這一區決定「檔案怎麼來」。有兩種模式:**我們主動去 FTP 抓**,或**對方推到我們的目錄**。

### `use_ftp_fetch`

| 項目 | 內容 |
|---|---|
| 型別 | bool |
| 預設 | `False` |
| 必填 | 否 |

**是什麼**:要不要建立 `XXX_fetch_ftp` 節點主動去 FTP 下載。

**不填會怎樣**:當成 `False`,系統會直接去 `input_folder` 找檔案,假設對方已經把檔案推進來了。

**設計考量**:這兩種模式的差異不只是「誰去拿檔案」,連 sensor 監控的對象都不一樣——
`True` 時 sensor 會叫 VM1 去列 **FTP 目錄**,`False` 時是列 **VM1 的本地目錄**。所以這個旗標一改,監控來源整個換掉。

**連帶影響**:設 `True` 就**必須**給 `archive_dir`,否則 Reload 直接失敗。因為主動下載的檔案需要有地方封存,不能下載完就散落在暫存區。

---

### `ftp_remote_template`

| 項目 | 內容 |
|---|---|
| 型別 | str |
| 預設 | 無 |
| 必填 | 與 `ftp_remote_dir` + `ftp_remote_filename_prefix` **二擇一** |

**是什麼**:完整的遠端檔案路徑模板,`{date}` 會被替換成日期。

```python
"ftp_remote_template": "/data/fromFPP/T_XXX/T_XXX_{date}.csv"
```

**什麼時候用**:遠端檔名**完全可預測**時。你知道日期就能算出完整檔名。

**限制**:`ftp_remote_template` 跟 `ftp_remote_dir`/`prefix` **不能同時給**,會在載入時報錯。

---

### `ftp_remote_dir` + `ftp_remote_filename_prefix`

| 項目 | 內容 |
|---|---|
| 型別 | str + str |
| 預設 | 無 |
| 必填 | 二擇一,而且**必須成對出現** |

**是什麼**:遠端目錄 + 檔名前綴。系統會叫 VM1 去那個目錄裡,找開頭符合前綴的檔案。

```python
"ftp_remote_dir": "/data/fromFPP/T_TXN_PS/",
"ftp_remote_filename_prefix": "T_TXN_PS_{date}",
```

**為什麼需要這個模式**:有些來源的檔名日期後面還帶時間戳,例如:

```
T_TXN_PS_20260714012819.zip
         └─日期─┘└時分秒┘   ← 這 6 碼我們事先不知道
```

這種檔名**沒辦法用 template 直接組出來**,只能給目錄和前綴,讓 VM1 自己去比對。

**限制**:
- 兩個必須同時給,只給一個會在載入時報錯
- `ftp_remote_filename_prefix` 去掉 `{date}` 之後,必須符合 `^[A-Za-z0-9_-]{1,128}$`
  (因為它會被組進遠端的 `lftp` 指令字串)

---

### `use_ftp_zip`

| 項目 | 內容 |
|---|---|
| 型別 | bool |
| 預設 | `False` |
| 必填 | 否 |

**是什麼**:來源是加密的 zip,下載後要先解壓縮。

**連帶影響(很重要)**:這個旗標會讓 **sensor 改成監控 `.zip` 而不是 `.csv`**。

```python
watch_ext = ".zip" if config.get("use_ftp_zip") else ".csv"
```

**設定錯了的典型症狀**:sensor tick log 顯示 `找到: []`,但你去目錄看明明有檔案——因為副檔名對不上。

**密碼怎麼給**:密碼**不寫在這裡**。系統會依表名自動組出環境變數名稱:

```
ZIP_PWD_<表名大寫>     例如 ZIP_PWD_T_TXN_PS
```

你要做的是在 **VM1 的 `.env`** 裡設定這個變數。

**為什麼要這樣設計**:密碼從頭到尾**不經過 Dagster、不經過 SSH 指令字串**。Dagster 只傳「變數的名字」過去,VM1 上的腳本自己去環境變數讀值。這樣即使有人看到 Dagster 的 log 或 SSH 指令,也拿不到密碼。

---

### `zip_inner_filename`

| 項目 | 內容 |
|---|---|
| 型別 | str |
| 預設 | `None`(取 zip 裡的第一個檔案) |
| 必填 | 否 |

**是什麼**:zip 裡面要抽出來的檔名。

**什麼時候要填**:zip 內的檔名跟外層 zip 檔名沒有關係時。例如外層是 `T_TXN_PS_20260801.zip`,裡面卻是 `EXNP_PSPDC55_PS`——這種就要明確指定。

**不填會怎樣**:VM1 的腳本會取 zip 裡的第一個檔案。單一檔案的 zip 這樣就夠了。

---

### `expected_part_count`

| 項目 | 內容 |
|---|---|
| 型別 | int |
| 預設 | `1` |
| 必填 | 否 |

**是什麼**:遠端把資料拆成幾個檔。大於 1 時,系統會下載全部再**合併成單一檔案**。

**檔名規則是寫死的**,只留 `{part}` 給遠端:

```
<表名>_{part}_<日期>.zip     （use_ftp_zip=True）
<表名>_{part}_<日期>.csv     （一般）
```

**設計考量**:合併在 VM1 上完成,Dagster 只知道「最後會有一個檔」。這樣下游的加密、清洗、BCP 完全不用知道來源被拆過幾份。

**目前的使用者**:`T_CUST` 設 `15`。

---

### `staging_subfolder`

| 項目 | 內容 |
|---|---|
| 型別 | str |
| 預設 | 表名 |
| 必填 | 否 |
| 限制 | `^[A-Za-z0-9_-]{1,128}$`——**不可含 `/` 或 `..`** |

**是什麼**:VM1 家目錄底下的暫存子資料夾,路徑會是 `~/<staging_subfolder>/`。

**為什麼要有這個東西**:這是**明文資料唯一落地的地方**。

有加密欄位的表,流程是:

```
FTP → ~/<staging>/xxx.csv           ← 明文，只在這裡
    → ~/<staging>/xxx_NAMED.csv     ← 明文
    → <input_folder>/xxx_ENCRYPTED.csv   ← 加密後才寫回正式目錄
```

明文集中在一個地方,封存清理節點才能確保**全部刪乾淨**。

**為什麼限制不能有 `/`**:這個值會被組成 `~/{staging_subfolder}/...` 送進 SSH 指令。如果允許 `/` 或 `..`,就可能被寫成 `../../etc` 這種路徑穿越。

---

### `archive_dir`

| 項目 | 內容 |
|---|---|
| 型別 | str |
| 預設 | 無 |
| 必填 | **`use_ftp_fetch=True` 時必填** |

**是什麼**:BCP 成功之後,把加密檔搬過去封存的目錄。

**封存節點做的事**:
1. 刪掉 staging 區的明文檔
2. 把加密後的檔案(有 zip 密碼的話會用**同一組密碼**重新壓縮)搬進 `archive_dir`
3. 其餘中繼檔全部刪掉

**結果**:`archive_dir` 裡最終**只留一個 `.zip`**。

**為什麼強制必填**:主動下載的檔案如果不封存,中繼檔會越積越多,而且明文可能留在磁碟上。強制填寫是為了逼你想清楚檔案的去向。

---

## 1 · 路徑與基礎設定

### `input_folder`

| 項目 | 內容 |
|---|---|
| 型別 | str |
| 預設 | FTP 模式下自動帶入表名;非 FTP 模式**無預設** |
| 必填 | **非 FTP 模式必填**(不填直接報錯) |

**是什麼**:原始檔案的所在目錄。**加密後的檔案也固定寫回這裡**。

**相對路徑的處理**:如果填的是相對路徑,會接在 `/run/media/root/D/data/` 後面:

```python
def resolve_path(base_dir, target_path):
    if os.path.isabs(target_path):
        return target_path              # 絕對路徑照用
    return os.path.join(base_dir, target_path)   # 相對路徑接在 base 後面
```

**目前的慣例是寫絕對路徑**,比較不會誤會。

**為什麼加密輸出固定寫回 `input_folder`**:這樣不管明文是從 staging 來還是從 `input_folder` 來,**下游邏輯完全不用改**。這是刻意的解耦。

---

### `output_folder`

| 項目 | 內容 |
|---|---|
| 型別 | str |
| 預設 | 同 `input_folder` |
| 必填 | 否 |

**是什麼**:清洗後的檔案輸出目錄。

**不填會怎樣**:清洗結果就寫回 `input_folder`,跟原始檔放在一起。沒有清洗節點的表,這個值只影響錯誤 log 的預設位置。

---

### `error_log_dir`

| 項目 | 內容 |
|---|---|
| 型別 | str |
| 預設 | `<output_folder>/bcp_error_logs` |
| 必填 | 否 |

**是什麼**:BCP 匯入失敗時,把被拒絕的資料列寫到哪裡。

**檔名格式**:`<表名>_error_<日期>.log`

**為什麼預設放在 output 底下**:排查 BCP 問題時,通常要對照「清洗後的檔案」和「被拒絕的列」,放在一起最方便。

---

### `template` ★必填★

| 項目 | 內容 |
|---|---|
| 型別 | str |
| 預設 | 無 |
| 必填 | **是** |

**是什麼**:本地端的標準檔名模板,**必須含 `{date}`**。

```python
"template": "T_TXN_PS_{date}.csv"
```

**`{date}` 會被代成什麼**:

| `freq` | 代入格式 | 範例(分區 `2026-08-01`) |
|---|---|---|
| `"daily"` | `YYYYMMDD` | `20260801` |
| `"monthly"` | `YYYYMM` | `202608` |

**為什麼要統一模板**:後面所有階段的檔名都是從這個名字**推導**出來的:

```
T_TXN_PS_20260801.csv
  → T_TXN_PS_20260801_NAMED.csv
  → T_TXN_PS_20260801_ENCRYPTED.csv
  → T_TXN_PS_20260801_CLEAN.csv
```

只要 `template` 對了,整條鏈的檔名都會對。

---

### `freq` ★必填★

| 項目 | 內容 |
|---|---|
| 型別 | `"daily"` \| `"monthly"` |
| 預設 | 無 |
| 必填 | **是** |

**是什麼**:檔案是每天一份還是每月一份。

**它同時決定四件事**:

1. **分區定義**——日檔一天一格,月檔一月一格
2. **`{date}` 的格式**——8 碼還是 6 碼
3. **群組**——`extract_load` 還是 `monthly_extract_load`
4. **由哪支 sensor 監控**——daily watcher 還是 monthly watcher

**⚠️ 不填會出現不一致狀態**,而且不會報錯:

```python
target_partitions_def = daily_partitions if freq == "daily" else monthly_partitions
target_group = "monthly_extract_load" if freq == "monthly" else "extract_load"
```

不填的話:
- 分區變成**月檔**(因為判斷式是「不是 daily 就是 monthly」)
- 群組卻是 **`extract_load`**(因為判斷式是「是 monthly 才給 monthly 群組」)
- 而且兩支 sensor 都用 `config.get("freq") != freq` 過濾,**兩邊都撈不到它**

結果就是「檔名日期變 6 碼、只能選每月 1 號、而且永遠不會自動觸發」。

**不要用「不填 freq」來達成手動專用**,那是副作用不是功能。要手動專用請用 `manual_only`。

---

### `delimiter`

| 項目 | 內容 |
|---|---|
| 型別 | str |
| 預設 | `","` |
| 必填 | 否(但**強烈建議明確指定**) |

**是什麼**:CSV 的欄位分隔符號。

#### ★ 為什麼不要用逗號,也不要用 `|`

這一項是**踩過坑之後才調整的**,不是隨便挑的。演進過程:

**第一版:用逗號 `,`**

失敗。**備註類欄位裡本來就會有逗號**——民眾填的地址、交易備註、客服註記,
只要有人打了一個「台北市中正區忠孝東路一段1號,3樓」,那一列的欄位數就爆掉了。
輕則欄位錯位,重則整批 BCP 失敗。

**第二版:改用管線符號 `|`**

大部分表沒問題,但還是**真的出現過** —— **有人的 email 欄位裡含有 `|`**。
使用者輸入的自由欄位什麼字元都可能出現,`|` 在鍵盤上就有,不是罕見到不會被打出來。

**第三版(目前):改用罕見符號,例如 `〨`**

```python
"delimiter": "〨",
```

`〨` 是蘇州碼子的「八」(U+3028)。挑它的理由很單純:

- **一般使用者不會、也很難打出來**——不在標準中文輸入法的常用字表裡
- 不是程式語言、Shell、SQL 的特殊字元,不會有跳脫問題
- 視覺上跟中文內容明顯不同,人工檢視檔案時一眼就能分辨欄位邊界

#### 選分隔符號的原則

> **分隔符號要選一個「使用者輸入不可能產生」的字元。**
> 不是選好看的,也不是選業界慣例——**業界慣例(逗號、`|`)都被使用者輸入打敗過**。

如果 `〨` 之後也出了問題,可以往同一系列或其他罕見符號找
(例如 `〩`、`␟`)。判斷標準只有一個:**這個字元有沒有可能出現在資料內容裡?**

#### 換分隔符號時要一起確認的事

分隔符號會**同時**影響好幾個環節,只改設定不夠:

| 環節 | 要確認什麼 |
|---|---|
| 來源檔本身 | 對方產檔時用的是不是同一個符號 |
| 檔案編碼 | `〨` 在 UTF-8 是 **3 bytes**。**檔案必須是 UTF-8**,如果來源是 Big5 會對不上 |
| 清洗函式 | VM1 的 `clean_functions/` 裡 `pd.read_csv(sep=...)` 要一起改 |
| BCP | `bcp_remote.py` 傳給 BCP 的 `-t` 參數要能接受多位元組字元 |
| 檔規 | 有開 `use_data_rule` 的話,`assign_columns_remote.py` 輸出的分隔符也要一致 |

**換完之後務必先手動跑一個 partition**,確認欄位數對得上、BCP 沒有錯誤列。

> `delimiter` 這個值會傳給 VM1 的腳本(`--delimiter`),經過 `shlex.quote()` 跳脫,
> 所以填特殊字元不會有指令注入的問題,可以放心用。

---

### `use_data_rule`

| 項目 | 內容 |
|---|---|
| 型別 | bool |
| 預設 | `False` |
| 必填 | 否 |

**是什麼**:要不要建立 `XXX_named` 節點,依「檔規」把固定寬度的資料切成欄位並補上欄位名稱。

**什麼時候要開**:來源檔**沒有表頭**,或是固定寬度格式。

**前置作業**:檔規裡必須有對應的 sheet,詳見下一項。

---

### `data_rule_sheet`

| 項目 | 內容 |
|---|---|
| 型別 | str |
| 預設 | 表名 |
| 必填 | 否 |

**是什麼**:檔規檔案裡對應的 sheet 名稱。

**什麼時候要填**:sheet 名稱跟表名不一樣時。例如 `T_TXN_PS_FIRST` 這張表用的是 `T_TXN_PS` 的檔規:

```python
"data_rule_sheet": "T_TXN_PS",
```

**設計考量**:讓「表名」跟「檔規名」解耦。同一份檔規可以被多張表共用,不用複製貼上。

---

### `manual_only`

| 項目 | 內容 |
|---|---|
| 型別 | bool |
| 預設 | `False` |
| 必填 | 否 |

**是什麼**:這張表**完全不自動觸發**,只能在 UI 上手動執行。

**設 `True` 之後**:

| 觸發途徑 | 會不會跑 |
|---|---|
| 日檔 / 月檔 sensor | ✗ 迴圈一開始就跳過 |
| `__DAILY_ASSET_JOB` / `__MONTHLY_ASSET_JOB` | ✗ 已從選集扣掉 |
| 自動觸發 sensor | ✗ 檔案線的節點本來就沒有自動條件 |
| UI 上手動 Materialize | **✓ 唯一入口** |

**什麼時候用**:一次性補檔、對方不定期才給、還在測試階段的表。

**⚠️ `freq` 還是要照實填**——這個旗標只關掉觸發,不影響分區與檔名格式。

---

## 2 · 處理邏輯

### `has_clean_func`

| 項目 | 內容 |
|---|---|
| 型別 | bool |
| 預設 | `False` |
| 必填 | 否 |

**是什麼**:要不要建立 `XXX_clean` 節點,執行這張表專屬的清洗邏輯。

**⚠️ 開了之後 VM1 必須配合**,否則執行時會直接失敗:

```
ValueError: 找不到 T_XXX 的清洗邏輯
```

需要做兩件事:
1. 建立 `/home/bcp_runner/scripts/clean_functions/<表名>_CLEAN.py`
2. 在 `/home/bcp_runner/scripts/clean_remote.py` 的 `CLEAN_FUNC_MAP` 註冊

詳細步驟見 [03 · Step 3-1](../日常維運/03_新增一張資料表.md#3-1-清洗函式只有設了-has_clean_func-true-才需要)。

**為什麼清洗邏輯不寫在 Dagster 這邊**:如果在 VM4 定義函式再序列化(pickle)透過 SSH 傳到 VM1 執行,等同於**遠端執行任意程式碼**,是明確的注入風險。所以清洗邏輯一律直接放在 VM1,Dagster 只傳「表名」這個字串過去,由 VM1 自己查表決定要跑哪支函式。

---

## 3 · 容錯機制

### `retries` / `retry_delay_sec`

| 項目 | `retries` | `retry_delay_sec` |
|---|---|---|
| 型別 | int | int |
| 預設 | `0` | `60` |
| 必填 | 否 | 否 |

**是什麼**:這張表的每個節點失敗後,自己重試幾次、間隔幾秒。

**⚠️ 預設是 0,代表完全不重試**:

```python
my_retry_policy = RetryPolicy(max_retries=max_retries, delay=delay_sec) if max_retries > 0 else None
```

沒填 `retries` 的表,失敗一次就直接紅了。**目前多數表都填 `3`。**

**怎麼選值**:

| 情況 | 建議 |
|---|---|
| 對方 FTP 常常抽風 | `retries: 5`,`retry_delay_sec: 300` |
| 失敗一定是資料問題,重試沒意義 | `retries: 0`,讓人早點看到 |
| 檔案很大跑很久 | 間隔調大,避免重試時前一次還沒清乾淨 |

**注意這只是第一層**。另外還有 Run 層的重試(sensor 送出時帶 `dagster/max_retries: 3`,策略是只重跑失敗的資產)。所以維運人員在 UI 上看到的紅色,是**兩層重試都用完**之後的結果——log 裡會寫 `Exceeded max_retries of 3`。

---

## 4 · BCP 匯入設定

### `bcp_target`

| 項目 | 內容 |
|---|---|
| 型別 | str |
| 預設 | 表名 |
| 必填 | 否 |
| 限制 | `^[A-Za-z_][A-Za-z0-9_]{0,127}$` |

**是什麼**:BCP 最終要灌進去的**目標表或 View 名稱**。

**為什麼要能指定 View**:BCP 對欄位順序很敏感。常見的情況是:

- 正式表有自動編號欄位、預設值欄位,但來源 CSV 沒有
- 來源 CSV 的欄位順序跟正式表不一樣

這時候建一個 View 把欄位順序對齊,讓 BCP 灌 View 就好,不用改正式表結構,也不用改來源檔。

```python
"bcp_target": "v_BCP_T_TXN_PS",
```

**⚠️ 排查陷阱**:BCP 顯示成功但正式表沒資料時,**先確認你查的是不是 View 背後真正的表**。

---

## 5 · 索引管理

### `use_index` / `index_name`

| 項目 | `use_index` | `index_name` |
|---|---|---|
| 型別 | bool | str |
| 預設 | `False` | `IX_<表名>` |
| 必填 | 否 | 否 |
| 限制 | — | `^[A-Za-z_][A-Za-z0-9_]{0,127}$` |

**是什麼**:BCP 之前先 `DISABLE` 索引,BCP 之後再 `REBUILD`(`FILLFACTOR = 100`)。

**為什麼要這樣做**:大量寫入時,每插入一列都要維護索引,非常慢。先關掉索引灌完再重建,整體通常快很多。

**什麼時候該開**:資料量大(百萬列以上)且有非叢集索引的表。資料量小的話,重建索引的成本可能比省下來的還多。

**設計上的保護**:DDL 都包了存在性檢查,索引不存在就跳過不會報錯:

```sql
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.XXX') AND name = 'IX_XXX')
BEGIN
    ALTER INDEX IX_XXX ON dbo.XXX DISABLE;
END
ELSE
    PRINT 'Index IX_XXX 不存在，略過';
```

**⚠️ 但這也代表索引名稱打錯不會有人告訴你**——它會安靜地跳過,你以為有加速其實沒有。填了 `use_index` 之後請去 log 確認有看到 `Index XXX DISABLE 完成`。

---

## 6 · 分區與生命週期管理

> ⚠️ **這裡的「分區」是 SQL Server 的實體資料分割(partition function / scheme),
> 跟 Dagster 的「分區(partition = 日期)」是完全不同的東西。** 不要混淆。

### `use_partition` / `partition_function`

| 項目 | `use_partition` | `partition_function` |
|---|---|---|
| 型別 | bool | str |
| 預設 | `False` | `""` |
| 必填 | 否 | **`use_partition=True` 時必填** |
| 限制 | — | `^[A-Za-z_][A-Za-z0-9_]{0,127}$` |

**是什麼**:啟用之後,每次 BCP 之前系統會**自動確保下個月的分區存在**。

**做的事**:

```
檢查 partition function 裡有沒有「下個月 1 號」這個界線
  ↓ 沒有
ALTER PARTITION SCHEME ... NEXT USED <最後一個 filegroup>
ALTER PARTITION FUNCTION ... SPLIT RANGE ('<下個月>')
  ↓ 有
略過
```

**為什麼要自動化**:分區表如果沒有預先切好下個月的範圍,月初的資料會全部塞進最後一個分區,越積越大,失去分區的意義。這種事很容易忘記,所以做成每次入庫都檢查一次。

**前置作業**:partition function / scheme / filegroup **必須先在資料庫建好**,Dagster 只負責維護,不負責初始建立。

---

### `retention_years` / `archive_table`

| 項目 | `retention_years` | `archive_table` |
|---|---|---|
| 型別 | int | str |
| 預設 | `None` | `""` |
| 必填 | 否 | 否 |
| 限制 | — | `^[A-Za-z_][A-Za-z0-9_]{0,127}$` |

**是什麼**:資料保留年限,以及過期資料要切換到哪張封存表。

**⚠️ 兩個都給才會執行**,只給一個等於沒設定:

```python
if retention_years and archive_table:
    # 才會執行淘汰
```

**做的事**:

```
算出「N 年前的那個月」
  ↓ 該分區存在
TRUNCATE TABLE dbo.<archive_table>      ← 先清空封存表
ALTER TABLE dbo.<表名> SWITCH PARTITION @pn TO dbo.<archive_table>
TRUNCATE TABLE dbo.<archive_table>      ← 再清空一次
ALTER PARTITION FUNCTION ... MERGE RANGE ('<過期月份>')
```

**為什麼要 `SWITCH` 而不是 `DELETE`**:`SWITCH PARTITION` 是**中繼資料層級的操作**,幾乎瞬間完成,不產生交易 log。用 `DELETE` 刪幾千萬列會鎖表很久、log 爆掉。

**為什麼 TRUNCATE 兩次**:第一次確保封存表是空的(否則 SWITCH 會失敗),第二次是真的把資料丟掉。

**⚠️ 資料是真的被刪掉的,不是搬去別的地方保存。** `archive_table` 只是一個過渡容器。要真正保留歷史,請自己另外做備份。

**限制**:`archive_table` 的**結構必須跟本表完全一致**(欄位、型別、索引、filegroup),否則 `SWITCH` 會失敗。

---

## 7 · 資安設定

### `encrypt_fields`

| 項目 | 內容 |
|---|---|
| 型別 | list[str] |
| 預設 | `[]` |
| 必填 | 否 |

**是什麼**:要加密的欄位清單。**非空才會建立 `XXX_encrypted` 節點。**

```python
"encrypt_fields": ["ACT_NO", "ACCT_NBR_ORI", "OWN_TRANS_ACCT"],
```

**連帶影響(很重要)**:設了加密欄位之後,**下載的落點會改變**:

| 有加密欄位 | 下載到 `~/<staging_subfolder>/`(暫存區) |
|---|---|
| 沒有加密欄位 | 下載到 `<input_folder>/`(正式目錄) |

**為什麼**:明文只能存在於暫存區,而且要能被封存節點確實刪除。正式目錄裡只應該有加密後的檔案。

**欄位名稱要跟資料來源一致**:
- 有開 `use_data_rule` → 用**檔規裡的欄位名**(因為加密是在套完檔規之後才做)
- 沒開 → 用來源 CSV 表頭的欄位名

**匯出時怎麼還原**:匯出設定裡的 `decrypt_fields` 會把它解回明文。所以**加密不是雜湊**,是可逆的。

---

## 完整範例

### 最簡:對方推檔、不加密、不清洗

```python
"T_SIMPLE": {
    "input_folder": "/run/media/root/D/data/fromFPP/T_SIMPLE",
    "output_folder": "/run/media/root/D/data/T_SIMPLE/T_SIMPLE_CLEANED",
    "template": "T_SIMPLE_{date}.csv",
    "freq": "daily",
    "delimiter": "|",
    "retries": 3,
    "retry_delay_sec": 60,
},
```

會產生 **1 個節點**:`database/T_SIMPLE`。

### 完整:FTP + zip + 檔規 + 加密 + 清洗 + 索引 + 分區

```python
"T_FULL": {
    # 0. FTP
    "use_ftp_fetch": True,
    "ftp_remote_dir": "/data/fromFPP/T_FULL/",
    "ftp_remote_filename_prefix": "T_FULL_{date}",
    "use_ftp_zip": True,
    "zip_inner_filename": "EXNP_XXXX",
    "staging_subfolder": "T_FULL",
    "archive_dir": "/run/media/root/D/data/archive/T_FULL",
    # 1. 路徑與基礎
    "input_folder": "/run/media/root/D/data/fromFPP/T_FULL",
    "output_folder": "/run/media/root/D/data/T_FULL/T_FULL_CLEANED",
    "template": "T_FULL_{date}.csv",
    "freq": "daily",
    "delimiter": "|",
    "use_data_rule": True,
    # 2. 處理邏輯
    "has_clean_func": True,
    # 3. 容錯
    "retries": 3,
    "retry_delay_sec": 60,
    # 4. BCP
    "bcp_target": "v_BCP_T_FULL",
    # 5. 索引
    "use_index": True,
    "index_name": "IX_T_FULL",
    # 6. 分區
    "use_partition": True,
    "partition_function": "pf_FULL_Monthly",
    "retention_years": 2,
    "archive_table": "T_FULL_Archive",
    # 7. 加密
    "encrypt_fields": ["ACT_NO", "ID"],
},
```

會產生 **6 個節點**,完整的一條鏈。

---

## 載入時就會失敗的錯誤一覽

這些都在 Reload code location 當下就會爆出來:

| 錯誤訊息 | 原因 |
|---|---|
| `❌ 嚴重錯誤：設定檔 (config) 中遺漏了必要的 'input_folder' 參數！` | 非 FTP 模式沒給 `input_folder` |
| `❌ XXX: use_ftp_fetch=True 時必須提供 archive_dir` | 開了 FTP 沒給封存目錄 |
| `❌ XXX: ftp_remote_dir 與 ftp_remote_filename_prefix 必須同時提供` | 只給了其中一個 |
| `❌ XXX: ftp_remote_template 與 ftp_remote_dir/... 不能同時使用` | 兩種模式都給了 |
| `❌ XXX: use_ftp_fetch=True 時必須提供 ftp_remote_template，或改用 ...` | 開了 FTP 但兩種模式都沒給 |
| `❌ 設定值不合法：table_name='...' 不符合安全的 SQL 識別字格式` | 表名有特殊字元或數字開頭 |
| `❌ 設定值不合法：staging_subfolder='...' 不是安全的路徑片段` | 含 `/`、`..` 或空白 |

**看到這些不要繞過去,它們是保護機制。** 改名字,不要改規則。

---

## 快速對照:哪些設定會產生節點

| 設定 | 產生的節點 | 資產名稱 |
|---|---|---|
| `use_ftp_fetch: True` | 下載 | `file_system/<表名>_fetch_ftp` |
| `use_data_rule: True` | 套檔規 | `file_system/<表名>_named` |
| `encrypt_fields: [...]` 非空 | 加密 | `file_system/<表名>_encrypted` |
| `has_clean_func: True` | 清洗 | `file_system/<表名>_clean` |
| (無條件) | 入庫 | `database/<表名>` |
| `use_ftp_fetch: True` | 封存清理 | `file_system/<表名>_archive_cleanup` |
