# 11 · Partition 與日期區間

**檔案**:`dagster_code/assets.py`

---

## 一、現在的邏輯是什麼

### 1-1 兩種分區定義

```python
# assets.py 第 43–44 行
daily_partitions = DailyPartitionsDefinition(start_date="2026-01-01", end_offset=1)
monthly_partitions = MonthlyPartitionsDefinition(start_date="2026-01-01", end_offset=1)
```

| 參數 | 意思 |
|---|---|
| `start_date="2026-01-01"` | 分區從這一天開始,**在此之前的日期不存在,無法補跑** |
| `end_offset=1` | 往未來多開一格。沒有這個設定的話,「今天」要等到明天才會出現 |

### 1-2 哪個資產用哪種分區

| 資產 | 依據 |
|---|---|
| 檔案線 / DB 同步線 | `freq == "daily"` → 日檔;**其他一律月檔** |
| dbt 模型 | tag 有 `monthly_job` 或是 snapshot → 月檔;否則日檔 |
| 匯出 | `freq`(預設 `"daily"`) |

檔案線的判斷式長這樣(注意它是「不是 daily 就是 monthly」):

```python
target_partitions_def = daily_partitions if freq == "daily" else monthly_partitions
```

**所以 `freq` 沒填 = 月檔**,而且檔名的 `{date}` 會變成 6 碼:

```python
formatted_date = partition_date.replace("-", "") if freq == "daily" else partition_date.replace("-", "")[:6]
```

| freq | 分區 key | `{date}` 代入結果 |
|---|---|---|
| `"daily"` | `2026-08-01` | `20260801` |
| `"monthly"` / 沒填 | `2026-08-01`(固定 1 號) | `202608` |

### 1-3 上下游差一天的特例(earlyjob)

有些來源會提早在前一天晚上先送一批資料(earlyjob),隔天早上才送正常的那批。
正常模型要跟前一天晚上的 earlyjob 結果比對,所以**下游要吃前一天的上游 partition**。

這些配對登記在 `assets.py` 的 `PREV_DAY_DEPS`:

```python
PREV_DAY_DEPS = {
    ("ATM_C2", "ATM_C2_earlyjob"),
    ("ATM_E2", "ATM_E2_earlyjob"),
}
```

`CustomDbtTranslator.get_partition_mapping` 只做一件事——查表:

```python
    def get_partition_mapping(self, dbt_resource_props, dbt_parent_resource_props):
        downstream_name = dbt_resource_props.get("name", "")
        upstream_name = dbt_parent_resource_props.get("name", "")

        if (downstream_name, upstream_name) in PREV_DAY_DEPS:
            return TimeWindowPartitionMapping(
                start_offset=-1,
                end_offset=-1,
                allow_nonexistent_upstream_partitions=True,
            )

        return None
```

| 參數 | 意思 |
|---|---|
| `start_offset=-1` / `end_offset=-1` | 上游往前挪一格(前一天) |
| `allow_nonexistent_upstream_partitions=True` | 上游那天沒資料也不擋住下游(**earlyjob 不一定每天都有**) |

其他所有相依都是預設的「同一天對同一天」。

### 1-4 匯出的日期區間

匯出會算一個查詢區間傳給 VM1:

```python
        if freq == "daily":
            start_date = current
            end_date = current + relativedelta(days=1)
        else:
            start_date = current.replace(day=1)
            end_date = start_date + relativedelta(months=1)
```

| freq | 區間 |
|---|---|
| daily | `[分區日, 分區日+1 天)` |
| monthly | `[該月 1 號, 次月 1 號)` |

另外有個 `one_more_day_flg`,設了之後**只有檔名的日期會 +1 天**,查詢區間不變。
用在「對方要求檔名寫隔天日期」的情境。

---

## 二、想調什麼,改哪裡

### 調整 A:讓分區可以往前補到更早的日期

```python
daily_partitions = DailyPartitionsDefinition(start_date="2026-01-01", end_offset=1)
                                                          ↑ 改這裡
```

⚠️ **這個改動影響全系統所有資產**。往前開越多,UI 上的分區格子越多,
資產頁面會變慢,backfill 也更容易誤選一大片。

**只想補幾天歷史資料的話,不要改這個** ——直接對現有分區做 backfill 就好。

---

### 調整 B:不要往未來多開一格

```python
end_offset=1    →    end_offset=0
```

改成 0 之後,「今天」的分區要等到明天才會出現。
**除非你確定當天不會有當天的資料,否則不要改。**

---

### 調整 C:新增一組「上游取前一天」的配對

在 `PREV_DAY_DEPS` 加一行就好,**不需要動 `get_partition_mapping` 的邏輯**:

```python
PREV_DAY_DEPS = {
    ("ATM_C2", "ATM_C2_earlyjob"),
    ("ATM_E2", "ATM_E2_earlyjob"),
    ("新下游模型", "新上游模型"),        # ← 加這行
}
```

名稱用 **model 檔名**(不是 alias)。順序是 `(下游, 上游)`,寫反了不會報錯,
但也不會生效——Reload 之後記得到 Lineage 分頁確認。

> 這是日常會遇到的操作,完整步驟(含要不要一起加匯出)見
> [04 · earlyjob 模型](../日常維運/04_新增一支dbt模型.md#earlyjob-模型下游要吃前一天的上游)。

---

### 調整 D:讓某張表從日檔改成月檔(或反過來)

**不能只改 `freq`。** 這個改動會讓:

1. 分區定義換掉 → **舊分區的執行紀錄全部對不上**
2. `{date}` 的格式從 8 碼變 6 碼 → **檔名規則變了**
3. 群組從 `extract_load` 變 `monthly_extract_load` → sensor 換一支管

**建議做法**:當成「一張新表」處理——改個新表名重新設定,舊的設 `manual_only: True` 保留一段時間再移除。

---

### 調整 E:改匯出的日期區間

例如要「往前抓 7 天」:

```python
        if freq == "daily":
            start_date = current - relativedelta(days=6)     # ← 改這裡
            end_date = current + relativedelta(days=1)
```

⚠️ 這會影響**所有**匯出。只想改一個的話,建議走 `source_sql` 自己寫 WHERE:

```python
    "source_sql": "SELECT * FROM dbo.mrt_XXX WHERE TABLE_DATE >= '{start_date}' AND TABLE_DATE < '{end_date}'",
```

或在設定裡加一個新的鍵(例如 `lookback_days`),再到 `build_export_assets` 讀取它。

---

## 三、改完怎麼驗證

1. Reload code location
2. **Assets** 頁面 → 點進受影響的資產 → 看分區狀態列
   - 格子的起訖日期對不對
   - 最右邊有沒有「今天」那一格
3. 手動 materialize 一個分區,看 log 裡的日期
   - 檔案線:log 會印出組出來的檔名,確認 `{date}` 格式正確
   - dbt:log 會印 `[Dagster -> dbt] 從清單成功解鎖日期: 2026-08-01`
   - 匯出:log 會印 `--start-date` / `--end-date`
4. 有改 partition mapping 的話,到下游資產的相依圖確認上游指向前一天那格

---

## 四、注意事項

| 事項 | 說明 |
|---|---|
| **改分區定義會讓歷史紀錄對不上** | Dagster 是用分區 key 存執行紀錄的,定義變了等於換一組 key |
| **`start_date` 往前開會拖慢 UI** | 分區數量直接影響資產頁面的渲染 |
| **資料庫的 partition 跟 Dagster 的 partition 是兩回事** | 前者是 SQL Server 的實體分割(`partition_function`),後者是排程的時間切片。兩者無關,不要混淆 |
| **月檔的分區 key 固定是 1 號** | `2026-08-01` 代表整個 8 月,不是 8 月 1 日 |
