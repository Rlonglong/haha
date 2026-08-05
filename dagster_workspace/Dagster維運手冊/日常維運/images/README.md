# 截圖清單

這個資料夾放手冊用到的截圖。檔名跟文件裡的連結是綁定的,**要換圖請沿用同一個檔名**。

---

## 目前有的圖

| 檔名 | 畫面 | 用在哪 |
|---|---|---|
| `01_runs_overview.png` | Runs 清單(All 分頁) | 01 · 步驟 1 |
| `02_run_detail.png` | 失敗 Run 的詳細頁,右側可看到 Re-executions 面板 | 01 · 步驟 2 |
| `03_run_logs.png` | 同上,已切到 stderr 且已點選步驟 | 01 · 步驟 3 |
| `04_reexecute.png` | Re-execute 下拉選單展開 | 01 · 步驟 4 |
| `05_sensors_list.png` | Automation 的 sensor 清單 | 01 · 步驟 5 |
| `06_code_locations.png` | Deployment → Code locations | 02 · 第 1 節 |
| `07_asset_detail.png` | Catalog 資產清單,含 Materialize selected 按鈕 | 02 · 第 2 節 |
| `08_partition_picker.png` | Launch runs 視窗,含分區選擇與 Options | 02 · 第 2 節 |
| `09_backfill.png` | Materialize 下拉選單(Materialize unsynced) | 02 · 第 3 節 |
| `10_sensor_detail.png` | Sensor 詳細頁,含 Cursor 與 Edit | 02 · 第 4 節 |
| `14_global_lineage.png` | 左邊選單 Lineage → Global asset lineage | 01、02 |
| `15_lineage_upstream.png` | 資產的 Lineage 分頁,Upstream 方向 | 01、02 |
| `16_lineage_downstream.png` | 資產的 Lineage 分頁,Downstream 方向 | 01、02 |

---

## 換圖或補圖時的注意事項

**框選**:紅色、粗細 3px。目前已框的重點:

| 圖 | 框住的東西 |
|---|---|
| 01 | 左側 Runs、Status 整欄 |
| 04 | Re-execute 下拉箭頭、`From selected` 與 `From failure` |
| 05 | 左側 Automation |
| 06 | Reload 按鈕 |
| 07 | 左側 Catalog、Materialize selected 按鈕 |
| 09 | `Materialize unsynced` 選項 |
| 10 | Cursor 的 Edit 按鈕 |
| 14 | 左側 Lineage |

**內容檢查**:截圖前確認畫面上沒有真實帳號、身分證號、金額等敏感資料。
目前的圖裡有內部主機名稱與路徑(`dagster.dai.post.gov.tw`、`/run/media/root/D/data/...`),
**這份文件只在內部流通,不要外流**。

**重截時盡量保留原本的情境**,例如:

- `02` / `03` 要選一筆**真的失敗**的 Run,而且看得到 `Exceeded max_retries of 3`
- `15` 要選一個上游有紅框(失敗)的資產,才看得出血緣圖的價值
- `16` 要選一個下游會連到 `export_to_csv` 的資產

---

## 缺號說明

`11`、`12`、`13` 是早期規劃後來取消的編號,直接跳過,不要補。
