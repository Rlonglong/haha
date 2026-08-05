# 截圖清單

把截圖依下表的檔名放進這個資料夾,文件裡的圖就會自動顯示。

**建議**:用 Windows 的「剪取工具」或 ShareX,框線用**紅色、粗細 3px**,統一風格。
截圖時請確認畫面上**沒有真實帳號、身分證號、金額**等敏感資料。

---

## 01_維運人員_每日操作手冊

| 檔名 | 要截什麼 | 要框什麼 |
|---|---|---|
| `01_runs_overview.png` | 左側選單點 Runs 之後的完整頁面 | ①左側選單的「Runs」②表格的「Status」整欄 ③上方狀態篩選器 |
| `02_run_detail.png` | 點進一筆**失敗**的 Run | ①上方 Run ID 與狀態標籤 ②中間步驟清單(紅色那行)③右下 log 區 |
| `03_run_logs.png` | Run 詳細頁的 log 區,拉到有錯誤的位置 | ①log 上方的層級篩選下拉 ②一段紅色錯誤訊息 |
| `04_reexecute.png` | Run 詳細頁右上角 | ①「Re-execute」按鈕 ②點開後的「Re-execute from failure」 |
| `05_sensors_list.png` | Automation / Sensors 頁面清單 | ①每列左邊的開關 ②「Last tick」欄位 |

## 02_開發人員_UI操作手冊

| 檔名 | 要截什麼 | 要框什麼 |
|---|---|---|
| `06_code_locations.png` | Deployment → Code locations | ①分頁標籤 ②該列的狀態(綠色 Loaded)③右側 Reload 按鈕 |
| `07_asset_detail.png` | 點進 `database/T_TXN_PS` 這類資產 | ①右上 Materialize 按鈕 ②分區狀態列 ③下方相依圖 |
| `08_partition_picker.png` | 點 Materialize 後的分區選擇視窗 | ①分區清單 ②Launch 按鈕 ③Upstream 選項 |
| `09_backfill.png` | 選取多個分區後的 backfill 畫面 | ①分區範圍 ②涵蓋的資產清單 ③Submit 按鈕 |
| `10_sensor_detail.png` | 點進 `daily_file_watcher_sensor` | ①右上開關與 Reset cursor ②Tick history ③展開的 tick log |

---

## 補充建議(非必要,但很有用)

| 檔名 | 內容 |
|---|---|
| `11_asset_graph.png` | Assets → 切到 Graph 檢視,截一張完整的資產相依圖 |
| `12_asset_group.png` | Assets 頁面依 group 摺疊的樣子,讓人快速理解四條線 |
| `13_run_timeline.png` | Overview 的 timeline,讓維運人員知道正常的一天長什麼樣 |
