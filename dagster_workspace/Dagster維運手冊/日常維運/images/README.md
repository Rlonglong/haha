# 截圖清單

把截圖依下表的檔名放進這個資料夾,文件裡的圖就會自動顯示。

**建議**:框線用**紅色、粗細 3px**,統一風格。
截圖前請確認畫面上**沒有真實帳號、身分證號、金額**等敏感資料。

---

## 01_維運人員_每日操作手冊

| 檔名 | 要截什麼 | 重點要框起來的地方 |
|---|---|---|
| `01_runs_overview.png` | 左邊選單點 Runs 之後的完整頁面 | ①左側選單的「Runs」②表格的「Status」整欄 ③上方狀態篩選器 |
| `02_run_detail.png` | 點進一筆**失敗**的 Run | ①上方 Run ID 與狀態 ②中間步驟清單(紅色那行)③**自動重試的痕跡**——同一步驟出現多次或 retry 標記 |
| `03_run_logs.png` | Run 詳細頁,**已經點選某個步驟**之後顯示出 log 的樣子 | ①被點選的那個步驟(要看得出來「有點到」)②右側/下方出現的 stdout 或 stderr ③log 的層級篩選 |
| `04_reexecute.png` | Run 詳細頁右上角,**下拉選單已展開** | ①「Re-execute from failure」②「Re-execute from selected」③(可選)灰掉或不建議用的「all steps」 |
| `05_sensors_list.png` | Automation / Sensors 頁面清單 | ①每列左邊的開關 ②「Last tick」欄位 |

## 02_開發人員_UI操作手冊

| 檔名 | 要截什麼 | 重點要框起來的地方 |
|---|---|---|
| `06_code_locations.png` | Deployment → Code locations | ①分頁標籤 ②該列狀態(綠色 Loaded)③Reload 按鈕 |
| `07_asset_detail.png` | Catalog 進去某個資產(例如 `database/T_TXN_PS`) | ①左側選單的「**Catalog**」②右上 Materialize 按鈕 ③分區狀態列 |
| `08_partition_picker.png` | 點 Materialize 之後的分區選擇畫面 | ①可拖曳的分區列 ②可手動輸入的起訖日期欄位 ③**Backfill only failed and missing partitions within selection** 這個勾選項 |
| `09_backfill.png` | Materialize 的**下拉箭頭已展開** | ①Materialize 旁邊的**下拉箭頭** ②「**Materialize unsynced**」選項 |
| `10_sensor_detail.png` | 點進 `daily_file_watcher_sensor` | ①**可編輯 cursor 的地方**(重點)②Tick history ③展開的 tick log |

## 血緣圖(兩篇都會用到)

| 檔名 | 要截什麼 | 重點要框起來的地方 |
|---|---|---|
| `14_lineage_view.png` | 血緣圖本體,最好選一個上下游都有東西的資產 | ①上游方向 ②下游方向 ③圖上的節點 |
| `15_view_in_asset_catalog.png` | 在圖上點某個節點後跳出的選單 | ①被點選的節點 ②「**View in asset catalog**」 |
| `16_catalog_lineage_tab.png` | 進到資產頁面後的上方標籤列 | ①「**Lineage**」標籤 ②(可選)左邊選單的 Catalog |

---

## 目前狀態

⚠️ **這個資料夾目前還沒有任何圖片檔。**
`01_runs_overview.png` 曾經上傳過但後來被刪除,其餘尚未上傳。
文件裡的圖片連結都已經寫好,檔案放進來就會顯示。
