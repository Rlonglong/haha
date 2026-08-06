# dagster_workspace 資料夾結構說明

這裡是 Dagster 排程系統的全部程式與設定。本檔只說明**哪個位置放什麼**；操作方式、新增流程、參數調整請看 [`Dagster維運手冊/`](./Dagster維運手冊/README.md)。

---

## 快速定位：我要做的事情在哪裡

| 我想做的事 | 去哪裡 |
|---|---|
| 我是維運人員，想知道每天要看什麼 | [日常維運/01_維運人員_每日操作手冊.md](./Dagster維運手冊/日常維運/01_維運人員_每日操作手冊.md) |
| 我是開發人員，想知道 UI 怎麼操作 | [日常維運/02_開發人員_UI操作手冊.md](./Dagster維運手冊/日常維運/02_開發人員_UI操作手冊.md) |
| 新增一張來源資料表 | [日常維運/03_新增一張資料表.md](./Dagster維運手冊/日常維運/03_新增一張資料表.md) |
| 新增一支 dbt 模型（SQL） | [日常維運/04_新增一支dbt模型.md](./Dagster維運手冊/日常維運/04_新增一支dbt模型.md) |
| 改 sensor 幾秒掃一次 | [進階調整/10_Sensor掃描頻率與觸發邏輯.md](./Dagster維運手冊/進階調整/10_Sensor掃描頻率與觸發邏輯.md) |
| 查某個設定值是什麼意思 | [附錄A_table_mapping設定詳解.md](./Dagster維運手冊/附錄/A_table_mapping設定詳解.md)(詳解)<br/>[進階調整/16_設定值總表.md](./Dagster維運手冊/進階調整/16_設定值總表.md)(快查) |
| 系統整體怎麼運作 | [00_系統架構總覽.md](./Dagster維運手冊/00_系統架構總覽.md) |

---

## 資料夾結構

> ⚠️ **路徑對照**：本資料夾 `dagster_workspace/` 就是容器裡的 `/app/workspace/`
> （docker 掛載的路徑，**非實際路徑**），對應 VM4 主機上的
> `/data/deploy/workspace/dagster_workspace/`（**實際掛載路徑**）。
> 程式碼裡看到 `/app/workspace/xxx`，就是這裡的 `xxx`。

```
dagster_workspace/
│
├── README.md                     ← 你正在看的這份：資料夾結構說明
├── Dagster維運手冊/                ← 所有文件都在這裡（見下方說明）
│
├── workspace.yml                 ← Dagster 要載入哪個 Python 模組
├── dagster_home/
│   └── dagster.yaml              ← Dagster 實例設定（併發、log、自動觸發）
│
├── dagster_code/                 ← Dagster 的 Python 程式
│   ├── __init__.py               ← <常態不需要改> 總入口：註冊 asset / job / sensor / resource
│   ├── assets.py                 ← <常態不需要改> 核心：EL 節點工廠、dbt 資產、匯出資產
│   ├── table_mapping.py          ← ★最常改的檔案★ 來源表(TABLE_CSV_MAPPING)、匯出(SQL_TO_CSV_MAPPING)
│   ├── sensors.py                ← <常態不需要改> 檔案到站偵測、失敗告警
│   ├── selections.py             ← <常態不需要改> 動態組出「月檔」「手動專用」的資產集合
│   ├── pipes_ssh_client.py       ← <常態不需要改> SSH 到 VM1 執行腳本的連線元件
│   ├── .env                      ← ★不進版控★ 資料庫連線帳密
│   └── db_sync/                  ← DB 直連同步線（跟檔案線平行的另一套）
│       ├── config.py             ← 設定檔：DB_SYNC_MAPPING
│       ├── assets.py             ← 撈取 / 加密 / 入庫 / 清理 四個節點
│       └── sensors.py            ← 輪詢來源 DB 筆數是否穩定
│
└── dbt_project/                  ← dbt 專案（SQL 轉換邏輯）
    ├── dbt_project.yml           ← dbt 專案設定
    ├── profiles.yml              ← ★未進版控★ 資料庫連線（帳密走環境變數，不寫明碼）
    ├── profiles.yml.example      ← 上面那份的範本，新環境複製一份改名即可
    ├── models/
    │   ├── sources.yml           ← ★所有 dbt 要用的來源表都要在這裡登記★
    │   ├── _groups.yml           ← 群組與負責人
    │   ├── intermediate/         ← 中介層（去沖正、淨額表）
    │   └── *.sql                 ← 產出層：37 支詐欺偵測模型
    ├── snapshots/                ← 2 支緩慢變動維度快照
    ├── macros/                   ← 共用的 Jinja macro
    │   ├── anti_fraud/           ← get_config() 代碼註冊表、出入帳判斷式
    │   └── global/               ← safe_divide 等通用工具
    ├── target/                   ← ★編譯產出★ 不進版控，執行後才生成
    ├── logs/                     ← dbt 自己的執行 log
    └── dbt_packages/             ← dbt 套件（目前無）
```

### 手冊資料夾

```
Dagster維運手冊/
├── README.md                          ← 手冊索引：誰該讀哪一篇
├── 00_系統架構總覽.md                   ← 先看這篇建立全貌
│
├── 日常維運/                            ← 照著做就好，不用改程式
│   ├── 01_維運人員_每日操作手冊.md        ← 給日常監控人員（含截圖）
│   ├── 02_開發人員_UI操作手冊.md          ← 給開發人員（含截圖）
│   ├── 03_新增一張資料表.md
│   ├── 04_新增一支dbt模型.md
│   ├── 05_新增一個CSV匯出.md
│   ├── 06_新增一張DB直連同步表.md
│   └── images/                         ← 截圖放這裡
│
├── 進階調整/                            ← 要改程式碼才能達成的調整
│   ├── 10_Sensor掃描頻率與觸發邏輯.md
│   ├── 11_Partition與日期區間.md
│   ├── 12_併發_重試_資源池.md
│   ├── 13_dbt執行行為.md
│   ├── 14_連線_路徑_環境變數.md
│   ├── 15_新增或調整處理節點.md
│   ├── 16_設定值總表.md
│   └── 17_疑難排解與已知問題.md
│
└── 附錄/
    └── A_table_mapping設定詳解.md        ← 每個設定值的意義、預設、限制與設計理由
```

---

## 不在這個資料夾、但流程強相依的東西

| 東西 | 位置 | 說明 |
|---|---|---|
| VM1 執行腳本 | VM1 `/home/bcp_runner/scripts/` | 真正做事的地方：下載、加解密、清洗、BCP、匯出 |
| VM1 清洗函式 | VM1 `/home/bcp_runner/scripts/clean_functions/` | 每張表一支 `{表名}_CLEAN.py` |
| VM1 機密 | VM1 `.env` | ZIP 密碼、來源 DB 解密金鑰 |
| VM4 機密 | `dagster_code/.env` | 我方 DB 連線帳密 |
| dbt 編譯產出 | `dbt_project/target/` | 執行後才生成，不進版控 |

---

## 修改後要做什麼

> 程式碼由 **GitLab CI/CD** 同步到 VM4，dbt 的 manifest 也在同步時一併重新產生，
> 所以正常流程是 **push → 等 pipeline 綠燈 → 到 UI 做 Reload**。

| 改了什麼 | push 後等 CI/CD | Reload definitions | 重啟容器 |
|---|---|---|---|
| `table_mapping.py`、`db_sync/config.py` | ✓ | ✓ | ✗ |
| `assets.py`、`sensors.py`、`selections.py` | ✓ | ✓ | ✗ |
| dbt model、snapshot、`sources.yml`、`macros/` | ✓（順便重產 manifest） | ✓ | ✗ |
| `dagster.yaml`、`workspace.yml` | ✓ | ✗ | **✓** |
| VM1 上的腳本 | 不走這條線，直接改 VM1 | ✗ | ✗（下次執行就生效） |

詳細步驟見 [02_開發人員_UI操作手冊.md](./Dagster維運手冊/日常維運/02_開發人員_UI操作手冊.md)。
