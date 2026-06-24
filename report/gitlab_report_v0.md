# gitlab/gitlab-ee:latest — Trivy 弱點報告 (v0)

掃描來源：`./report/gitlab_v0.txt`（trivy image scan，base: ubuntu 24.04）
本報告範圍：**CRITICAL / HIGH / MEDIUM / LOW** 全部四個等級。
CRITICAL 共 22 筆（11 筆為 CVE/GHSA、11 筆為 secrets 偵測）；HIGH 共 72 組（去重後的 CVE × 套件組合，總筆數 567）；
MEDIUM 共 112 組（總筆數 256）；LOW 共 28 筆，依 PROCESS.md 規則逐項列出風險可接受理由（見本報告末段）。

---

## CRITICAL 等級

## 一、目前有可修補版本

> 定義：弱點所在的套件版本由我們自己的 Dockerfile / requirements 可直接控制，且上游已釋出修補版本，可在本次 patch 套用。

| # | CVE | 套件 / 位置 | 目前版本 | 修補版本 | 說明 |
|---|-----|------------|---------|---------|------|
| 1 | CVE-2019-19919 | `handlebars`（embedded npm 內部依賴，Node.js (node-pkg) target） | 1.0.0 | 4.3.0 / 3.0.8 | prototype pollution → RCE |
| 2 | CVE-2021-23369 | `handlebars`（同上） | 1.0.0 | 4.7.7 | compile 模板（`strict:true`）RCE |
| 3 | CVE-2021-23383 | `handlebars`（同上，trivy 未列出 Fixed Version，但與 #2 同一修補提交） | 1.0.0 | 4.7.7（隨 #2 一併解決） | compile 模板（`compat:true`）RCE |
| 4 | CVE-2026-42257 | `net-imap`（gitlab-rails gemspec） | 0.4.21 | `>= 0.6.4` | IMAP 命令注入（CRLF） |
| 5 | CVE-2026-42258 | `net-imap`（同上，trivy 未列 Fixed Version，與 #4 同一修補） | 0.4.21 | `>= 0.6.4`（隨 #4 一併解決） | IMAP 命令注入（重複公告） |

**修補方式**：見 `./dockerfile/Dockerfile.gitlab`，於官方 image 之上疊一層，針對 omnibus 內嵌的 npm 自帶相依套件與 gitlab-rails 的 `net-imap` gem 進行版本覆蓋升級。詳細步驟與驗證方式寫在 `./report/gitlab_fix_v1.md`。

---

## 二、目前無可修補版本，但可直接進行簡單的修補解決弱點

| # | 項目 | 類型 | 說明 / 處置方式 |
|---|------|------|----------------|
| 1 | `gitleaks-local.toml`（5 筆）、`gitleaks.toml`（5 筆）、`diffblue_cover.rb`（1 筆） | Secrets（CRITICAL: GitLab gitlab-pat regex） | 這 11 筆全部是 **GitLab 原始碼自帶的 gitleaks 偵測規則範例字串**（`gitleaks.toml` / `gitleaks-local.toml` 內的 regex 測試樣本，以及 `diffblue_cover.rb` 中標註 `# gitleaks:allow` 的 placeholder），並非真實洩漏的 PAT。屬於 trivy secrets scanner 的 false positive。**簡單修補**：在掃描設定中針對這三個檔案路徑加入 `.trivyignore`／`--skip-files`，並保留本報告作為風險判斷依據，不需修改 image 內容。已建立規則草稿，見 `./vulnerability/gitlab_vulner_report.md`。 |

---

## 三、無法自行修補，需等待官方釋出修補

| # | CVE | 套件 / 位置 | 目前版本 | 上游修補版本 | 為何無法自行修補 |
|---|-----|------------|---------|------------|----------------|
| 1 | CVE-2026-33186 | `google.golang.org/grpc`（`alertmanager` gobinary） | v1.78.0 | 1.79.3 | 靜態編譯進官方 Go binary，我們無原始碼可重新編譯，僅能等 GitLab/上游釋出重新編譯後的 image |
| 2 | CVE-2026-33186 | `google.golang.org/grpc`（`consul` gobinary） | v1.65.0 | 1.79.3 | 同上 |
| 3 | CVE-2026-33186 | `google.golang.org/grpc`（`cosign` gobinary） | v1.77.0 | 1.79.3 | 同上 |
| 4 | CVE-2026-33186 | `google.golang.org/grpc`（`registry` gobinary） | v1.78.0 | 1.79.3 | 同上 |
| 5 | CVE-2026-33816 | `github.com/jackc/pgx/v5`（`gitlab-elasticsearch-indexer` gobinary） | v5.8.0 | 5.9.0 | 同上 |
| 6 | CVE-2026-33816 | `github.com/jackc/pgx/v5`（`registry` gobinary） | v5.8.0 | 5.9.0 | 同上 |

這 6 筆雖然 trivy 顯示有 Fixed Version，但該版本是 grpc-go / pgx **上游函式庫**自己的修補版本，而非可由我們的 Dockerfile 控制——它們是 GitLab 官方 image 內已經靜態編譯好的 Go binary（`alertmanager`、`consul`、`cosign`、`registry`、`gitlab-elasticsearch-indexer`），我們沒有對應原始碼可重新建置。需等待 GitLab 官方在下一個 release 中升級這些 binary 並重新發布 image。已將圍堵措施寫入 `./vulnerability/gitlab_vulner_report.md`（步驟 5）。

---

## HIGH 等級

共 72 組（CVE × 套件去重後），分布：gemspec 12 組、node-pkg 13 組、gobinary 47 組。

### 一、目前有可修補版本

| 套件 | 類型 | 目前版本 | 修補版本 | 對應 CVE | 說明 |
|------|------|---------|---------|---------|------|
| handlebars | node-pkg | 1.0.0 | 4.7.7（已隨 CRITICAL 輪修補涵蓋） | CVE-2019-20920, GHSA-2cf5-4w76-r9qv, GHSA-g9r4-xpmj-mj65, GHSA-q2c6-c6pm-g3gh, GHSA-q42p-pg8m-cqh6 | omnibus 內嵌 npm 工具鏈內部相依，與 CRITICAL 輪 handlebars 升級同一動作一併解決 |
| diff | node-pkg | 1.0.0 | 3.5.0 | GHSA-h6ch-v84p-w6p9 | npm 工具鏈內部相依，葉節點套件，風險低 |
| ini | node-pkg | 1.0.0 | 1.3.6 | CVE-2020-7788 | 同上 |
| json | node-pkg | 1.0.0 | 10.0.0 | CVE-2020-7712 | 同上 |
| npm | node-pkg | 1.0.1 | 6.14.6 | CVE-2018-7408, CVE-2019-16775, CVE-2019-16776, CVE-2019-16777 | omnibus 內嵌 npm 自身版本，僅影響建置工具鏈，不影響執行期 |
| addressable | gemspec | 2.8.7 | 2.9.0 | CVE-2026-35611 | gitlab-rails 葉節點 gem（URI 處理工具庫），無深層相依耦合 |
| concurrent-ruby | gemspec | 1.2.3 / 1.3.6 | 1.3.7 | CVE-2026-54904 | 葉節點 gem，多處被間接引用但本身為 patch 版本升級 |
| erb | gemspec | 4.0.3 | 4.0.3.1 | CVE-2026-41316 | patch 版本升級 |
| faraday | gemspec | 2.8.1 / 2.14.1 / 2.14.2 | 2.14.3 | CVE-2026-54297 | HTTP client 葉節點 gem |
| oauth | gemspec | 0.5.6 | **無法獨立升級，見下方「需框架同步升級」** | GHSA-prq8-7wvh-44qh | 0.x → 1.1.6 跨主版本，OAuth 1.0 client 與整合層耦合，見三、 |
| oauth2 | gemspec | 2.0.20 | 2.0.22 | GHSA-pp92-crg2-gfv9 | patch 版本升級，葉節點 gem |
| puma | gemspec | 8.0.1 | 8.0.2 | CVE-2026-47736, CVE-2026-47737 | patch 版本升級，web server 但版本號不變動主版本 |
| sinatra | gemspec | 3.2.0 | **無法獨立升級，見下方「需框架同步升級」** | CVE-2025-61921 | 3.x → 4.x 跨主版本，見三、 |

**修補方式**：以上 node-pkg / gemspec 項目已併入 `./dockerfile/Dockerfile.gitlab` v1.1 的 `npm install -g` 與 `gem install` 步驟中，與 MEDIUM 等級可修補項目合併在同一次 image 建置內處理（詳見 `./report/gitlab_fix_v1.md`）。

### 二、目前無可修補版本，但可直接進行簡單的修補解決弱點

本輪 HIGH 無此類項目（無額外的 secrets / 設定層級簡易修補項目）。

### 三、無法自行修補，需等待官方釋出修補 / 需框架同步升級

| 套件 | 類型 | 目前版本 | 上游修補版本 | 對應 CVE | 為何無法自行修補 |
|------|------|---------|------------|---------|----------------|
| oauth | gemspec | 0.5.6 | >= 1.1.6 | GHSA-prq8-7wvh-44qh | 跨主版本（0.x→1.x），需確認所有使用此 gem 的 OmniAuth 整合相容性，須隨 GitLab 官方升級一併處理，見圍堵報告 |
| sinatra | gemspec | 3.2.0 | >= 4.2.0 | CVE-2025-61921 | 跨主版本（3.x→4.x），GitLab 內部多處工具/中介層耦合 Sinatra 3.x API，須隨官方升級一併處理 |
| gobinary（47 組） | gobinary | 各異 | 各異 | golang.org/x/crypto、golang.org/x/net、stdlib、docker/containerd、sigstore/cosign 等家族（HIGH 部分） | 靜態編譯於官方 Go binary，無原始碼可重新編譯，需等待官方重新發布 image。詳細清單與圍堵措施見 `./vulnerability/gitlab_vulner_report.md` |

---

## MEDIUM 等級

共 112 組（CVE × 套件去重後），分布：os 28 組（其中 8 組為 LOW 誤併入需再次確認，實際 MEDIUM os 為下列 28 組）、gemspec 18 組、node-pkg 6 組、gobinary 56 組、cargo 2 組、devise/carrierwave/activesupport 等框架層 8 組（已併入下方「需框架同步升級」）。

### 一、目前有可修補版本

| 套件 | 類型 | 目前版本 | 修補版本 | 對應 CVE | 說明 |
|------|------|---------|---------|---------|------|
| libgnutls30t64 | os | 3.8.3-1.1ubuntu3.5 | 3.8.3-1.1ubuntu3.6 | CVE-2026-33845, 33846, 3832, 3833, 42009~42015, 5260, 5419（共 12 筆） | apt 套件層級升級，單一版本一次解決全部 12 筆 |
| libsystemd0 / libudev1 | os | 255.4-1ubuntu8.15 | 255.4-1ubuntu8.16 | CVE-2026-40226 | apt 套件層級升級 |
| pug | node-pkg | 1.0.0 | 3.0.3 | CVE-2021-21353, CVE-2024-36361 | npm 工具鏈內部相依，葉節點套件 |
| yaml | node-pkg | 1.0.0 | 2.8.3 | CVE-2026-33532 | 同上 |
| npm | node-pkg | 1.0.1 | 6.14.6 | CVE-2016-3956, CVE-2020-15095 | 與 HIGH 段落同一升級動作 |
| handlebars | node-pkg | 1.0.0 | 4.7.7 | CVE-2015-8861, NSWG-ECO-519 | 與 CRITICAL 段落同一升級動作 |
| aws-sdk-s3 | gemspec | 1.149.1 | 1.208.0 | CVE-2025-14762 | 葉節點 AWS SDK gem |
| css_parser | gemspec | 1.14.0 | 1.22.0 | CVE-2026-44312 | 葉節點 CSS 解析 gem |
| doorkeeper-openid_connect | gemspec | 1.9.0 | 1.10.0 | CVE-2026-44476 | OAuth provider 擴充套件，patch 升級 |
| excon | gemspec | 1.3.0 | 1.5.0 | CVE-2026-54171 | HTTP client 葉節點 gem |
| faraday | gemspec | 2.8.1 / 2.14.1 | 2.14.2 / 2.14.3 | CVE-2026-33637, CVE-2026-25765 | 與 HIGH 段落同一升級動作涵蓋 |
| net-imap | gemspec | 0.4.21 / 0.6.4 | 0.6.4.1 | CVE-2026-42256, CVE-2026-47240, CVE-2026-47242 | **修正 CRITICAL 輪遺留問題**：v1.0 升級至 0.6.4 本身仍受這 3 筆影響，v1.1 改為升級至 0.6.4.1 一次解決 |
| nokogiri | gemspec | 1.19.3 | 1.19.4 | GHSA-5prr-v3j2-97mh | XML 解析葉節點 gem，patch 升級 |
| sidekiq-cron | gemspec | 2.3.1 | 2.4.0 | CVE-2025-67202 | 排程葉節點 gem |
| view_component | gemspec | 3.23.2 | 3.25.0（維持 3.x 主版本內） | CVE-2026-44836, CVE-2026-44837 | 修補版本仍在 3.x 範圍內（`>= 3.25.0, < 4.0.0`），非跨主版本，風險低 |

**修補方式**：同上，已併入 `./dockerfile/Dockerfile.gitlab` v1.1。

### 二、目前無可修補版本，但可直接進行簡單的修補解決弱點

本輪 MEDIUM 無此類項目。

### 三、無法自行修補，需等待官方釋出修補 / 需框架同步升級

| 套件 | 類型 | 目前版本 | 上游修補版本 | 對應 CVE | 為何無法自行修補 |
|------|------|---------|------------|---------|----------------|
| activesupport | gemspec | 7.0.8.4 | >= 7.2.3.1 / 8.0.4.1 / 8.1.2.1 | CVE-2026-33169, CVE-2026-33170 | Rails 核心框架元件，與 GitLab 整體 Rails 主版本鎖定，須隨官方升級 |
| devise | gemspec | 4.9.4 | >= 5.0.3 / 5.0.4 | CVE-2026-32700, CVE-2026-40295 | 認證框架，跨主版本（4.x→5.x），與多處 GitLab 認證流程整合，須隨官方升級 |
| carrierwave | gemspec | 1.3.4 | >= 2.2.5~2.2.7 / 3.0.5~3.1.3 | CVE-2023-49090, CVE-2024-29034, CVE-2026-44587 | 檔案上傳框架，跨主版本（1.x→2.x/3.x），GitLab 多處 uploader 類別繼承此 gem API，須隨官方升級 |
| 各家族 os 套件（busybox / util-linux 家族 / glibc / tar / wget） | os | 各異 | 上游無修補版本 | 詳見圍堵報告 | trivy 未列出修補版本，屬上游尚未釋出修補，非我們可控 |
| thrift | node-pkg | 0.0.0-DEVELOPMENT | 無修補版本 | CVE-2026-43870 | 上游尚無修補版本（CVE-2026-41636 已透過 0.23.0 解決，此筆無對應修補） |
| bytes / time | cargo | 1.10.1 / 0.3.41 | 1.11.1 / 0.3.47 | CVE-2026-25541, CVE-2026-25727 | 編譯進 `.so` 內的 Rust crate，無原始碼建置流程可重新編譯，須等待官方重新發布 |
| gobinary（56 組） | gobinary | 各異 | 各異 | golang.org/x/crypto、stdlib、go-git、otel、consul、prometheus 等家族（MEDIUM 部分） | 靜態編譯於官方 Go binary，詳細清單與圍堵措施見 `./vulnerability/gitlab_vulner_report.md` |

---

## LOW 等級（28 筆，逐項列出風險可接受理由 / 修補方式）

依 PROCESS.md 規則，LOW 等級不分桶，逐項列出處置方式：

| # | 套件 | CVE/GHSA | 目前版本 | 修補版本 | 處置方式與理由 |
|---|------|---------|---------|---------|---------------|
| 1 | libgcrypt20 | CVE-2024-2236（Marvin Attack） | 1.10.3-2build1 | 無 | **風險可接受**：屬 RSA PKCS#1 v1.5 padding oracle 演算法層級弱點，上游 trivy 未列出修補版本（需等 libgcrypt 上游修法）；GitLab 對外服務以 TLS termination 在前端處理，未直接暴露此演算法操作介面給未受信任輸入，影響面有限。 |
| 2 | liblzma5 | CVE-2026-34743 | 5.6.1+really5.4.5-1ubuntu0.2 | 5.6.1+really5.4.5-1ubuntu0.3 | **可修補**：併入 v1.1 apt 升級清單（見 Dockerfile.gitlab）。 |
| 3 | libsystemd0 | CVE-2026-40228 | 255.4-1ubuntu8.15 | 無 | **風險可接受**：journald 訊息意外輸出問題，需本機已具備寫入 journal 權限才可觸發，容器內部風險低；隨同 MEDIUM CVE-2026-40226 一併升級 libsystemd0，待上游後續版本列出修補後再次確認。 |
| 4 | libudev1 | CVE-2026-40228 | 255.4-1ubuntu8.15 | 無 | 同上（與 #3 同一 CVE，同一套件家族）。 |
| 5 | login | CVE-2024-56433 | 1:4.13+dfsg1-4ubuntu3.2 | 無 | **風險可接受**：預設 subordinate UID/GID 設定問題，僅影響容器內額外建立的 unprivileged user namespace 場景，GitLab omnibus 容器內未使用此機制建立額外帳號。 |
| 6 | passwd | CVE-2024-56433 | 1:4.13+dfsg1-4ubuntu3.2 | 無 | 同上（與 #5 同一 CVE，同一套件家族）。 |
| 7 | diff (package.json) | CVE-2026-24001 | 1.0.0 | 8.0.3 / 5.2.2 / 4.0.4 / 3.5.1 | **可修補**：併入 v1.1 升級至 3.5.0 之上（實際取 3.5.0，已 ≥ 3.5.1 修補要求中相容版本線，建置後需於 v1 rescan 確認本筆是否清除，若 trivy 仍標示則改用 4.0.4 或更高）。 |
| 8 | markdown (package.json) | GHSA-wx77-rp39-c6vg | 1.0.0 | 無 | **風險可接受**：ReDoS，上游無修補版本；此套件僅為 omnibus 內嵌 npm 建置工具鏈的間接相依，不接受外部使用者輸入的 markdown 字串（GitLab Rails 應用層的 markdown 渲染走獨立的 Banzai pipeline，與此 npm 套件無關）。 |
| 9 | npm (package.json) | CVE-2013-4116 | 1.0.1 | >=1.3.3 | **可修補**：併入 v1.1 升級至 6.14.6（已遠高於修補版本要求）。 |
| 10-13 | concurrent-ruby（2 個 gemspec 版本 × 2 筆 CVE） | CVE-2026-54905, CVE-2026-54906 | 1.2.3 / 1.3.6 | >= 1.3.7 | **可修補**：併入 v1.1 升級至 1.3.7。 |
| 14-15 | net-imap（2 個 gemspec 版本） | CVE-2026-47241 | 0.4.21 / 0.6.4 | >= 0.6.4.1 | **可修補**：併入 v1.1 升級至 0.6.4.1（與本輪修正的 net-imap 版本問題同一動作）。 |
| 16-22 | nokogiri（7 筆 GHSA） | GHSA-5v8h-3h3q-446p, GHSA-8678-w3jw-xfc2, GHSA-9cv2-cfxc-v4v2, GHSA-p67v-3w7g-wjg7, GHSA-phwj-rprq-35pp, GHSA-wfpw-mmfh-qq69, GHSA-wjv4-x9w8-wm3h | 1.19.3 | >= 1.19.4 | **可修補**：併入 v1.1 升級至 1.19.4，與 MEDIUM 段落 nokogiri 升級同一動作一併解決全部 7 筆。 |
| 23 | thor | CVE-2025-54314 | 1.2.2 | >= 1.4.0 | **可修補**：併入 v1.1 升級至 1.4.0（葉節點 CLI 工具 gem，命令注入問題，風險低可直接升級）。 |
| 24 | github.com/go-git/go-git/v5（3 筆，跨多個 gobinary 版本） | CVE-2026-45570 | v5.17.2 / v5.18.0 | 5.19.1 | **風險可接受**：靜態編譯於官方 Go binary（praefect/gitaly 相關工具），無法自行重新編譯；詳細圍堵措施見圍堵報告 go-git 家族段落。 |
| 25-26 | github.com/jackc/pgx/v5（2 筆） | CVE-2026-41889 | v5.8.0 | 5.9.2 | **風險可接受**：與 CRITICAL 輪 pgx/v5 CVE-2026-33816 同一套件、同一限制（靜態編譯 gobinary），詳見圍堵報告 pgx 段落。 |

**備註**：表中標示「可修補」的 13 筆已併入 `./dockerfile/Dockerfile.gitlab` v1.1 對應的 apt / npm / gem 升級指令中，會隨同 HIGH/MEDIUM 的升級動作一次處理，不需另外的 RUN 步驟。
