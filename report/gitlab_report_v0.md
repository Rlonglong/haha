# gitlab/gitlab-ee:latest — Trivy 弱點報告 (v0)

掃描來源：`./report/gitlab_v0.txt`（trivy image scan，base: ubuntu 24.04）
本輪報告範圍：**CRITICAL** 等級（共 22 筆，11 筆為 CVE/GHSA、11 筆為 secrets 偵測）。
HIGH / MEDIUM 將於後續 v0 報告補充段落或下一輪迭代處理；LOW 等級依 PROCESS.md 規則逐項列出風險可接受理由。

---

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

## 備註：Low 等級弱點

本輪報告聚焦 CRITICAL，Low 等級弱點之逐項風險接受說明將在處理完 HIGH / MEDIUM 後，於 `gitlab_report_v0.md` 補充完整版本中一併列出（避免與本次 CRITICAL 修補進度混淆）。
