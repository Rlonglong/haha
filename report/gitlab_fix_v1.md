# gitlab — 修補紀錄 v1（CRITICAL → HIGH/MEDIUM/LOW）

對應 image tag：`dai/gitlab:v1.0`（CRITICAL）→ `dai/gitlab:v1.1`（本次擴充 HIGH/MEDIUM/LOW）
對應 Dockerfile：`./dockerfile/Dockerfile.gitlab`
對應報告：`./report/gitlab_report_v0.md`

## v1.0 修補範圍（CRITICAL）

僅處理上一份報告「一、目前有可修補版本」區塊列出的 5 筆 CRITICAL CVE（實際對應 2 個套件升級動作）：

| CVE | 套件 | 處置 |
|-----|------|------|
| CVE-2019-19919 | handlebars | 升級至 4.7.7（同時覆蓋此 CVE 的修補版本 4.3.0/3.0.8） |
| CVE-2021-23369 | handlebars | 升級至 4.7.7 |
| CVE-2021-23383 | handlebars | 升級至 4.7.7（與 CVE-2021-23369 同一修補提交，trivy DB 未列版本但實際隨同解決） |
| CVE-2026-42257 | net-imap | 升級至 0.6.4 |
| CVE-2026-42258 | net-imap | 升級至 0.6.4（與 CVE-2026-42257 同一漏洞的重複公告） |

### 修補方法

1. **net-imap**：用 `gem install net-imap -v 0.6.4` 直接安裝到 omnibus 內嵌的 gem 路徑，覆蓋原有 0.4.21。net-imap 是 GitLab「以郵件回覆 issue/MR」功能用到的葉節點 gem，沒有被其他 gem 深度依賴鎖版，覆蓋升級風險低。
2. **handlebars**：用 `npm install -g handlebars@4.7.7` 升級 omnibus 內嵌 npm 工具鏈自身用到的 handlebars（此版本被偵測在 "Node.js (node-pkg)" target 下，是 npm 內部相依而非 gitlab-rails 應用層相依），不影響 GitLab Rails 應用本身。

## v1.1 修補範圍（HIGH / MEDIUM / LOW）

承接 `./report/gitlab_report_v0.md` 新增的 HIGH/MEDIUM/LOW 段落，本輪處理三類「目前有可修補版本」項目：

### 1. 修正 v1.0 遺留問題：net-imap 0.6.4 → 0.6.4.1

v1.0 將 net-imap 升級到 0.6.4 解決了 2 筆 CRITICAL，但 0.6.4 本身仍受 CVE-2026-42256（MEDIUM）、CVE-2026-47240（MEDIUM）、CVE-2026-47242（MEDIUM）、CVE-2026-47241（LOW）共 4 筆影響。本輪將目標版本改為 `0.6.4.1`，一次解決全部尚存的 net-imap CVE。

### 2. OS（apt）：5 個套件

| 套件 | 修補版本 | 涵蓋 CVE 數 |
|------|---------|------------|
| libgnutls30t64 | 3.8.3-1.1ubuntu3.6 | 12（MEDIUM） |
| libsystemd0 / libudev1 | 255.4-1ubuntu8.16 | 1（MEDIUM，CVE-2026-40226） |
| liblzma5 | 5.6.1+really5.4.5-1ubuntu0.3 | 1（LOW） |
| libgcrypt20 | 套件管理員提供之最新版本 | 0（CVE-2024-2236 Marvin Attack 上游無修補版本，僅嘗試取得套件維護者後續 patch，無法保證清除，見圍堵報告） |

用 `apt-get install -y --only-upgrade` 而非指定精確版本號，避免 base image 的 ubuntu 套件源版本與我們手動指定的版本字串不一致導致安裝失敗；以套件管理員當下提供的最新版本為準。

### 3. node-pkg（npm，omnibus 內嵌工具鏈內部相依）：7 個套件

diff@3.5.0、ini@1.3.6、json@10.0.0、npm@6.14.6、pug@3.0.3、thrift@0.23.0（僅解決 CVE-2026-41636，CVE-2026-43870 上游無修補版本）、yaml@2.8.3。皆為 npm 工具鏈自身相依，不影響 gitlab-rails 執行期。

### 4. gemspec（gitlab-rails 葉節點 gem）：15 個套件

addressable@2.9.0、aws-sdk-s3@1.208.0、concurrent-ruby@1.3.7、css_parser@1.22.0、doorkeeper-openid_connect@1.10.0、erb@4.0.3.1、excon@1.5.0、faraday@2.14.3、net-imap@0.6.4.1、nokogiri@1.19.4、oauth2@2.0.22、puma@8.0.2、sidekiq-cron@2.4.0、thor@1.4.0、view_component@3.25.0。

**篩選原則**：僅挑選 patch/minor 版本升級、或修補版本仍落在同一主版本內（如 view_component `>= 3.25.0, < 4.0.0`）的套件，理由是這類升級不改變公開 API 介面，不需要同步調整呼叫端程式碼。明確排除以下 5 個需要跨主版本升級的框架層 gem：

| 套件 | 目前版本 | 需升級到 | 排除理由 |
|------|---------|---------|---------|
| activesupport | 7.0.8.4 | 7.2.3.1+ / 8.x | Rails 核心框架元件，版本與 GitLab 整體 Rails 主版本鎖定耦合 |
| devise | 4.9.4 | 5.0.3+ | 認證框架跨主版本升級，影響全站登入/認證流程 |
| carrierwave | 1.3.4 | 2.2.5+ / 3.0.5+ | 檔案上傳框架跨主版本升級，GitLab 多處 uploader 類別繼承其 API |
| oauth | 0.5.6 | 1.1.6+ | OAuth 1.0 client 跨主版本升級，影響既有第三方整合 |
| sinatra | 3.2.0 | 4.2.0+ | Web 框架跨主版本升級，多處工具/中介層耦合其 3.x API |

這 5 個套件改採「風險可接受 + 主動圍堵措施」處置，並等待 GitLab 官方在下一個 release 中同步升級整個 Rails/Gemfile.lock 後一併解決，理由與圍堵措施詳見 `./vulnerability/gitlab_vulner_report.md`。

## requirements_gitlab.in 說明

PROCESS.md 預設每個服務會有 `requirements_xx.in`，但 GitLab 為 Ruby/Node 為主的 Omnibus 套件，並非 pip/python 體系，沒有對應的 `requirements.in` 可用。版本鎖定改以 Dockerfile 內的 `gem install` / `npm install` / `apt-get install --only-upgrade` 指令直接表達，相依 CVE 對照已在 `Dockerfile.gitlab` 註解中列出。

## 待辦 / 下一輪

- 本輪（v1.0 + v1.1）已涵蓋 CRITICAL、HIGH、MEDIUM、LOW 四個等級中「可直接升級解決」的全部項目。
- 無法自行修補的項目（5 個框架層 gem、os 無上游修補套件如 busybox/util-linux 家族/glibc/tar/wget、node-pkg thrift CVE-2026-43870、cargo bytes/time、83 組 gobinary）已全部記錄在 `./vulnerability/gitlab_vulner_report.md`，附帶風險可接受理由與主動圍堵措施，不在本次 image 修補範圍，待 GitLab 官方下一版 release 後重新掃描確認。

## v1.2 修正（依 v1 實際重新掃描結果發現並修正，詳見 `./report/gitlab_report_v1.md`）

使用者在 `./report/gitlab_v1.txt` 提供了 v1.1 image 實際 build 後的 trivy 重新掃描結果，比對後發現：

1. **OS（apt）與大部分 node-pkg（npm）修補確認生效**：libgnutls30t64、libsystemd0/libudev1、liblzma5、ini、json、npm、pug、yaml、thrift（部分）全部如預期清除。
2. **`diff`、`handlebars` 需要再升版**：`diff` 3.5.0 仍被標記 CVE-2026-24001（實際修補門檻 `>= 3.5.1`，選版時少看一個 patch 版本），改為 5.2.2；`handlebars` 4.7.7 在 v1 掃描時被標記新公開的 CVE-2026-33937（CRITICAL，v0 掃描時尚未公開，非修補失敗），改為 4.7.9。
3. **全部 15 個 gemspec 修補（含 net-imap）實際沒有清除**：根因是 `gem install -v <新版>` 在 RubyGems 屬於新增式安裝，不會移除舊版本，磁碟上新舊版本並存，trivy 直接掃實體 gemspec 檔案所以仍標記舊版；更嚴重的是 gitlab-rails 透過 `Gemfile.lock` 鎖定精確版本，Bundler 執行期可能仍載入舊版本，等同升級沒有真正在應用層生效。

   修正為 `gem install`（裝新版） → `BUNDLE_GEMFILE=.../Gemfile bundle update --local --conservative <gems>`（更新 Gemfile.lock 鎖定版本，僅限本機已裝版本，不牽動其他套件） → `gem cleanup <gems>`（移除磁碟上不再被引用的舊版本檔案）三段式，已寫入 `Dockerfile.gitlab` v1.2。**此修正機制目前只能靠推理確認正確，本機 sandbox 無 docker/bundler/trivy 可驗證，需使用者在實機 build + rescan 後才能確認 gemspec 弱點是否真正清除、以及 gitlab-rails 是否能正常開機。**

## 待執行（需要實際 Docker/Trivy 環境）

> 此 remote 容器內沒有可用的 docker daemon 與 trivy，以下指令需在有 Docker 與 Trivy 的環境中執行，目前僅提供 Dockerfile 與文件，尚未實際 build / rescan / 封裝：

```bash
docker build -t dai/gitlab:v1.2 -f dockerfile/Dockerfile.gitlab .
trivy image dai/gitlab:v1.2 > report/gitlab_v2.txt
# 將 v2 掃描結果跟 report/gitlab_v1.txt 比對，重點確認：
#   1) 15 個 gemspec 套件（含 net-imap）的舊版本 gemspec 是否真的從磁碟上消失
#   2) gitlab-rails 服務是否能正常啟動（bundle update 改了 Gemfile.lock，需驗證沒有破壞相依關係）
#   3) handlebars / diff 的新版本是否清除原本標記的 CVE
docker save dai/gitlab:v1.2 | gzip > images/gitlab_v1.2.tar.gz
```
