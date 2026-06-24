# gitlab — Trivy 弱點報告 (v1 rescan 驗證)

掃描來源：`./report/gitlab_v1.txt`（trivy image scan，image tag 顯示為 `dai/gitlab:v1.0`，內容比對後確認實際對應 `Dockerfile.gitlab` v1.1 的修補內容——詳見「附註：image tag 命名疑點」）。
比對基準：`./report/gitlab_v0.txt`（修補前原始掃描）。
本報告目的：依 PROCESS.md 第 3 步要求，驗證 v1.0/v1.1 宣稱「已修補」的項目是否在重新掃描後真的消失。

**結論先講：OS（apt）與大部分 node-pkg（npm）修補確認生效；但全部 15 個 gemspec（gitlab-rails gem）修補沒有真正清除舊版弱點，包含原本最關鍵的 CRITICAL net-imap 修補。**

---

## 總量比對

| | LOW | MEDIUM | HIGH | CRITICAL | 合計 |
|---|---|---|---|---|---|
| v0（修補前） | 28 | 263 | 572 | 22 | 885 |
| v1（修補後重掃） | 27 | 242 | 564 | 20 | 853 |
| 變化 | -1 | -21 | -8 | -2 | -32 |

總數有下降，但下降幅度遠小於預期（OS + node-pkg + 15 個 gem 升級應該要清掉遠超過 32 筆）。逐項拆解如下。

---

## 一、確認修補生效的項目

### OS（apt）—— 全部生效

`gitlab/gitlab-ee:latest (ubuntu 24.04)` target 重掃後：`Total: 28 (LOW: 5, MEDIUM: 23, HIGH: 0, CRITICAL: 0)`。

- `libgnutls30t64`：12 筆 MEDIUM 全部清除。
- `libsystemd0` / `libudev1`：CVE-2026-40226（MEDIUM）清除；CVE-2026-40228（LOW，上游無修補版本）依原計畫保留，圍堵措施維持有效。
- `liblzma5`：CVE-2026-34743（LOW）清除。
- `libgcrypt20`：CVE-2024-2236 Marvin Attack（LOW）依原計畫保留（上游無修補版本，圍堵措施維持有效）。

**結論：apt 升級的修補機制（`apt-get install --only-upgrade`）有效，dpkg 單版本特性使得舊版本確實被取代，無需額外動作。**

### node-pkg（npm）—— 大部分生效，2 個套件需要再升版

`Node.js (node-pkg)` target 重掃後：`Total: 12 (LOW: 3, MEDIUM: 2, HIGH: 6, CRITICAL: 1)`，僅剩 `diff`、`handlebars`、`thrift`、`markdown`（非本次處理對象）有殘留：

| 套件 | 狀況 |
|------|------|
| `ini` / `json` / `npm` / `pug` / `yaml` | **全部清除，0 筆殘留** |
| `thrift` | 符合預期：CVE-2026-41636 已清除，CVE-2026-43870（上游無修補版本）依計畫保留 |
| `diff` | **未完全清除**：升級到 3.5.0 仍被標記 CVE-2026-24001（LOW），fixed version 門檻其實是 `>= 3.5.1`，是我先前選版時的疏漏，差一個 patch 版本 |
| `handlebars` | **出現新的 CRITICAL**：CVE-2026-33937（CRITICAL，fix 4.7.9）+ CVE-2026-33938/33939/33940/33941（HIGH）+ CVE-2026-33916（MEDIUM）+ 2 筆 LOW，全部是 v0 掃描當時還沒公開、在 v1 掃描時才新出現的揭露，**不是修補失敗**，是 4.7.7 本身在這段時間內被發現新弱點，需要再升版到 4.7.9 |

**結論：npm 全域安裝機制本身有效（同套件單版本取代），`diff` 需把目標版本上修，`handlebars` 需要再升版因應新公開的 CVE。**

---

## 二、確認修補沒有生效的項目：全部 15 個 gemspec 升級

`Ruby` target 重掃後：`Total: 55 (LOW: 14, MEDIUM: 24, HIGH: 15, CRITICAL: 2)`。

逐一比對 Report Summary，發現**每一個**我們用 `gem install <pkg> -v <新版本>` 升級的套件，磁碟上都同時存在「舊版本 gemspec（仍被標記弱點）」與「新版本 gemspec（乾淨）」：

| 套件 | 舊版本（仍被標記） | 新版本（已安裝且乾淨） | 仍殘留的 CVE |
|------|------|------|------|
| **net-imap** | 0.4.21（8 筆，含原本最重要的 CRITICAL） | 0.6.4.1 | **CVE-2026-42257 / CVE-2026-42258（CRITICAL）** 等 8 筆全部還在 |
| addressable | 2.8.7 | 2.9.0 | CVE-2026-35611（HIGH） |
| aws-sdk-s3 | 1.149.1 | 1.224.0（注意：非 Dockerfile 指定的 1.208.0，見下方附註） | CVE-2025-14762（MEDIUM） |
| concurrent-ruby | 1.2.3 / 1.3.6 | 1.3.7 | CVE-2026-54904/54905/54906（HIGH） |
| css_parser | 1.14.0 | 1.22.0 | CVE-2026-44312（MEDIUM） |
| doorkeeper-openid_connect | 1.9.0 | （1.10.0 未在掃描中確認新版乾淨記錄） | CVE-2026-44476 |
| erb | 4.0.3 | 4.0.3.1 | CVE-2026-41316（HIGH） |
| excon | 1.3.0 | 1.5.0 | CVE-2026-54171（MEDIUM） |
| faraday | 2.8.1 / 2.14.1 / 2.14.2 | 2.14.3 | CVE-2026-54297/33637/25765 |
| nokogiri | 1.19.3 | 1.19.4 | GHSA-5prr-v3j2-97mh（MEDIUM）+ 7 筆 LOW |
| oauth2 | 2.0.20 | 2.0.22 | GHSA-pp92-crg2-gfv9 |
| puma | 8.0.1 | 8.0.2 | CVE-2026-47736/47737 |
| sidekiq-cron | 2.3.1 | 2.4.0 | CVE-2025-67202（MEDIUM） |
| thor | 1.2.2 | 1.5.0（注意：非 Dockerfile 指定的 1.4.0，見下方附註） | CVE-2025-54314（LOW） |
| view_component | 3.23.2 | 3.25.0 | CVE-2026-44836/44837（MEDIUM） |

### 根本原因

`gem install <pkg> -v <新版本>` 在 RubyGems 是**新增式安裝**：它把新版本的 gemspec / 檔案裝進共用的 gem 目錄，**不會移除或取代舊版本**。Trivy 的 gemspec 掃描是直接列舉磁碟上所有實體存在的 `.gemspec` 檔案（在 Report Summary 看到的是各版本各自獨立一個 target，例如 `specifications/net-imap-0.4.21.gemspec`），所以只要舊版本還在磁碟上，就會持續被標記，無論裝了多新的版本。

進一步比對 v0 原始（未修補）掃描可確認：**這個多版本並存現象是 GitLab 官方 image 本來就有的內建狀態**，不是我們這次操作造成的——v0 掃描裡 `net-imap` 就已經同時存在 0.4.21 與 0.6.4 兩個版本（後者推測是 omnibus 建置過程中某個依賴帶進來的）。

更深一層的風險是：GitLab 的 `gitlab-rails/Gemfile.lock` 把每個 gem 鎖在精確版本，Bundler（尤其 Omnibus 慣用的 frozen/deployment 模式）執行期只會載入 **Gemfile.lock 鎖定的版本**。也就是說，即使我們多裝了新版本，gitlab-rails 實際執行時很可能仍然載入舊的、Gemfile.lock 鎖定的那個版本——升級可能根本沒有在執行期生效，只是讓 Trivy 看到「多一個乾淨版本」而已，弱點面並未真正縮小。

### 修正方案（已設計，尚待實機驗證）

改用以下機制取代單純的 `gem install`：

```bash
BUNDLE_GEMFILE=/opt/gitlab/embedded/service/gitlab-rails/Gemfile \
  /opt/gitlab/embedded/bin/bundle update --local --conservative <gem 名稱...>
/opt/gitlab/embedded/bin/gem cleanup <gem 名稱...>
```

- `gem install` 步驟仍須先執行，把新版本裝進本機 gem 目錄。
- `bundle update --local --conservative`：在不連網（`--local`，因為新版本已經在前一層裝好）的前提下，只更新 `Gemfile.lock` 裡這幾個套件的鎖定版本到本機已有的新版本，且 `--conservative` 限制不去動其他沒指定的相依套件版本，降低牽連風險。
- `gem cleanup <gem 名稱...>`：在 Gemfile.lock 改完之後，實體移除磁碟上不再被任何 Gemfile.lock 引用的舊版本檔案，這樣 Trivy 才會真的看不到舊版本的 gemspec。

**注意：此方案目前只能靠推理確認其機制正確，本機 sandbox 沒有 docker/bundler/trivy 可以實際驗證，需要使用者在有 Docker 環境的機器上 build + rescan 才能確認真的清除。**

---

## 三、其他需要修正的版本號

| 套件 | Dockerfile 目前指定 | v1 掃描實際看到的新版本 | 處置 |
|------|------|------|------|
| `handlebars` | 4.7.7 | 4.7.7（乾淨，但新 CVE 出現） | 上修到 4.7.9 |
| `diff` | 3.5.0 | 3.5.0（仍被標記） | 上修到 ≥ 3.5.1（建議直接拉高到較新版本留緩衝） |
| `thor` | 1.4.0 | 實際裝到 1.5.0 | 待確認原因（見下） |
| `aws-sdk-s3` | 1.208.0 | 實際裝到 1.224.0 | 待確認原因（見下） |

`thor` 與 `aws-sdk-s3` 的版本號跟 Dockerfile 裡寫的不一致，原因尚未確認，可能是：(a) 實際 build 時用的 Dockerfile 版本跟目前看到的不完全一樣；(b) `gem install -v` 在某些相依條件下被 resolver 調整到其他版本。這兩個套件目前掃描結果都是**乾淨**的（無 CVE），所以不影響「新版本是否安全」的結論，但會在下一輪 Dockerfile 修正時把版本號對齊成實際使用的版本，避免文件與實際不一致。

---

## 附註：image tag 命名疑點

`gitlab_v1.txt` 掃描結果標頭顯示的 image tag 是 `dai/gitlab:v1.0`，但內容裡同時看得到 `net-imap-0.6.4.1.gemspec`、`liblzma5` 等只在 v1.1 版 Dockerfile 才有的修補項目，代表實際 build 出來的 image 內容是 v1.1（或更新）的程度，只是沒有重新打 tag 成 v1.1。這點請使用者確認 build 流程中的 tag 命名是否需要校正，避免之後版本對照混淆。

---

## 待辦 / 下一輪（v1.2）

1. 修正 `Dockerfile.gitlab`：
   - 把 gemspec 區塊的 `gem install` 改為「`gem install` → `bundle update --local --conservative` → `gem cleanup`」三段式。
   - `handlebars` 4.7.7 → 4.7.9。
   - `diff` 3.5.0 → 更高版本（如 5.2.2）。
   - 核對並對齊 `thor`、`aws-sdk-s3` 實際版本號。
2. 修正後需要使用者在有 Docker/Trivy 的環境重新 build + rescan，產出 `gitlab_v2.txt`，再次驗證 gemspec 弱點是否真正清除（包含驗證 gitlab-rails 應用本身開機正常，因為 `bundle update` 會改 `Gemfile.lock`，需確認沒有破壞既有相依關係）。
3. 待確認 `doorkeeper-openid_connect` 1.10.0 新版本在重掃後是否乾淨（v1 掃描資料行被截斷未完整確認，下一輪重新檢查）。
