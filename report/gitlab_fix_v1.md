# gitlab — 修補紀錄 v1（CRITICAL 等級）

對應 image tag：`dai/gitlab:v1.0`
對應 Dockerfile：`./dockerfile/Dockerfile.gitlab`
對應報告：`./report/gitlab_report_v0.md`

## 本次修補範圍

僅處理上一份報告「一、目前有可修補版本」區塊列出的 5 筆 CRITICAL CVE（實際對應 2 個套件升級動作）：

| CVE | 套件 | 處置 |
|-----|------|------|
| CVE-2019-19919 | handlebars | 升級至 4.7.7（同時覆蓋此 CVE 的修補版本 4.3.0/3.0.8） |
| CVE-2021-23369 | handlebars | 升級至 4.7.7 |
| CVE-2021-23383 | handlebars | 升級至 4.7.7（與 CVE-2021-23369 同一修補提交，trivy DB 未列版本但實際隨同解決） |
| CVE-2026-42257 | net-imap | 升級至 0.6.4 |
| CVE-2026-42258 | net-imap | 升級至 0.6.4（與 CVE-2026-42257 同一漏洞的重複公告） |

## 修補方法

1. **net-imap**：用 `gem install net-imap -v 0.6.4` 直接安裝到 omnibus 內嵌的 gem 路徑，覆蓋原有 0.4.21。net-imap 是 GitLab「以郵件回覆 issue/MR」功能用到的葉節點 gem，沒有被其他 gem 深度依賴鎖版，覆蓋升級風險低。
2. **handlebars**：用 `npm install -g handlebars@4.7.7` 升級 omnibus 內嵌 npm 工具鏈自身用到的 handlebars（此版本被偵測在 "Node.js (node-pkg)" target 下，是 npm 內部相依而非 gitlab-rails 應用層相依），不影響 GitLab Rails 應用本身。

## requirements_gitlab.in 說明

PROCESS.md 預設每個服務會有 `requirements_xx.in`，但 GitLab 為 Ruby/Node 為主的 Omnibus 套件，並非 pip/python 體系，沒有對應的 `requirements.in` 可用。版本鎖定改以 Dockerfile 內的 `gem install` / `npm install` 指令直接表達，相依 CVE 對照已在 `Dockerfile.gitlab` 註解中列出。

## 待辦 / 下一輪

- 本輪只處理 CRITICAL；HIGH（81 筆）、MEDIUM（93 筆）將於下一輪報告（`gitlab_report_v1.md` 或擴充 v0）中依同樣三分類方式處理。
- CRITICAL 中無法修補的 6 筆（grpc-go、pgx/v5，皆為官方靜態編譯 Go binary）與 11 筆 secrets 假陽性，已記錄在 `./vulnerability/gitlab_vulner_report.md`，不在本次 image 修補範圍。

## 待執行（需要實際 Docker/Trivy 環境）

> 此 remote 容器內沒有可用的 docker daemon 與 trivy，以下指令需在有 Docker 與 Trivy 的環境中執行，目前僅提供 Dockerfile 與文件，尚未實際 build / rescan / 封裝：

```bash
docker build -t dai/gitlab:v1.0 -f dockerfile/Dockerfile.gitlab .
trivy image dai/gitlab:v1.0 > report/gitlab_v1.txt
# 將 v1 掃描結果整理成 report/gitlab_report_v1.md，比對 CRITICAL 是否已清除
docker save dai/gitlab:v1.0 | gzip > images/gitlab_v1.0.tar.gz
```
