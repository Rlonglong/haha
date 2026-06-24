# 軟體弱點修復程序

## 目錄結構
1. `./dockerfile` : 放 Dockerfile.xx 跟 requirement_xx.in
2. `./images` : 放 `docker save` 的壓縮檔。
3. `./report` : 放 trivy 的掃描結果跟弱點修復過程。
4. `./test_compose` : 放最終 image 的測試，現階段完全不用管這個資料夾。
5. `./vulnerability` : 放漏洞修補的
6. 外圍的 docker-compose 完全不用管。

## 修補流程
1. 透過 trivy 掃描 Docker image，產出 `xx_report_v0.md` 和 `xx_v0.txt`，xx 是該服務的名稱，markdown 是統整 Medium 以上的弱點所寫成的報告。report 的目的是要讓下一步可以快速的進行修補的動作，所以要分成 `目前有可修補版本`、`目前無可修補版本，但可直接進行簡單的修補解決弱點`、`無法自行修補，需等待官方釋出修補` 這三個區塊。
2. 針對第一步的 report，修改 Dockerfile.xx 跟 requirements_xx.in 進行修補，並將修補的方法或措施寫成 `xx_fix_v?.md`。Dockerfile.xx 跟 requirements_xx.in 裡面也簡單用註解列出這個區塊解決什麼 CVE 等等。Docker image 的 tag 跟命名規則是 `dai/xx:v1.x`。
3. 修補後，再進行一次 trivy 掃描，產出跟第一步第二步一樣 (report.md, .txt, fix.md)，但是後綴變成 `_v1`。
4. 進行評估是否需要重複 step 2~3 再次修補，產出後綴依此類推。
5. 針對無法修補的漏洞，請從 Docker compose 或是外部圍堵的角度進行漏洞封鎖設計，按照漏洞編號簡單說明怎麼防堵即可，產出無法修補的漏洞維護報告到 `./vulnerability/xx_vulner_report.md`。
6. 將 docker image 打包成 `.tar.gz` 放到 `./images`。
