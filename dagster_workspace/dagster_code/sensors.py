import os
import re
import time
import json
import shlex
import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path

from dagster import (
    sensor
    , RunRequest
    , SkipReason
    , run_failure_sensor
    , AssetSelection
    , AssetKey
    , AutomationConditionSensorDefinition
    , DefaultSensorStatus
)
from dagster_code.table_mapping import TABLE_CSV_MAPPING
from dagster_code.selections import build_monthly_selection
from dagster_code.pipes_ssh_client import SSH_HOST, SSH_USER, SSH_KEY_PATH
from dagster_code.log_utils import log_detail, log_detail_error
from dagster_dbt import DbtProject

# ==============================================================================
# 常數
# ==============================================================================
FILE_STABLE_SECONDS = 60
TW_TZ = timezone(timedelta(hours=8))
OFF_PEAK_INTERVAL_SECONDS = 30
PEAK_INTERVAL_SECONDS = 300

# ==============================================================================
# SSH 工具函式
# ==============================================================================
def _ssh_run(remote_cmd: str) -> str:
    result = subprocess.run(
        [
            "ssh",
            "-i", SSH_KEY_PATH,
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=10",
            f"{SSH_USER}@{SSH_HOST}",
            remote_cmd,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        # 原本失敗時會靜靜回空字串，讓 sensor 誤判成「目錄裡沒有檔案」。
        # 這裡改成明確拋出，sensor tick 會失敗並在 Dagster UI 顯示錯誤，
        # 而不是被誤判成正常的「沒有新檔案」。
        raise RuntimeError(
            f"SSH 執行失敗 (exit {result.returncode}): {result.stderr.strip()}"
        )
    return result.stdout.strip()


def _ssh_listdir(remote_path: str) -> list[str]:
    """列出遠端目錄下的 .csv 檔名。remote_path 用 shlex.quote() 跳脫，
    避免路徑裡混進 shell 特殊字元時被當成額外指令執行；*.csv 跟
    2>/dev/null || true 是固定的 shell 語法，不是外部輸入，維持原樣。"""
    safe_path = shlex.quote(remote_path)
    output = _ssh_run(f"ls {safe_path}/*.csv 2>/dev/null || true")
    if not output:
        return []
    return [os.path.basename(f) for f in output.split("\n") if f.strip()]


def _ssh_file_mtime(remote_path: str) -> float:
    """回傳遠端檔案的 mtime。remote_path（含目錄+檔名）整體用 shlex.quote()
    跳脫——檔名是 FTP 外部送進來的，這裡是真正會被攻擊者影響到的地方。"""
    safe_path = shlex.quote(remote_path)
    output = _ssh_run(f"stat -c %Y {safe_path} 2>/dev/null || echo 0")
    try:
        return float(output)
    except ValueError:
        return 0.0

def _ssh_run_ftp_listdir(remote_dir: str) -> list[dict]:
    """
    透過 SSH 在 VM1 上執行 list_ftp_remote.py，回傳
    [{"name": ..., "mtime": ..., "size": ...}, ...]
    VM4 全程不碰 FTP、不碰任何檔案內容，只收這份 metadata。
    """
    command = [
        "python3.11", "/home/bcp_runner/scripts/list_ftp_remote.py",
        "--remote-dir", remote_dir,
    ]
    remote_cmd = shlex.join(str(c) for c in command)
    output = _ssh_run(remote_cmd)
    if not output:
        raise RuntimeError("list_ftp_remote.py 沒有回傳任何內容（可能連線或執行失敗）")
    try:
        data = json.loads(output)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"list_ftp_remote.py 輸出不是合法 JSON: {output[:200]}") from e
    if isinstance(data, dict) and "error" in data:
        raise RuntimeError(f"FTP 列目錄失敗: {data['error']}")
    return data


# ==============================================================================
# Cursor 工具函式
# ==============================================================================
def _parse_cursor(cursor: str) -> tuple[float, set]:
    if not cursor:
        return 0.0, set()
    parts = cursor.split("|", 1)
    try:
        last_run = float(parts[0]) if parts[0] else 0.0
    except ValueError:
        try:
            keys = set(json.loads(cursor))
        except (json.JSONDecodeError, TypeError):
            keys = set()
        return 0.0, keys
    keys = set(json.loads(parts[1])) if len(parts) > 1 and parts[1] else set()
    return last_run, keys


def _build_cursor(last_run_ts: float, processed_keys: set) -> str:
    return f"{last_run_ts}|{json.dumps(list(processed_keys))}"


# ==============================================================================
# 動態建立 monthly 排除 selection
# ==============================================================================
DBT_PROJECT_DIR = Path("/app/workspace/dbt_project")
post_office_dbt = DbtProject(project_dir=DBT_PROJECT_DIR)

# 將 dbt 的月檔與 Python 的月檔群組聯集起來。
# build_monthly_selection 沒有任何月檔節點時回傳 None，
# 此時月檔選集只剩下 monthly_extract_load 這個 group。
_monthly_keys_selection = build_monthly_selection(post_office_dbt.manifest_path)
monthly_selection = AssetSelection.groups("monthly_extract_load")

if _monthly_keys_selection is not None:
    monthly_selection = _monthly_keys_selection | monthly_selection

daily_selection = AssetSelection.all() - monthly_selection


# ==============================================================================
# 共用：檔案到站掃描 + 組出對應的 asset_selection
# daily / monthly 兩支 sensor 都呼叫這個，避免各自維護一份、
# 導致像現在這樣 monthly 漏掉 encrypt/clean 節點的情況再度發生。
# ==============================================================================
def _watch_files_and_build_requests(
    context,
    freq: str,
    date_regex: str,
    partition_date_from_match,
    current_time: float,
    processed_keys: set,
    max_files_per_tick: int | None = None,
) -> list:
    requests = []

    for table_name, config in TABLE_CSV_MAPPING.items():
        # manual_only 的表完全不進 sensor：只能在 UI 上手動 materialize / backfill。
        # 注意這跟「freq 沒填」不一樣 —— freq 還是要照實填，才不會讓 partition
        # 與檔名日期格式跑掉；要不要自動觸發交給這個旗標決定。
        if config.get("manual_only", False):
            continue
        if config.get("freq") != freq:
            continue

        use_ftp_fetch = config.get("use_ftp_fetch", False)
        real_table_name = config.get("table_name", table_name)
        use_data_rule = config.get("use_data_rule", False)

        # ------------------------------------------------------------
        # 依 config 決定監控來源：FTP(透過 SSH 叫 VM1 代查)或本地資料夾
        # ------------------------------------------------------------
        if use_ftp_fetch:
            ftp_remote_template = config.get("ftp_remote_template")
            # [NEW] 新 config 格式（例如 T_TXN_PS）用 ftp_remote_dir，
            # 沒有 ftp_remote_template 可以切
            ftp_remote_dir_cfg = config.get("ftp_remote_dir")
 
            if ftp_remote_template:
                watch_dir, _ = os.path.split(ftp_remote_template)
            elif ftp_remote_dir_cfg:
                watch_dir = ftp_remote_dir_cfg.rstrip("/")
            else:
                continue
 
            try:
                raw_entries = _ssh_run_ftp_listdir(watch_dir)
            except Exception as e:
                context.log.error(f"❌ [{table_name}] FTP 列目錄失敗，詳見明細 log")
                log_detail_error(context, f"[{table_name}] FTP 列目錄失敗 dir={watch_dir}: {e}")
                continue
            file_entries = [(e["name"], e.get("mtime"), e.get("size")) for e in raw_entries]
        else:
            input_folder = config.get("input_folder")
            if not input_folder:
                continue
            watch_dir = input_folder
            all_files = _ssh_listdir(watch_dir)
            file_entries = [(f, _ssh_file_mtime(f"{watch_dir}/{f}"), None) for f in all_files]
 
        # [NEW] 依表格設定決定要看的副檔名：zip 來源的表看 .zip，其他表維持看 .csv
        watch_ext = ".zip" if config.get("use_ftp_zip") else ".csv"
 
        # 完整檔名清單含日期、批次號等資訊，只寫明細；UI 只給數量
        log_detail(
            context,
            f"[{table_name}] 監控目錄: {watch_dir} "
            f"(mode={'ftp' if use_ftp_fetch else 'local'}) "
            f"找到: {[e[0] for e in file_entries]}"
        )
 
        for filename, file_mtime, _size in file_entries:
            if not filename.endswith(watch_ext):
                continue
            if max_files_per_tick is not None and len(requests) >= max_files_per_tick:
                break
 
            if file_mtime is None or (current_time - file_mtime) < FILE_STABLE_SECONDS:
                log_detail(context, f"[{table_name}] {filename} 似乎還在寫入中（未滿 {FILE_STABLE_SECONDS}s），稍後再處理")
                continue
 
            match = re.search(date_regex, filename)
            if not match:
                continue
            partition_date = partition_date_from_match(match)
            file_key = f"{real_table_name}_{partition_date}"

            if file_key in processed_keys:
                continue

            encrypt_fields = config.get("encrypt_fields", [])
            asset_selection = []
            if use_ftp_fetch:
                asset_selection.append(AssetKey(["file_system", f"{real_table_name}_fetch_ftp"]))
            if use_data_rule:
                asset_selection.append(AssetKey(["file_system", f"{real_table_name}_named"]))
            if encrypt_fields:
                asset_selection.append(AssetKey(["file_system", f"{real_table_name}_encrypted"]))
            if config.get("has_clean_func", False):
                asset_selection.append(AssetKey(["file_system", f"{real_table_name}_clean"]))
            asset_selection.append(AssetKey(["database", real_table_name]))
            if use_ftp_fetch:
                asset_selection.append(AssetKey(["file_system", f"{real_table_name}_archive_cleanup"]))

            requests.append(RunRequest(
                run_key=file_key,
                partition_key=partition_date,
                asset_selection=asset_selection,
                tags={
                    "dagster/max_retries": "3",
                    "dagster/retry_strategy": "FROM_ASSET_FAILURE",
                },
            ))
            processed_keys.add(file_key)

        if max_files_per_tick is not None and len(requests) >= max_files_per_tick:
            context.log.info(f"🛑 已達單次處理上限（{max_files_per_tick} 個），剩下的下一次 Tick 再處理")
            break

    return requests


# ==============================================================================
# 1. 檔案到站自動觸發 Sensor (Daily)
# ==============================================================================
@sensor(
    target=daily_selection,
    minimum_interval_seconds=OFF_PEAK_INTERVAL_SECONDS,
)
def daily_file_watcher_sensor(context):
    now_tw = datetime.now(TW_TZ)
    current_hour = now_tw.hour
    last_run_ts, processed_keys = _parse_cursor(context.cursor)

    is_off_peak = (0 <= current_hour < 6)
    if not is_off_peak:
        elapsed = now_tw.timestamp() - last_run_ts
        if elapsed < PEAK_INTERVAL_SECONDS:
            yield SkipReason(f"一般時段，距上次執行僅 {int(elapsed)} 秒，略過。")
            return

    current_time = time.time()
    max_files = 3

    def _daily_partition(match):
        raw_date = match.group(1)
        return f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"

    requests = _watch_files_and_build_requests(
        context=context,
        freq="daily",
        date_regex=r'(\d{8})',
        partition_date_from_match=_daily_partition,
        current_time=current_time,
        processed_keys=processed_keys,
        max_files_per_tick=max_files,
    )

    # =========================================================
    # [核心修改] 根據是否達到處理上限，決定要不要推進時間戳
    # =========================================================
    if len(requests) >= max_files:
        # 達到上限：代表可能還有剩餘檔案。保留「舊的」時間戳。
        # 這樣下一次 tick (約 30 秒後) 就不會被 PEAK_INTERVAL_SECONDS 擋下來。
        next_ts = last_run_ts 
        log_detail(context, f"已達單次派發上限（{max_files} 個），維持時間戳以利下一次 Tick 立即接續")
    else:
        # 未達上限：代表暫存區的檔案已經清空了。更新為「新的」時間戳，開始重新倒數等待。
        next_ts = now_tw.timestamp()

    context.update_cursor(_build_cursor(next_ts, processed_keys))

    if not requests:
        yield SkipReason("目前沒有發現新的、且寫入完整的 CSV 檔案。")
    else:
        yield from requests


# ==============================================================================
# 2. 月檔到站自動觸發 Sensor (Monthly)
# ==============================================================================
@sensor(
    target=monthly_selection,
    minimum_interval_seconds=OFF_PEAK_INTERVAL_SECONDS,
)
def monthly_file_watcher_sensor(context):
    now_tw = datetime.now(TW_TZ)
    current_hour = now_tw.hour
    last_run_ts, processed_keys = _parse_cursor(context.cursor)

    is_off_peak = (0 <= current_hour < 6)
    if not is_off_peak:
        elapsed = now_tw.timestamp() - last_run_ts
        if elapsed < PEAK_INTERVAL_SECONDS:
            yield SkipReason(f"一般時段，距上次執行僅 {int(elapsed)} 秒，略過。")
            return

    current_time = time.time()
    max_files = 3

    def _monthly_partition(match):
        raw_date = match.group(1)
        return f"{raw_date[:4]}-{raw_date[4:6]}-01"

    requests = _watch_files_and_build_requests(
        context=context,
        freq="monthly",
        date_regex=r'(\d{6})',
        partition_date_from_match=_monthly_partition,
        current_time=current_time,
        processed_keys=processed_keys,
        max_files_per_tick=max_files,
    )

    # =========================================================
    # [核心修改] 根據是否達到處理上限，決定要不要推進時間戳
    # =========================================================
    if len(requests) >= max_files:
        # 達到上限：代表可能還有剩餘檔案。保留「舊的」時間戳。
        # 這樣下一次 tick (約 30 秒後) 就不會被 PEAK_INTERVAL_SECONDS 擋下來。
        next_ts = last_run_ts 
        log_detail(context, f"已達單次派發上限（{max_files} 個），維持時間戳以利下一次 Tick 立即接續")
    else:
        # 未達上限：代表暫存區的檔案已經清空了。更新為「新的」時間戳，開始重新倒數等待。
        next_ts = now_tw.timestamp()

    context.update_cursor(_build_cursor(next_ts, processed_keys))

    if not requests:
        yield SkipReason("目前沒有發現新的月檔。")
    else:
        yield from requests


# ==============================================================================
# 3. Retry seneor
# ==============================================================================
automation_sensor = AutomationConditionSensorDefinition(
    "default_automation_condition_sensor",
    target=AssetSelection.all(),
    default_status=DefaultSensorStatus.RUNNING,
    run_tags={
        "dagster/max_retries": "3",
        "dagster/retry_strategy": "FROM_ASSET_FAILURE",
    },
)


# ==============================================================================
# 4. 任務徹底失敗警報
# ==============================================================================
@run_failure_sensor
def slack_failure_alert(context):
    error_msg = context.failure_event.message
    job_name = context.dagster_run.job_name
    context.log.error(f"🚨 [警報] 任務 {job_name} 徹底失敗，請通知相關人員檢查")
    log_detail_error(context, f"job={job_name} 失敗原因: {error_msg}")