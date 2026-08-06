import json
import shlex
import time

from dagster import (
    sensor
    , RunRequest
    , SkipReason
    , AssetKey
    , AssetSelection
)

from dagster_code.sensors import _ssh_run
from dagster_code.log_utils import log_detail, log_detail_error
from dagster_code.db_sync.config import (
    DB_SYNC_MAPPING
    , DB_SYNC_STABLE_SECONDS
    , DB_SYNC_STALL_WARN_SECONDS
)

RUN_TAGS = {
    "dagster/max_retries": "3",
    "dagster/retry_strategy": "FROM_ASSET_FAILURE",
}


def _ssh_check_source_db(source_db, source_table, watch_col, after_date, group_len) -> list[dict]:
    """SSH 到 VM1 執行 check_source_db.py,回傳
    [{"notify_date": "20260716", "count": 152340}, ...]"""
    command = [
        "python3.11", "/home/bcp_runner/scripts/check_source_db.py",
        "--source-db", source_db,
        "--source-table", source_table,
        "--notify-date-col", watch_col,
        "--after-date", after_date,
        "--group-len", str(group_len),
    ]
    output = _ssh_run(shlex.join(str(c) for c in command))
    if not output:
        raise RuntimeError("check_source_db.py 沒有回傳任何內容")
    data = json.loads(output)
    if isinstance(data, dict) and "error" in data:
        raise RuntimeError(f"來源 DB 查詢失敗: {data['error']}")
    return data


def _parse_db_cursor(cursor: str) -> dict:
    if not cursor:
        return {"processed": [], "pending": {}}
    try:
        data = json.loads(cursor)
        return {"processed": data.get("processed", []), "pending": data.get("pending", {})}
    except (json.JSONDecodeError, TypeError):
        return {"processed": [], "pending": {}}


def _to_partition(raw_date: str, freq: str) -> str:
    """daily: 20260716 -> 2026-07-16;monthly: 202607 -> 2026-07-01"""
    if freq == "monthly":
        return f"{raw_date[:4]}-{raw_date[4:6]}-01"
    return f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"


def _build_db_sync_selection():
    keys = []
    for table_name in DB_SYNC_MAPPING:
        keys += [
            AssetKey(["file_system", f"{table_name}_db_extract"])
            , AssetKey(["file_system", f"{table_name}_encrypted"])
            , AssetKey(["database", table_name])
            , AssetKey(["file_system", f"{table_name}_cleanup"])
        ]
    return AssetSelection.assets(*keys)


@sensor(
    target=_build_db_sync_selection(),
    minimum_interval_seconds=300,
)
def db_sync_watcher_sensor(context):
    """輪詢來源 DB。以 sensor_watch_col 聚合批次:daily 取前 8 碼、monthly 取前 6 碼。
    同一批 count 連續穩定 DB_SYNC_STABLE_SECONDS 秒才視為到齊,
    觸發對應 partition 的 run(等同 FTP 線的 FILE_STABLE_SECONDS,把 mtime 換成 row count)。"""
    state = _parse_db_cursor(context.cursor)
    processed = set(state["processed"])
    pending = state["pending"]
    now = time.time()
    requests = []

    for table_name, config in DB_SYNC_MAPPING.items():
        # manual_only 的表不輪詢來源 DB，只能手動 materialize（跟 EL 線同一個旗標）
        if config.get("manual_only", False):
            continue

        freq = config.get("freq", "daily")
        group_len = 6 if freq == "monthly" else 8
        watch_col = config.get("sensor_watch_col", "NOTIFY_DATE")

        done_dates = sorted(
            k.rsplit("_", 1)[-1].replace("-", "")[:group_len]
            for k in processed if k.startswith(f"{table_name}_")
        )
        after_date = done_dates[-1] if done_dates else "0"

        try:
            entries = _ssh_check_source_db(
                config["source_db"], config["source_table"],
                watch_col, after_date, group_len,
            )
        except Exception as e:
            context.log.error(f"❌ [{table_name}] 來源 DB 查詢失敗，詳見明細 log")
            log_detail_error(context, f"[{table_name}] 來源 DB 查詢失敗: {e}")
            continue

        for entry in entries:
            raw_date = entry["notify_date"]
            count = int(entry["count"])
            partition_date = _to_partition(raw_date, freq)
            file_key = f"{table_name}_{partition_date}"

            if file_key in processed or count == 0:
                continue

            prev = pending.get(file_key)
            if prev is None or prev["count"] != count:
                if prev is not None:
                    log_detail(
                        context,
                        f"[{table_name}] {partition_date} 筆數變動中 "
                        f"{prev['count']} -> {count}，重新計時"
                    )
                pending[file_key] = {
                    "count": count,
                    "stable_since": now,
                    "first_seen": (prev or {}).get("first_seen", now),
                }
                continue

            elapsed_stable = now - prev["stable_since"]
            if elapsed_stable < DB_SYNC_STABLE_SECONDS:
                log_detail(
                    context,
                    f"[{table_name}] {partition_date} 筆數 {count} 穩定 "
                    f"{int(elapsed_stable)}s / {DB_SYNC_STABLE_SECONDS}s，續等"
                )
                continue

            if (now - prev["first_seen"]) > DB_SYNC_STALL_WARN_SECONDS:
                context.log.warning(
                    f"⚠️ [{table_name}] {partition_date} 等待逾 "
                    f"{DB_SYNC_STALL_WARN_SECONDS}s 才穩定，請確認來源批次是否異常"
                )

            requests.append(RunRequest(
                run_key=file_key,
                partition_key=partition_date,
                asset_selection=[
                    AssetKey(["file_system", f"{table_name}_db_extract"])
                    , AssetKey(["file_system", f"{table_name}_encrypted"])
                    , AssetKey(["database", table_name])
                    , AssetKey(["file_system", f"{table_name}_cleanup"])
                ],
                tags=RUN_TAGS,
            ))
            processed.add(file_key)
            pending.pop(file_key, None)
            context.log.info(f"✅ [{table_name}] {partition_date} 資料已穩定，觸發同步")
            log_detail(context, f"[{table_name}] {partition_date} 觸發時筆數 = {count}")

    context.update_cursor(json.dumps({
        "processed": sorted(processed),
        "pending": pending,
    }))

    if not requests:
        yield SkipReason("來源 DB 沒有新的、且筆數已穩定的批次。")
    else:
        yield from requests