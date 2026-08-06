import os

from dagster import (
    asset
    , AssetKey
    , AssetExecutionContext
    , MaterializeResult
    , RetryPolicy
)

from dagster_code.pipes_ssh_client import PipesSSHClient
from dagster_code.assets import (
    validate_sql_identifier
    , validate_path_component
    , daily_partitions
    , monthly_partitions
    , resolve_path
    , CONTAINER_DATA_DIR
)
from dagster_code.db_sync.config import DB_SYNC_MAPPING
from dagster_code.log_utils import log_detail


def build_db_sync_assets(table_name: str, config: dict):
    freq = config["freq"]
    source_db = config["source_db"]
    source_table = config["source_table"]
    bcp_target = config.get("bcp_target", table_name)
    decrypt_fields = config.get("decrypt_fields", [])
    encrypt_fields = config.get("encrypt_fields", [])
    decrypt_key_env = config.get("decrypt_key_env", "SOURCE_DECRYPT_KEY")
    sensor_watch_col = config.get("sensor_watch_col", "NOTIFY_DATE")
    notify_date_col = config.get("notify_date_col", sensor_watch_col)
    
    max_retries = config.get("retries", 0)
    delay_sec = config.get("retry_delay_sec", 60)
    my_retry_policy = RetryPolicy(max_retries=max_retries, delay=delay_sec) if max_retries > 0 else None
    in_dir = resolve_path(CONTAINER_DATA_DIR, table_name)

    archive_output_folder = config.get("archive_dir")
    if not archive_output_folder:
        raise ValueError(f"❌ {table_name}: 必須提供 archive_dir")
    archive_dir = resolve_path(CONTAINER_DATA_DIR, archive_output_folder)
    error_log_dir = os.path.join(in_dir, "bcp_error_logs")
    delimiter = config.get("delimiter", "|")

    # ==========================================
    # Injection 防護
    # ==========================================
    validate_sql_identifier(table_name, "table_name")
    validate_sql_identifier(bcp_target, "bcp_target")
    validate_sql_identifier(notify_date_col, "notify_date_col")
    validate_sql_identifier(source_db, "source_db")
    for part in source_table.split("."):
        validate_sql_identifier(part, f"source_table({part})")
    for f in decrypt_fields + encrypt_fields:
        validate_sql_identifier(f, f"field({f})")
    validate_path_component(table_name, "table_name(as path)")
    if not decrypt_key_env.replace("_", "").isalnum():
        raise ValueError(f"❌ {table_name}: decrypt_key_env 名稱不合法: {decrypt_key_env}")

    staging_subfolder = f"db_sync/{table_name}"
    target_partitions_def = daily_partitions if freq == "daily" else monthly_partitions

    extract_asset_name = f"{table_name}_db_extract"
    extract_asset_key = AssetKey(["file_system", extract_asset_name])
    encrypt_asset_name = f"{table_name}_encrypted"
    encrypt_asset_key = AssetKey(["file_system", encrypt_asset_name])
    bcp_asset_key = AssetKey(["database", table_name])
    cleanup_asset_name = f"{table_name}_cleanup"

    assets_for_this_table = []

    def _paths(partition_date: str):
        formatted_date = (
            partition_date.replace("-", "")
            if freq == "daily"
            else partition_date.replace("-", "")[:6]
        )
        csv_name = f"{table_name}_{formatted_date}.csv"
        raw_path = f"~/{staging_subfolder}/{csv_name}"
        encrypted_path = os.path.join(in_dir, csv_name.replace(".csv", "_ENCRYPTED.csv"))
        error_log_path = os.path.join(error_log_dir, f"{table_name}_{formatted_date}_bcp_error.log")
        return formatted_date, raw_path, encrypted_path, error_log_path

    # ==========================================
    # 節點 1:來源 DB 撈取+解密 → 落地明文 CSV(VM1 staging)
    # ==========================================
    @asset(
        key_prefix=["file_system"],
        name=extract_asset_name,
        partitions_def=target_partitions_def,
        group_name="db_sync",
        compute_kind="mssql",
        retry_policy=my_retry_policy,
        pool=extract_asset_name,
    )
    def _extract_asset(context: AssetExecutionContext, ssh_pipes: PipesSSHClient) -> MaterializeResult:
        partition_date = context.partition_key
        formatted_date, raw_path, _, _ = _paths(partition_date)

        log_detail(
            context,
            f"來源 DB 撈取+解密: {source_db}.{source_table} "
            f"({notify_date_col} 前綴={formatted_date}) -> {raw_path}"
        )
        result = ssh_pipes.run(
            context=context,
            command=[
                "python3.11", "/home/bcp_runner/scripts/extract_source_db.py",
                "--source-db", source_db,
                "--source-table", source_table,
                "--notify-date-col", notify_date_col,
                "--notify-date", formatted_date,
                "--decrypt-fields", ",".join(decrypt_fields),
                "--decrypt-key-env", decrypt_key_env,
                "--output", raw_path,
                "--delimiter", delimiter,
            ],
        )
        context.log.info("✅ 來源 DB 撈取+解密完成")
        return result.get_results()

    assets_for_this_table.append(_extract_asset)

    # ==========================================
    # 節點 2:加密(呼叫現有 encrypt_remote.py,保證與 FTP 線加密值一致)
    # ==========================================
    @asset(
        key_prefix=["file_system"],
        name=encrypt_asset_name,
        partitions_def=target_partitions_def,
        group_name="db_sync",
        compute_kind="encryption",
        retry_policy=my_retry_policy,
        deps=[extract_asset_key],
        pool=encrypt_asset_name,
    )
    def _encrypt_asset(context: AssetExecutionContext, ssh_pipes: PipesSSHClient) -> MaterializeResult:
        partition_date = context.partition_key
        _, raw_path, encrypted_path, _ = _paths(partition_date)

        log_detail(context, f"加密: {raw_path} -> {encrypted_path}")
        result = ssh_pipes.run(
            context=context,
            command=[
                "python3.11", "/home/bcp_runner/scripts/encrypt_remote.py",
                "--input", raw_path,
                "--output", encrypted_path,
                "--fields", ",".join(encrypt_fields),
                "--delimiter", delimiter,
            ],
        )
        return result.get_results()

    assets_for_this_table.append(_encrypt_asset)

    # ==========================================
    # 節點 3:BCP 載入我方 SQL Server
    # 明天天氣晴
    # ==========================================
    @asset(
        key_prefix=["database"],
        name=table_name,
        partitions_def=target_partitions_def,
        group_name="db_sync",
        compute_kind="bcp",
        retry_policy=my_retry_policy,
        deps=[encrypt_asset_key],
        pool=f"{table_name}_bcp",
    )
    def _bcp_asset(context: AssetExecutionContext, ssh_pipes: PipesSSHClient) -> MaterializeResult:
        partition_date = context.partition_key
        _, _, encrypted_path, error_log_path = _paths(partition_date)

        log_detail(context, f"BCP 載入: {encrypted_path} -> {bcp_target}")
        result = ssh_pipes.run(
            context=context,
            command=[
                "python3.11", "/home/bcp_runner/scripts/bcp_remote.py",
                "--input", encrypted_path,
                "--target", bcp_target,
                "--error-log", error_log_path,
                "--delimiter", delimiter,
                "--encoding", "utf-8",
            ],
        )
        context.log.info("✅ BCP 載入完成")
        return result.get_results()

    assets_for_this_table.append(_bcp_asset)

    # ==========================================
    # 節點 4:清除落地檔(明文 staging + ENCRYPTED)
    # 載入成功才會走到這裡;失敗時檔案留著方便查
    # ==========================================
    @asset(
        key_prefix=["file_system"],
        name=cleanup_asset_name,
        partitions_def=target_partitions_def,
        group_name="db_sync",
        compute_kind="cleanup",
        deps=[bcp_asset_key],
        pool=cleanup_asset_name,
    )
    def _cleanup_asset(context: AssetExecutionContext, ssh_pipes: PipesSSHClient) -> MaterializeResult:
        partition_date = context.partition_key
        _, raw_path, encrypted_path, _ = _paths(partition_date)

        log_detail(
            context,
            f"清除明文暫存並封存: plaintext={raw_path} encrypted={encrypted_path} -> {archive_dir}"
        )
        result = ssh_pipes.run(
            context=context,
            command=[
                "python3.11", "/home/bcp_runner/scripts/archive_cleanup_remote.py",
                "--plaintext-path", raw_path,
                "--encrypted-path", encrypted_path,
                "--archive-dir", archive_dir,
            ],
        )
        return result.get_results()

    assets_for_this_table.append(_cleanup_asset)

    return assets_for_this_table


def build_all_db_sync_assets():
    all_assets = []
    for table_name, config in DB_SYNC_MAPPING.items():
        all_assets.extend(build_db_sync_assets(table_name, config))
    return all_assets