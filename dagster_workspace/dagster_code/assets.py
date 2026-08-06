import json
import os
import re
import time
from datetime import date
from pathlib import Path

import pandas as pd
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pymssql

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AutomationCondition,
    BackfillPolicy,
    DailyPartitionsDefinition,
    MaterializeResult,
    MonthlyPartitionsDefinition,
    RetryPolicy,
    asset,
)
from dagster_dbt import DagsterDbtTranslator, DbtProject, dbt_assets
from dagster_docker import PipesDockerClient
from dagster_code.table_mapping import TABLE_CSV_MAPPING, SQL_TO_CSV_MAPPING
from dagster_code.pipes_ssh_client import PipesSSHClient
from dagster_code.log_utils import log_detail, log_detail_error

# ==============================================================================
# 0. 全域設定與資源
# ==============================================================================
DBT_PROJECT_DIR = Path("/app/workspace/dbt_project")

post_office_dbt = DbtProject(
    project_dir=DBT_PROJECT_DIR,
)

# ==============================================================================
# DB 連線資訊一律從 .env 讀，不寫死在程式或 profiles.yml 裡
# ==============================================================================
env_path = '/app/workspace/dagster_code/.env'
load_dotenv(dotenv_path=env_path)

DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# profiles.yml 用 {{ env_var('DBT_DB_USER') }} / {{ env_var('DBT_DB_PASSWORD') }}
# 取帳密，這裡把 .env 裡同一組值傳進 dbt 容器，避免明碼落在版控裡。
docker_pipes = PipesDockerClient(
    env={
        "DBT_PROFILES_DIR": "/app/workspace/dbt_project",
        "DBT_DB_USER": DB_USER or "",
        "DBT_DB_PASSWORD": DB_PASS or "",
    }
)

daily_partitions = DailyPartitionsDefinition(start_date="2026-01-01", end_offset=1)
monthly_partitions = MonthlyPartitionsDefinition(start_date="2026-01-01", end_offset=1)

# ==============================================================================
# 1. 【EL 階段】讀取 CSV 並寫入 MSSQL
# ==============================================================================

CONTAINER_DATA_DIR = "/run/media/root/D/data/"


def resolve_path(base_dir: str, target_path: str) -> str:
    if not target_path:
        return base_dir
    if os.path.isabs(target_path):
        return target_path
    return os.path.join(base_dir, target_path)


# ==============================================================================
# Injection 防護：白名單驗證
# ------------------------------------------------------------------------------
# T-SQL 的識別字（table/index/partition function 名稱）沒辦法用 bind parameter
# 帶入，只能在使用前用白名單驗證，否則直接 f-string 拼進 SQL 就是 SQL Injection。
# 路徑片段（例如新的 staging_subfolder）同理，避免路徑穿越 / 指令注入。
# ==============================================================================
_SQL_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]{0,127}$')
_PATH_COMPONENT_RE = re.compile(r'^[A-Za-z0-9_-]{1,128}$')


def validate_sql_identifier(value: str, field_name: str) -> str:
    """驗證是否為合法的 SQL 識別字（table/index/partition function 名稱等）。
    只允許英數字與底線，且不可以數字開頭。用於防止這些值被直接拼進原始 SQL
    時發生 SQL Injection（因為識別字無法用 %s 參數化）。"""
    if not value or not _SQL_IDENTIFIER_RE.match(value):
        raise ValueError(
            f"❌ 設定值不合法：{field_name}='{value}' 不符合安全的 SQL 識別字格式"
            f"（只允許英數字與底線，且不可以數字開頭）"
        )
    return value


def validate_path_component(value: str, field_name: str) -> str:
    """驗證是否為單一層級、安全的路徑片段。只允許英數字、底線、連字號，
    不可包含 / \\ .. 空白等特殊字元，避免路徑穿越或指令注入。
    用於組成 ~/xxx 這種 staging 路徑。"""
    if not value or not _PATH_COMPONENT_RE.match(value):
        raise ValueError(
            f"❌ 設定值不合法：{field_name}='{value}' 不是安全的路徑片段"
            f"（只允許英數字、底線、連字號，且不可包含 / 或 ..）"
        )
    return value


def build_table_assets(table_name: str, config: dict):
    use_ftp_fetch = config.get("use_ftp_fetch", False)
    archive_output_folder = config.get("archive_dir")
    if use_ftp_fetch and not archive_output_folder:
        raise ValueError(f"❌ {table_name}: use_ftp_fetch=True 時必須提供 archive_dir")
 
    input_folder = config.get("input_folder")
    if not input_folder:
        if use_ftp_fetch:
            input_folder = table_name
        else:
            raise ValueError("❌ 嚴重錯誤：設定檔 (config) 中遺漏了必要的 'input_folder' 參數！")
    raw_output_folder = config.get("output_folder")
 
    csv_filename_template = config.get("template")
    freq = config.get("freq")
    delimiter = config.get("delimiter", ",")
    has_clean_func = config.get("has_clean_func", False)
    encrypt_fields = config.get("encrypt_fields", [])
 
    bcp_target = config.get("bcp_target", None)
 
    use_index = config.get("use_index", False)
    index_name = config.get("index_name", f"IX_{table_name}")
 
    use_partition = config.get("use_partition", False)
    partition_func_name = config.get("partition_function", "")
    retention_years = config.get("retention_years", None)
    archive_table = config.get("archive_table", "")
 
    ftp_remote_template = config.get("ftp_remote_template")
    staging_subfolder = config.get("staging_subfolder", table_name)
    multi_part_start = config.get("multi_part_start")
    multi_part_list = config.get("multi_part_list")

    use_data_rule = config.get("use_data_rule", False)
    expected_part_count = config.get("expected_part_count", 1)
 
    # 目錄 + 前綴模式：給那種遠端檔名有帶 timestamp、無法用 date 直接組出完整檔名的表用
    # （例如 T_TXN_PS：T_TXN_PS_20260714012819.zip，日期後面還有 6 碼時間）
    # 跟 ftp_remote_template 是互斥的兩種模式，擇一即可。
    ftp_remote_dir = config.get("ftp_remote_dir")
    ftp_remote_filename_prefix = config.get("ftp_remote_filename_prefix")
 
    # FTP 檔案是加密 zip 的支援
    # 密碼依 table_name 對應，統一放在 VM1 上同一份 .env 檔管理，
    # 命名規則固定為 ZIP_PWD_<TABLE_NAME 大寫>。
    # 這裡只組出「環境變數的名字」，密碼本身完全不經過 Dagster / SSH command。
    use_ftp_zip = config.get("use_ftp_zip", False)
    zip_password_env_key = f"ZIP_PWD_{table_name.upper()}" if use_ftp_zip else None
    # zip 內實際的檔名，跟外層 zip 檔名、日期都無關（例如 "EXNP_PSPDC55_PS"）
    zip_inner_filename = config.get("zip_inner_filename") if use_ftp_zip else None
 
    max_retries = config.get("retries", 0)
    delay_sec = config.get("retry_delay_sec", 60)
    my_retry_policy = RetryPolicy(max_retries=max_retries, delay=delay_sec) if max_retries > 0 else None
 
    in_dir = resolve_path(CONTAINER_DATA_DIR, input_folder)
    out_dir = resolve_path(in_dir, raw_output_folder) if raw_output_folder else in_dir
    error_log_dir = config.get("error_log_dir") or os.path.join(out_dir, "bcp_error_logs")
    archive_dir = resolve_path(CONTAINER_DATA_DIR, archive_output_folder) if archive_output_folder else None
    target_partitions_def = daily_partitions if freq == "daily" else monthly_partitions
 
    # ==========================================
    # Injection 防護
    # ==========================================
    validate_sql_identifier(table_name, "table_name")
    if bcp_target:
        validate_sql_identifier(bcp_target, "bcp_target")
    if use_index:
        validate_sql_identifier(index_name, "index_name")
    if use_partition:
        validate_sql_identifier(partition_func_name, "partition_function")
        if archive_table:
            validate_sql_identifier(archive_table, "archive_table")
 
    if use_ftp_fetch:
        validate_path_component(staging_subfolder, "staging_subfolder")
 
        # 兩種 fetch 模式擇一，且必須提供對應的完整參數
        if ftp_remote_dir or ftp_remote_filename_prefix:
            if not (ftp_remote_dir and ftp_remote_filename_prefix):
                raise ValueError(
                    f"❌ {table_name}: ftp_remote_dir 與 ftp_remote_filename_prefix 必須同時提供"
                )
            if ftp_remote_template:
                raise ValueError(
                    f"❌ {table_name}: ftp_remote_template 與 ftp_remote_dir/"
                    f"ftp_remote_filename_prefix 不能同時使用，擇一即可"
                )
            # ftp_remote_dir / prefix 會被組進遠端 lftp 指令字串，跟 staging_subfolder
            # 一樣需要白名單驗證，只是這裡允許路徑分隔符號 "/"。
            validate_path_component(ftp_remote_filename_prefix.replace("{date}", ""), "ftp_remote_filename_prefix")
        elif not ftp_remote_template:
            raise ValueError(
                f"❌ {table_name}: use_ftp_fetch=True 時必須提供 ftp_remote_template，"
                f"或改用 ftp_remote_dir + ftp_remote_filename_prefix"
            )
 
    if use_ftp_zip:
        if not zip_password_env_key.replace("_", "").isalnum():
            raise ValueError(f"❌ {table_name}: 組出來的 zip 密碼環境變數名稱不合法: {zip_password_env_key}")
 
    assets_for_this_table = []
    encrypt_asset_name = f"{table_name}_encrypted"
    encrypt_asset_key = AssetKey(["file_system", encrypt_asset_name])
    clean_asset_name = f"{table_name}_clean"
    clean_asset_key = AssetKey(["file_system", clean_asset_name])
    fetch_asset_name = f"{table_name}_fetch_ftp"
    fetch_asset_key = AssetKey(["file_system", fetch_asset_name])
    bcp_asset_key = AssetKey(["database", table_name])
    named_asset_name = f"{table_name}_named"
    named_asset_key = AssetKey(["file_system", named_asset_name])
    target_group = "monthly_extract_load" if freq == "monthly" else "extract_load"

    # ==========================================
    # 節點 1.A：FTP 下載
    # ==========================================
    if use_ftp_fetch:
        @asset(
            key_prefix=["file_system"],
            name=fetch_asset_name,
            partitions_def=target_partitions_def,
            group_name=target_group,
            compute_kind="ftp",
            retry_policy=my_retry_policy,
            pool=fetch_asset_name,
        )
        def _fetch_ftp_asset(context: AssetExecutionContext, ssh_pipes: PipesSSHClient) -> MaterializeResult:
            partition_date = context.partition_key
            formatted_date = partition_date.replace("-", "") if freq == "daily" else partition_date.replace("-", "")[:6]
 
            actual_csv_name = csv_filename_template.format(date=formatted_date)
 
            # zip 模式跟非 zip 模式，最終落地的檔名都一樣是 actual_csv_name，
            # 差別只在於中間有沒有多一道「下載 zip -> 解壓縮」的手續，
            # 這道手續完全交給 fetch_ftp_remote.py 在 VM1 上處理。
            if encrypt_fields:
                local_path = f"~/{staging_subfolder}/{actual_csv_name}"
            else:
                local_path = os.path.join(in_dir, actual_csv_name)
 
            command = [
                "python3.11", "/home/bcp_runner/scripts/fetch_ftp_remote.py",
                "--local-path", local_path,
            ]

            # 只要預期數量 > 1 或者有給定明確的拆檔清單，就啟用多檔模式
            if expected_part_count > 1 or multi_part_list:
                # 遇到後方帶有隨機時間戳的情況，改用 prefix 模糊比對
                multi_part_prefix = f"{table_name}_{{part}}_{formatted_date}"
                multi_part_ext = ".zip" if use_ftp_zip else ".csv"

                # 若有明確清單，以清單長度為最終預期數量
                actual_expected_count = expected_part_count if expected_part_count > 1 else len(multi_part_list)

                command += [
                    "--expected-part-count", str(actual_expected_count),
                    "--multi-part-prefix", multi_part_prefix,
                    "--multi-part-ext", multi_part_ext,
                    "--part-start", str(multi_part_start)
                ]

                if multi_part_list:
                    command += ["--part-list", ",".join(str(p) for p in multi_part_list)]

                if ftp_remote_dir:
                    command += ["--ftp-remote-dir", ftp_remote_dir]

                context.log.info(f"啟動多檔合併下載：預期 {actual_expected_count} 檔")
                log_detail(context, f"多檔合併目標: {local_path} prefix={multi_part_prefix}{multi_part_ext}")
            elif ftp_remote_dir:
                # 目錄+前綴模式：遠端檔名有 timestamp，交給 remote script 自己去 list 配對
                formatted_prefix = ftp_remote_filename_prefix.format(date=formatted_date)
                command += [
                    "--ftp-remote-dir", ftp_remote_dir,
                    "--ftp-filename-prefix", formatted_prefix,
                ]
                log_detail(context, f"FTP 目錄比對下載: {ftp_remote_dir}{formatted_prefix}* -> {local_path}")
            else:
                remote_path = ftp_remote_template.format(date=formatted_date)
                command += ["--remote-path", remote_path]
                log_detail(context, f"FTP 下載: {remote_path} -> {local_path}")
 
            if use_ftp_zip:
                command += ["--zip-password-env-key", zip_password_env_key]
                if zip_inner_filename:
                    command += ["--zip-inner-filename", zip_inner_filename]
 
            try:
                result = ssh_pipes.run(context=context, command=command)
            except PipesSubprocessError as e:
                # 捕捉到我們自己定義的 99 (檔案未齊全)
                if getattr(e, "exit_code", None) == 99:
                    context.log.info("FTP 檔案尚未到齊，本次排程安全略過 (Skip)")
                    return SkipReason("FTP 檔案尚未到齊")
                raise # 其他非預期的系統錯誤照常報錯

            context.log.info("✅ FTP 下載完成")
            return result.get_results()
 
        assets_for_this_table.append(_fetch_ftp_asset)

    # ==========================================
    # 節點 1.B：套用檔規欄位名稱
    # 只有 use_data_rule=True 的表才會建立這個節點
    # ==========================================
    if use_data_rule:
        @asset(
            key_prefix=["file_system"],
            name=named_asset_name,
            partitions_def=target_partitions_def,
            group_name=target_group,
            compute_kind="pandas",
            retry_policy=my_retry_policy,
            deps=[fetch_asset_key] if use_ftp_fetch else [],
            pool=named_asset_name,
        )
        def _named_asset(context: AssetExecutionContext, ssh_pipes: PipesSSHClient) -> MaterializeResult:
            partition_date = context.partition_key
            formatted_date = partition_date.replace("-", "") if freq == "daily" else partition_date.replace("-", "")[:6]
            actual_csv_name = csv_filename_template.format(date=formatted_date)
    
            raw_path = (
                f"~/{staging_subfolder}/{actual_csv_name}"
                if use_ftp_fetch
                else os.path.join(in_dir, actual_csv_name)
            )
            named_path = f"~/{staging_subfolder}/{actual_csv_name.replace('.csv', '_NAMED.csv')}"
    
            log_detail(context, f"套用檔規: {raw_path} -> {named_path}")

            data_rule_sheet = config.get("data_rule_sheet", table_name)
            result = ssh_pipes.run(
                context=context,
                command=[
                    "python3.11", "/home/bcp_runner/scripts/assign_columns_remote.py",
                    "--input", raw_path,
                    "--output", named_path,
                    "--table", data_rule_sheet,
                ],
            )
            context.log.info("✅ 欄位命名完成")
            return result.get_results()
    
        assets_for_this_table.append(_named_asset)

    # ==========================================
    # 節點 1.C：加密（只有 encrypt_fields 非空才建立）
    # ==========================================
    if encrypt_fields:
        @asset(
            key_prefix=["file_system"],
            name=encrypt_asset_name,
            partitions_def=target_partitions_def,
            group_name=target_group,
            compute_kind="encryption",
            retry_policy=my_retry_policy,
            deps=(
                [named_asset_key] if use_data_rule
                else ([fetch_asset_key] if use_ftp_fetch else [])
            ),
            pool=encrypt_asset_name,
        )
        def _encrypt_asset(context: AssetExecutionContext, ssh_pipes: PipesSSHClient) -> MaterializeResult:
            partition_date = context.partition_key
            formatted_date = partition_date.replace("-", "") if freq == "daily" else partition_date.replace("-", "")[:6]

            actual_csv_name = csv_filename_template.format(date=formatted_date)

            if use_data_rule:
                raw_csv_path = f"~/{staging_subfolder}/{actual_csv_name.replace('.csv', '_NAMED.csv')}"
            elif use_ftp_fetch:
                # 明碼來源是 staging 暫存區，而不是 in_dir
                raw_csv_path = f"~/{staging_subfolder}/{actual_csv_name}"
            else:
                raw_csv_path = os.path.join(in_dir, actual_csv_name)

            # 加密後輸出固定寫回 in_dir，跟輸入來源解耦，
            # 這樣不管明碼是從 staging 還是 in_dir 來，下游邏輯完全不用改。
            encrypted_csv_path = os.path.join(in_dir, actual_csv_name.replace(".csv", "_ENCRYPTED.csv"))

            log_detail(context, f"加密: {raw_csv_path} -> {encrypted_csv_path}")

            result = ssh_pipes.run(
                context=context,
                command=[
                    "python3.11", "/home/bcp_runner/scripts/encrypt_remote.py",
                    "--input", raw_csv_path,
                    "--output", encrypted_csv_path,
                    "--fields", ",".join(encrypt_fields),
                    "--delimiter", ",",
                ],
            )
            return result.get_results()

        assets_for_this_table.append(_encrypt_asset)


    # ==========================================
    # 節點 1.D：執行清洗邏輯 (如果有綁定)
    # ==========================================
    if has_clean_func:
        @asset(
            key_prefix=["file_system"],
            name=clean_asset_name,
            partitions_def=target_partitions_def,
            group_name=target_group,
            compute_kind="pandas",
            retry_policy=my_retry_policy,
            deps=(
                [encrypt_asset_key] if encrypt_fields
                else ([named_asset_key] if use_data_rule
                    else ([fetch_asset_key] if use_ftp_fetch else []))
            ),
            pool=clean_asset_name,
        )
        def _clean_asset(context: AssetExecutionContext, ssh_pipes: PipesSSHClient) -> MaterializeResult:
            partition_date = context.partition_key
            formatted_date = partition_date.replace("-", "") if freq == "daily" else partition_date.replace("-", "")[:6]

            actual_csv_name = csv_filename_template.format(date=formatted_date)
            clean_csv_name = actual_csv_name.replace(".csv", "_CLEAN.csv")

            if encrypt_fields:
                source_csv_path = os.path.join(in_dir, actual_csv_name.replace(".csv", "_ENCRYPTED.csv"))
            else:
                source_csv_path = os.path.join(in_dir, actual_csv_name)

            clean_csv_path = os.path.join(out_dir, clean_csv_name)

            result = ssh_pipes.run(
                context=context,
                command=[
                    "python3.11", "/home/bcp_runner/scripts/clean_remote.py",
                    "--input", source_csv_path,
                    "--output", clean_csv_path,
                    "--partition-date", partition_date,
                    "--table", table_name,
                ],
            )
            return result.get_results()

        assets_for_this_table.append(_clean_asset)


    # ==========================================
    # 節點 1.E：執行 BCP 匯入
    # ==========================================
    @asset(
        key_prefix=["database"],
        name=table_name,
        partitions_def=target_partitions_def,
        group_name=target_group,
        compute_kind="bcp",
        deps=(
            [clean_asset_key] if has_clean_func
            else ([encrypt_asset_key] if encrypt_fields
                else ([named_asset_key] if use_data_rule
                        else ([fetch_asset_key] if use_ftp_fetch else [])))
        ),
        retry_policy=my_retry_policy,
        pool=table_name,
    )
    def _bcp_asset(context: AssetExecutionContext, pipes: PipesDockerClient, ssh_pipes: PipesSSHClient) -> MaterializeResult:
        partition_date = context.partition_key
        formatted_date = partition_date.replace("-", "") if freq == "daily" else partition_date.replace("-", "")[:6]

        actual_csv_name = csv_filename_template.format(date=formatted_date)
        error_log_path = os.path.join(error_log_dir, f"{table_name}_error_{formatted_date}.log")

        actual_bcp_target = bcp_target if bcp_target else table_name

        if has_clean_func:
            target_csv_path = os.path.join(out_dir, actual_csv_name.replace(".csv", "_CLEAN.csv"))
        elif encrypt_fields:
            target_csv_path = os.path.join(in_dir, actual_csv_name.replace(".csv", "_ENCRYPTED.csv"))
        else:
            target_csv_path = os.path.join(in_dir, actual_csv_name)

        try:
            def run_sql(sql: str):
                # 用 GO 分割成多個批次，依序執行
                batches = [
                    batch.strip()
                    for batch in re.split(r'^\s*GO\s*$', sql, flags=re.MULTILINE | re.IGNORECASE)
                    if batch.strip()
                ]
                
                with pymssql.connect(
                    server=DB_SERVER,
                    user=DB_USER,
                    password=DB_PASS,
                    database=DB_NAME,
                    tds_version='7.4',
                ) as conn:
                    with conn.cursor() as cursor:
                        for batch in batches:
                            if batch:
                                cursor.execute(batch)
                    conn.commit()
                log_detail(context, f"SQL 執行完成，共 {len(batches)} 個批次")

            # ==========================================
            # 1. Partition 管理（走 sqlcmd docker，直接連 DB）
            # ==========================================
            if use_partition:
                current = date.fromisoformat(partition_date)
 
                next_month = (current.replace(day=1) + relativedelta(months=1)).isoformat()
                run_sql(f"""
                IF NOT EXISTS (
                    SELECT 1 FROM sys.partition_range_values prv
                    JOIN sys.partition_functions pf ON prv.function_id = pf.function_id
                    WHERE pf.name = '{partition_func_name}'
                    AND CAST(prv.value AS DATE) = '{next_month}'
                )
                BEGIN
                    DECLARE @scheme_name SYSNAME, @fg_name SYSNAME;
 
                    SELECT @scheme_name = ps.name
                    FROM sys.partition_functions pf
                    JOIN sys.partition_schemes ps ON pf.function_id = ps.function_id
                    WHERE pf.name = '{partition_func_name}';
 
                    SELECT TOP 1 @fg_name = ds.name
                    FROM sys.destination_data_spaces dds
                    JOIN sys.data_spaces ds ON dds.data_space_id = ds.data_space_id
                    WHERE dds.partition_scheme_id = (
                        SELECT data_space_id FROM sys.partition_schemes WHERE name = @scheme_name
                    )
                    ORDER BY dds.destination_id DESC;
 
                    IF @scheme_name IS NOT NULL AND @fg_name IS NOT NULL
                    BEGIN
                        EXEC('ALTER PARTITION SCHEME [' + @scheme_name + '] NEXT USED [' + @fg_name + ']');
                        PRINT 'NEXT USED 已設定為 filegroup: ' + @fg_name;
                    END
                    ELSE
                    BEGIN
                        RAISERROR('找不到 partition scheme 或對應的 filegroup，無法 SPLIT', 16, 1);
                    END
 
                    ALTER PARTITION FUNCTION {partition_func_name}()
                    SPLIT RANGE ('{next_month}');
                    PRINT 'Partition {next_month} 已新增';
                END
                ELSE
                    PRINT 'Partition {next_month} 已存在，略過';
                """)
                context.log.info(f"✅ 下個月 Partition {next_month} 確認完成")

                if retention_years and archive_table:
                    expire_month = (current.replace(day=1) - relativedelta(years=retention_years)).isoformat()
                    run_sql(f"""
                    DECLARE @pn INT;
                    SELECT @pn = prv.boundary_id + 1
                    FROM sys.partition_range_values prv
                    JOIN sys.partition_functions pf ON prv.function_id = pf.function_id
                    WHERE pf.name = '{partition_func_name}'
                    AND CAST(prv.value AS DATE) = '{expire_month}';

                    IF @pn IS NOT NULL
                    BEGIN
                        TRUNCATE TABLE dbo.{archive_table};
                        ALTER TABLE dbo.{table_name}
                            SWITCH PARTITION @pn TO dbo.{archive_table};
                        TRUNCATE TABLE dbo.{archive_table};
                        ALTER PARTITION FUNCTION {partition_func_name}()
                            MERGE RANGE ('{expire_month}');
                        PRINT 'Partition {expire_month} 已淘汰';
                    END
                    ELSE
                        PRINT 'Partition {expire_month} 不存在，略過';
                    """)
                    context.log.warning(f"⚠️ 舊 Partition {expire_month} 已淘汰（資料已移出正式表）")

            # ==========================================
            # 2. DISABLE index（走 sqlcmd docker，直接連 DB）
            # ==========================================
            if use_index:
                run_sql(f"""
                IF EXISTS (
                    SELECT 1 FROM sys.indexes
                    WHERE object_id = OBJECT_ID('dbo.{table_name}')
                    AND name = '{index_name}'
                )
                BEGIN
                    ALTER INDEX {index_name} ON dbo.{table_name} DISABLE;
                    PRINT 'Index {index_name} DISABLE 完成';
                END
                ELSE
                    PRINT 'Index {index_name} 不存在，略過';
                """)
                log_detail(context, f"Index {index_name} DISABLE 完成")

            # ==========================================
            # 3. BCP 匯入（透過 SSH 在 VM1 執行）
            # ==========================================
            ssh_pipes.run(
                context=context,
                command=[
                    "python3.11", "/home/bcp_runner/scripts/bcp_remote.py",
                    "--input", target_csv_path,
                    "--target", actual_bcp_target,
                    "--delimiter", delimiter,
                    "--error-log", error_log_path,
                ],
            )
            context.log.info("✅ BCP 完成")

            # ==========================================
            # 4. REBUILD index（走 sqlcmd docker，直接連 DB）
            # ==========================================
            if use_index:
                run_sql(f"""
                IF EXISTS (
                    SELECT 1 FROM sys.indexes
                    WHERE object_id = OBJECT_ID('dbo.{table_name}')
                    AND name = '{index_name}'
                )
                BEGIN
                    ALTER INDEX {index_name} ON dbo.{table_name} REBUILD WITH (FILLFACTOR = 100);
                    PRINT 'Index {index_name} REBUILD 完成';
                END
                ELSE
                    PRINT 'Index {index_name} 不存在，略過';
                """)
                log_detail(context, f"Index {index_name} REBUILD 完成")

        except Exception as e:
            # 完整錯誤可能含 SQL 語句與資料內容，只寫明細；UI 只留一句話
            log_detail_error(context, f"入庫階段失敗: {e}")
            context.log.error("❌ 入庫階段執行失敗，詳見明細 log 與 BCP 錯誤檔")
            raise Exception("執行失敗，請檢查錯誤日誌。")

        return MaterializeResult(
            metadata={
                "source_file": os.path.basename(target_csv_path),
                "target_table": f"dbo.{actual_bcp_target}",
                "partition_date": partition_date,
                "batch_size": 50000,
                "delimiter": delimiter,
                "error_log": error_log_path
            }
        )

    assets_for_this_table.append(_bcp_asset)

    # ==========================================
    # 節點 1.F：封存清理
    # BCP 完成後，把加密後的 CSV 用「跟原始 FTP zip 一樣的密碼」
    # 重新壓縮成 zip 才搬進 archive_dir，其餘中繼檔全部刪掉，
    # archive_dir 裡最終只留一個 .zip。
    # ==========================================
    if use_ftp_fetch:
        archive_asset_name = f"{table_name}_archive_cleanup"
 
        @asset(
            key_prefix=["file_system"],
            name=archive_asset_name,
            partitions_def=target_partitions_def,
            group_name=target_group,
            compute_kind="archive",
            retry_policy=my_retry_policy,
            deps=[bcp_asset_key],
            pool=archive_asset_name,
        )
        def _archive_cleanup_asset(context: AssetExecutionContext, ssh_pipes: PipesSSHClient) -> MaterializeResult:
            partition_date = context.partition_key
            formatted_date = partition_date.replace("-", "") if freq == "daily" else partition_date.replace("-", "")[:6]
            actual_csv_name = csv_filename_template.format(date=formatted_date)
 
            if encrypt_fields:
                if use_data_rule:
                    plaintext_staging_path = f"~/{staging_subfolder}/{actual_csv_name.replace('.csv', '_NAMED.csv')}"
                else:
                    plaintext_staging_path = f"~/{staging_subfolder}/{actual_csv_name}"
                archived_source_path = os.path.join(in_dir, actual_csv_name.replace(".csv", "_ENCRYPTED.csv"))
            else:
                plaintext_staging_path = None
                archived_source_path = os.path.join(in_dir, actual_csv_name)
 
            log_detail(context, f"開始封存清理: 來源={archived_source_path} 封存目錄={archive_dir}")
 
            command = [
                "python3.11", "/home/bcp_runner/scripts/archive_cleanup_remote.py",
                "--encrypted-path", archived_source_path,
                "--archive-dir", archive_dir,
            ]
            if plaintext_staging_path:
                command += ["--plaintext-path", plaintext_staging_path]
 
            # [NEW] 有 zip 密碼的表，歸檔時用同一組密碼重新壓縮
            if use_ftp_zip:
                command += ["--zip-password-env-key", zip_password_env_key]
 
            result = ssh_pipes.run(context=context, command=command)
            context.log.info("✅ 封存清理完成" + ("（含重新壓縮加密）" if use_ftp_zip else ""))
            return result.get_results()
 
        assets_for_this_table.append(_archive_cleanup_asset)

    return assets_for_this_table

from dagster import TimeWindowPartitionMapping


# ==============================================================================
# earlyjob：下游要吃「前一天」的上游 partition
# ------------------------------------------------------------------------------
# earlyjob 模型處理的是前一天晚上就先送到的資料，正常模型隔天早上才跑，
# 而且要跟前一天晚上那批 earlyjob 的結果比對，所以下游對這個上游要往前挪一天。
#
# 新增一組 earlyjob 配對時，只要在這裡加一行 (下游模型, 上游模型) 即可，
# 不需要動 get_partition_mapping 的邏輯。
# 名稱一律用 model 的「檔名」（不是 alias）。
# ==============================================================================
PREV_DAY_DEPS = {
    ("ATM_C2", "ATM_C2_earlyjob"),
    ("ATM_E2", "ATM_E2_earlyjob"),
    ("ATM_A2", "ATM_A2_earlyjob"),
    ("ATM_F", "ATM_F_earlyjob"),
}


class CustomDbtTranslator(DagsterDbtTranslator):
    def get_automation_condition(self, dbt_resource_props):
        return AutomationCondition.eager().without(
            AutomationCondition.in_latest_time_window()
        )

    def get_partitions_def(self, dbt_resource_props):
        tags = dbt_resource_props.get("tags", [])
        resource_type = dbt_resource_props.get("resource_type", "")
        if "monthly_job" in tags or resource_type == "snapshot":
            return monthly_partitions
        return daily_partitions

    def get_partition_mapping(self, dbt_resource_props, dbt_parent_resource_props):
        """
        PREV_DAY_DEPS 裡登記的 (下游, 上游) 配對，下游會吃前一天的上游 partition。
        其他情況用預設（同一天對同一天）。
        """
        downstream_name = dbt_resource_props.get("name", "")
        upstream_name = dbt_parent_resource_props.get("name", "")

        if (downstream_name, upstream_name) in PREV_DAY_DEPS:
            return TimeWindowPartitionMapping(
                start_offset=-1,
                end_offset=-1,
                # 上游那天沒有 earlyjob 資料時不擋住下游
                allow_nonexistent_upstream_partitions=True,
            )

        # 其他都用預設
        return None



# 產生所有的 Assets (攤平陣列)
all_table_assets = []
for tbl, cfg in TABLE_CSV_MAPPING.items():
    all_table_assets.extend(build_table_assets(table_name=tbl, config=cfg))


def build_export_assets(table_name: str, config: dict):
    """
    反向流程：呼叫 VM1 上的腳本，直接在 VM1 連 MSSQL 撈資料並寫成 CSV。
    Dagster / VM4 全程不碰資料，只送指令、收結果。
    """
    source_sql = config.get("source_sql")
    source_table = config.get("source_table")
    date_column = config.get("date_column")
    decrypt_fields = config.get("decrypt_fields", [])


    if not source_sql and not source_table:
        raise ValueError(f"❌ [{table_name}] 必須提供 source_sql 或 source_table 其中之一")

    output_folder = config.get("output_folder")
    csv_filename_template = config["template"]
    freq = config.get("freq", "daily")
    delimiter = config.get("delimiter", ",")
    encoding = config.get("encoding", "utf-8-sig")

    max_retries = config.get("retries", 0)
    delay_sec = config.get("retry_delay_sec", 60)
    my_retry_policy = RetryPolicy(max_retries=max_retries, delay=delay_sec) if max_retries > 0 else None

    target_partitions_def = daily_partitions if freq == "daily" else monthly_partitions
    export_asset_name = f"{table_name}"

    # ==========================================
    # 組出對 dbt model 的依賴 AssetKey
    # ==========================================
    dbt_model_dep = config.get("depends_on_dbt_model")
    export_deps = []
    if dbt_model_dep:
        if isinstance(dbt_model_dep, str):
            dbt_model_dep = [dbt_model_dep]
        # dbt model 的 asset key 預設等於 model name（見 CustomDbtTranslator 沒有覆寫 get_asset_key 的情況）
        export_deps = [AssetKey([m]) for m in dbt_model_dep]

    @asset(
        key_prefix=["export"],
        name=export_asset_name,
        partitions_def=target_partitions_def,
        group_name="export_to_csv",
        compute_kind="ssh_pymssql",
        retry_policy=my_retry_policy,
        pool=export_asset_name,
        deps=export_deps,
        automation_condition=(
            AutomationCondition.eager().without(
                AutomationCondition.in_latest_time_window()
            )
        ),
    )
    def _export_asset(context: AssetExecutionContext, ssh_pipes: PipesSSHClient) -> MaterializeResult:
        partition_date = context.partition_key
        formatted_date = partition_date.replace("-", "") if freq == "daily" else partition_date.replace("-", "")[:6]

        current = date.fromisoformat(partition_date)
        if freq == "daily":
            start_date = current
            end_date = current + relativedelta(days=1)
        else:
            start_date = current.replace(day=1)
            end_date = start_date + relativedelta(months=1)

        output_folder = config.get("output_folder")
        csv_filename_template = config.get("template")
        ftp_remote_path = config.get("ftp_remote_path")

        one_more_day_flg = config.get("one_more_day_flg")
        if one_more_day_flg:
            # 1. 先將原本的 partition_date 字串轉為 date 物件，並加上 1 天
            # (如果你有 import datetime.timedelta，也可以用 timedelta(days=1))
            plus_one_date_obj = date.fromisoformat(partition_date) + relativedelta(days=1)
            plus_one_date_str = plus_one_date_obj.isoformat()  # 轉回 "YYYY-MM-DD" 字串
            
            # 2. 依照 freq 格式化日期 (daily 格式化為 "YYYYMMDD", monthly 格式化為 "YYYYMM")
            formatted_date = plus_one_date_str.replace("-", "") if freq == "daily" else plus_one_date_str.replace("-", "")[:6]
            
        actual_csv_name = csv_filename_template.format(date=formatted_date) if csv_filename_template else None

        # 在 _export_asset 裡
        remote_csv_path = os.path.join(output_folder, actual_csv_name) if output_folder and actual_csv_name else None


        command = [
            "python3.11", "/home/bcp_runner/scripts/export_remote.py",
            "--delimiter", f"{delimiter}",
            "--encoding", encoding,
            "--start-date", start_date.isoformat(),
            "--end-date", end_date.isoformat(),
        ]

        # ------------------------------------------------------------
        # 落地策略
        # 匯出的名單是「解密後的明文」，能不落地就不要落地。
        # 有設 ftp_remote_path 代表要直接送 FTP，此時預設不寫本機檔案；
        # 只有明確設 keep_local_copy=True（測試用）才會同時落地，而且會
        # 在 log 留下警告，避免有人測完忘記關掉。
        # ------------------------------------------------------------
        keep_local_copy = config.get("keep_local_copy", False)

        if ftp_remote_path and remote_csv_path:
            if keep_local_copy:
                context.log.warning(
                    "⚠️ keep_local_copy=True：解密後的明文 CSV 會同時留在 VM1，"
                    "僅供測試，上線前請移除此設定"
                )
                log_detail(context, f"keep_local_copy 落地路徑: {remote_csv_path}")
            else:
                log_detail(
                    context,
                    f"已設定 ftp_remote_path，不落地，忽略 output_folder（{output_folder}）",
                )
                remote_csv_path = None

        if remote_csv_path:
            command.extend(["--output", remote_csv_path])

        if ftp_remote_path:
            ftp_dir = ftp_remote_path.format(date=formatted_date)
            if ftp_dir.endswith("/"):
                ftp_dir = ftp_dir + actual_csv_name
            command.extend(["--ftp-remote-path", ftp_dir])

        if source_sql:
            command.extend(["--sql", source_sql])
        else:
            command.extend([
                "--table", source_table,
                "--date-column", date_column,
            ])
            
        if decrypt_fields:
            command.extend(["--decrypt-fields", ",".join(decrypt_fields)])

        result = ssh_pipes.run(context=context, command=command)

        return result.get_results()

    return [_export_asset]


all_export_assets = []
for tbl, cfg in SQL_TO_CSV_MAPPING.items():
    all_export_assets.extend(build_export_assets(table_name=tbl, config=cfg))


# ==============================================================================
# 2. 【Transform 階段】執行 Docker 內部的 dbt
# ==============================================================================
def get_topological_levels(models, manifest):
    nodes = manifest.get("nodes", {})

    dep_map = {}
    for node_id, node in nodes.items():
        if node.get("resource_type") in ("model", "snapshot"):
            name = node["name"]
            if name in models:
                deps = [
                    n.split(".")[-1]
                    for n in node.get("depends_on", {}).get("nodes", [])
                    if ("model." in n or "snapshot." in n) and n.split(".")[-1] in models
                ]
                dep_map[name] = deps

    for m in models:
        if m not in dep_map:
            dep_map[m] = []

    in_degree = {m: 0 for m in dep_map}
    adjacency = {m: [] for m in dep_map}

    for m, deps in dep_map.items():
        for d in deps:
            if d in adjacency:
                adjacency[d].append(m)
                in_degree[m] += 1

    levels = []
    current_level = [m for m in in_degree if in_degree[m] == 0]

    while current_level:
        levels.append(current_level)
        next_level = []
        for node in current_level:
            for neighbor in adjacency.get(node, []):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    next_level.append(neighbor)
        current_level = next_level

    return levels


def _run_dbt_levels(context, pipes, manifest, selected_models, partition_date, is_snapshot=False):
    levels = get_topological_levels(selected_models, manifest)
    context.log.info(f"[拓撲分層] 共 {len(levels)} 層")
    log_detail(context, f"拓撲分層明細: {levels}")

    max_retries = 2
    retry_delay = 30

    for level_idx, level_models in enumerate(levels):
        context.log.info(f"▶ 執行第 {level_idx + 1} 層（{len(level_models)} 個模型）")
        log_detail(context, f"第 {level_idx + 1} 層模型: {level_models}")

        for attempt in range(max_retries + 1):
            try:
                dbt_cmd = "snapshot" if is_snapshot else "build"
                dbt_build_args = [dbt_cmd]
                dbt_build_args.append("--no-fail-fast")
                dbt_build_args.extend(["--select", " ".join(level_models)])
                dbt_build_args.append("--debug")

                if partition_date:
                    dbt_vars = {"target_date": partition_date}
                    dbt_build_args.extend(["--vars", json.dumps(dbt_vars)])

                full_command = ["dbt"] + dbt_build_args + ["--profiles-dir", "."]
                log_detail(context, f"dbt 指令: {' '.join(full_command)}")

                pipes.run(
                    command=full_command,
                    image="dai/dagster:v2.6",
                    context=context,
                    extras={"stream_logs": True},
                    container_kwargs={
                        "working_dir": "/app/workspace/dbt_project",
                        "network": "docker-compose_vm4-network",
                        "volumes": {
                            "/data/deploy/workspace/dagster_workspace/dbt_project": {
                                "bind": "/app/workspace/dbt_project",
                                "mode": "rw",
                            }
                        }
                    }
                )

                for model_name in level_models:
                    yield MaterializeResult(
                        asset_key=AssetKey([model_name]),
                        metadata={
                            "description": f"第 {level_idx + 1} 層，dbt model 成功",
                            "level": level_idx + 1
                        }
                    )
                context.log.info(f"✅ 第 {level_idx + 1} 層完成")
                log_detail(context, f"第 {level_idx + 1} 層完成: {level_models}")
                break

            except Exception as e:
                if attempt < max_retries:
                    context.log.warning(
                        f"⚠️ 第 {level_idx + 1} 層失敗 (attempt {attempt + 1})，"
                        f"{retry_delay} 秒後重試"
                    )
                    log_detail_error(
                        context,
                        f"第 {level_idx + 1} 層失敗 (attempt {attempt + 1}) "
                        f"models={level_models} 原因: {e}",
                    )
                    time.sleep(retry_delay)
                else:
                    context.log.error(
                        f"❌ 第 {level_idx + 1} 層徹底失敗，詳見明細 log"
                    )
                    log_detail_error(
                        context,
                        f"第 {level_idx + 1} 層徹底失敗 models={level_models} 原因: {e}",
                    )
                    raise e


# ==============================================================================
# 2a. Daily dbt models
# ==============================================================================
@dbt_assets(
    manifest=post_office_dbt.manifest_path,
    partitions_def=daily_partitions,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=1),
    dagster_dbt_translator=CustomDbtTranslator(),
    select="tag:daily_job",
)
def post_office_dbt_assets(context: AssetExecutionContext, pipes: PipesDockerClient):
    selected_models = [key.path[-1] for key in context.selected_asset_keys]
    partition_date = None
    try:
        keys = context.partition_keys
        if keys:
            partition_date = keys[0]
            log_detail(context, f"dbt target_date = {partition_date}")
    except Exception:
        context.log.warning("⚠️ [警告] 完全未偵測到任何 Partition 資訊，將走預設流程")

    with open(post_office_dbt.manifest_path) as f:
        manifest = json.load(f)

    yield from _run_dbt_levels(context, pipes, manifest, selected_models, partition_date)


# ==============================================================================
# 2b. Monthly dbt snapshots & models
# ==============================================================================
@dbt_assets(
    manifest=post_office_dbt.manifest_path,
    partitions_def=monthly_partitions,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=1),
    dagster_dbt_translator=CustomDbtTranslator(),
    select="tag:monthly_job"
)
def post_office_dbt_monthly_assets(context: AssetExecutionContext, pipes: PipesDockerClient):
    selected_models = [key.path[-1] for key in context.selected_asset_keys]
    partition_date = None
    try:
        keys = context.partition_keys
        if keys:
            partition_date = keys[0]
            log_detail(context, f"dbt target_date = {partition_date}")
    except Exception:
        context.log.warning("⚠️ [警告] 完全未偵測到任何 Partition 資訊，將走預設流程")

    with open(post_office_dbt.manifest_path) as f:
        manifest = json.load(f)

    yield from _run_dbt_levels(context, pipes, manifest, selected_models, partition_date, is_snapshot=True)


# ==============================================================================
# 3. 註冊 Definitions
# ==============================================================================
# defs = Definitions(
#     assets=[*all_table_assets, post_office_dbt_assets],
#     resources={
#         "pipes": docker_pipes
#     }
# )
