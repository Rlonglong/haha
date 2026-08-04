import json
from pathlib import Path

from dagster import (
    Definitions,
    define_asset_job,
    AssetSelection,
    AssetKey
)

from dagster_code.assets import (
    all_table_assets,
    all_export_assets,
    post_office_dbt_assets,
    post_office_dbt_monthly_assets,
    docker_pipes,
    post_office_dbt
)
from dagster_code.sensors import daily_file_watcher_sensor, monthly_file_watcher_sensor, automation_sensor, slack_failure_alert
from dagster_code.table_mapping import TABLE_CSV_MAPPING
from dagster_code.assets import post_office_dbt
from dagster_code.selections import build_monthly_selection, build_manual_only_selection
from dagster_code.pipes_ssh_client import PipesSSHClient, SSH_HOST, SSH_USER, SSH_KEY_PATH
from dagster_code.db_sync.assets import build_all_db_sync_assets
from dagster_code.db_sync.sensors import db_sync_watcher_sensor


# ==============================================================================
# 動態建立 Monthly/Daily Selection
# ==============================================================================

# build_monthly_selection 沒有任何月檔節點時回傳 None，
# 此時月檔選集只剩下 monthly_extract_load 這個 group。
_monthly_keys_selection = build_monthly_selection(post_office_dbt.manifest_path)
monthly_selection = AssetSelection.groups("monthly_extract_load")

if _monthly_keys_selection is not None:
    monthly_selection = _monthly_keys_selection | monthly_selection

daily_selection = AssetSelection.all() - monthly_selection

# manual_only=True 的表：連批次 job 都不涵蓋，只能單獨手動 materialize。
# 沒有任何表設 manual_only 時是 None，此時兩個 job 的選集維持原樣。
manual_only_selection = build_manual_only_selection()

daily_job_selection = AssetSelection.all() - monthly_selection
monthly_job_selection = monthly_selection

if manual_only_selection is not None:
    daily_job_selection = daily_job_selection - manual_only_selection
    monthly_job_selection = monthly_job_selection - manual_only_selection

# ==============================================================================
# Job 定義
# ==============================================================================
daily_all_assets_job = define_asset_job(
    name="__DAILY_ASSET_JOB",
    selection=daily_job_selection
)

monthly_assets_job = define_asset_job(
    name="__MONTHLY_ASSET_JOB",
    selection=monthly_job_selection
)

ssh_pipes_client = PipesSSHClient(
    ssh_host=SSH_HOST,
    ssh_user=SSH_USER,
    ssh_key_path=SSH_KEY_PATH,
)

# ==============================================================================
# Definitions
# ==============================================================================
defs = Definitions(
    assets=[*all_table_assets, *all_export_assets, post_office_dbt_assets, post_office_dbt_monthly_assets, *build_all_db_sync_assets()],
    jobs=[daily_all_assets_job, monthly_assets_job],
    sensors=[daily_file_watcher_sensor, monthly_file_watcher_sensor, automation_sensor, slack_failure_alert, db_sync_watcher_sensor],
    resources={
        "pipes": docker_pipes,
        "ssh_pipes": ssh_pipes_client
    }
)