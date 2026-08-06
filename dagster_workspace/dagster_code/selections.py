import json
from dagster import AssetSelection, AssetKey
from dagster_code.table_mapping import TABLE_CSV_MAPPING
from dagster_code.db_sync.config import DB_SYNC_MAPPING


def build_el_asset_keys(table_name: str, config: dict) -> list[AssetKey]:
    """依 config 組出這張 EL 表實際會存在的節點 asset key。
    ⚠️ 建立條件必須跟 assets.py::build_table_assets 保持一致，
    那邊多加一個節點，這裡也要跟著加。"""
    use_ftp_fetch = config.get("use_ftp_fetch", False)

    keys = []
    if use_ftp_fetch:
        keys.append(AssetKey(["file_system", f"{table_name}_fetch_ftp"]))
    if config.get("use_data_rule", False):
        keys.append(AssetKey(["file_system", f"{table_name}_named"]))
    if config.get("encrypt_fields", []):
        keys.append(AssetKey(["file_system", f"{table_name}_encrypted"]))
    if config.get("has_clean_func", False):
        keys.append(AssetKey(["file_system", f"{table_name}_clean"]))
    keys.append(AssetKey(["database", table_name]))
    if use_ftp_fetch:
        keys.append(AssetKey(["file_system", f"{table_name}_archive_cleanup"]))
    return keys


def build_db_sync_asset_keys(table_name: str) -> list[AssetKey]:
    """db_sync 線固定四個節點，全部都會建立。"""
    return [
        AssetKey(["file_system", f"{table_name}_db_extract"]),
        AssetKey(["file_system", f"{table_name}_encrypted"]),
        AssetKey(["database", table_name]),
        AssetKey(["file_system", f"{table_name}_cleanup"]),
    ]


def build_manual_only_selection():
    """manual_only=True 的表：sensor 不碰，也要從 __DAILY / __MONTHLY 兩個
    批次 job 的選集裡扣掉，確保只有「明確點這張表」才會執行。

    沒有任何表設 manual_only 時回傳 None（而不是空的 AssetSelection），
    呼叫端要自己判斷 —— 這樣就不需要依賴「空選集」的 API，
    不同 dagster 版本都能用。"""
    keys = []
    for table_name, config in TABLE_CSV_MAPPING.items():
        if config.get("manual_only", False):
            keys.extend(build_el_asset_keys(table_name, config))
    for table_name, config in DB_SYNC_MAPPING.items():
        if config.get("manual_only", False):
            keys.extend(build_db_sync_asset_keys(table_name))

    if not keys:
        return None
    return AssetSelection.keys(*keys)


def build_monthly_selection(manifest_path):
    """月檔節點的 asset key 集合：EL 層的月檔表 + dbt 層有 monthly_job tag
    或是 snapshot 的節點。

    兩層合成同一份 key 清單再一次轉成 AssetSelection，完全不需要「空選集」
    的 API；都沒有的話回傳 None，由呼叫端判斷。"""
    monthly_keys = []

    # EL 層：freq == "monthly" 的表
    monthly_keys += [
        AssetKey(["database", t])
        for t, c in TABLE_CSV_MAPPING.items()
        if c.get("freq") == "monthly"
    ]

    # dbt 層：從 manifest 找 monthly_job tag 或 snapshot
    with open(manifest_path) as f:
        manifest = json.load(f)

    monthly_keys += [
        AssetKey([node["name"]])
        for node in manifest.get("nodes", {}).values()
        if "monthly_job" in node.get("tags", [])
        or node.get("resource_type") == "snapshot"
    ]

    if not monthly_keys:
        return None
    return AssetSelection.keys(*monthly_keys)