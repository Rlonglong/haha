import json
from dagster import AssetSelection, AssetKey
from dagster_code.table_mapping import TABLE_CSV_MAPPING

def build_monthly_selection(manifest_path):
    # EL 層
    monthly_tables = [
        t for t, c in TABLE_CSV_MAPPING.items()
        if c.get("freq") == "monthly"
    ]
    monthly_el_selection = AssetSelection.keys(
        *[AssetKey(["database", t]) for t in monthly_tables]
    ) if monthly_tables else AssetSelection.nothing()

    # dbt 層：從 manifest 找 monthly_job tag 或 snapshot
    with open(manifest_path) as f:
        manifest = json.load(f)

    monthly_dbt_keys = [
        AssetKey([node["name"]])
        for node in manifest.get("nodes", {}).values()
        if "monthly_job" in node.get("tags", [])
        or node.get("resource_type") == "snapshot"
    ]
    monthly_dbt_selection = (
        AssetSelection.keys(*monthly_dbt_keys)
        if monthly_dbt_keys
        else AssetSelection.nothing()
    )

    return monthly_el_selection | monthly_dbt_selection