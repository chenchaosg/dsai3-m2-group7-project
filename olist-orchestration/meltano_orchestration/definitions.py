# =============================================================================
# definitions.py - if defines two standalone Jobs for Meltano & dbt
# =============================================================================

import os
import json
import glob
from pathlib import Path
import subprocess

from dagster import (
    Definitions,
    job,
    ScheduleDefinition,
    OpExecutionContext,
    op,
    Out,
    In,
    Nothing,
    Output,
)
from dagster_meltano import MeltanoResource

# --- 1. variables ---
PROJECT_ROOT_DIR = Path(__file__).parent.parent
MELTANO_PROJECT_DIR = PROJECT_ROOT_DIR / "meltano_orchestration"
DBT_PROJECT_DIR = PROJECT_ROOT_DIR / "dbt_orchestration" 
DATA_DIR = MELTANO_PROJECT_DIR / "data"
MANIFEST_FILE_PATH = MELTANO_PROJECT_DIR / "olist_files_definition.json"

PRIMARY_KEY_MAP = {
    "olist_customers_dataset": ["customer_id"], "olist_geolocation_dataset": ["geolocation_zip_code_prefix"],
    "olist_order_items_dataset": ["order_id", "order_item_id"], "olist_order_payments_dataset": ["order_id", "payment_sequential"],
    "olist_order_reviews_dataset": ["review_id", "order_id"], "olist_orders_dataset": ["order_id"],
    "olist_products_dataset": ["product_id"], "olist_sellers_dataset": ["seller_id"],
    "product_category_name_translation": ["product_category_name"],
}

# --- 2. define resources ---
meltano_resource = MeltanoResource(project_dir=str(MELTANO_PROJECT_DIR))

# --- 3. define Ops ---

# Meltano related Ops
@op(name="build_json_manifest", out={"manifest_path": Out(str, is_required=False)})
def build_json_manifest_op(context: OpExecutionContext):
    context.log.info("Scanning for CSV files.")
    search_pattern = os.path.join(DATA_DIR, "*.csv")
    found_files = glob.glob(search_pattern)
    if not found_files:
        context.log.info("No CSV files found. Skipping.")
        return
    file_definitions = [{"entity": os.path.basename(fp).replace('.csv',''), "path": fp, "keys": PRIMARY_KEY_MAP.get(os.path.basename(fp).replace('.csv',''), [])} for fp in found_files]
    with open(MANIFEST_FILE_PATH, 'w') as f:
        json.dump(file_definitions, f, indent=2)
    yield Output(str(MANIFEST_FILE_PATH), "manifest_path")

@op(
    name="run_meltano_extract_load",
    required_resource_keys={"meltano"},
    ins={"manifest_path": In(str)}
)
def run_meltano_op(context: OpExecutionContext, manifest_path: str):
    context.log.info(f"Received manifest path '{manifest_path}'. Starting Meltano EL process.")
    context.resources.meltano.execute_command(
        "run tap-csv target-bigquery --force",
        os.environ,
        context.log
    )

# dbt related Op
@op(name="run_dbt_transform")
def run_dbt_op(context: OpExecutionContext):
    context.log.info(f"Starting dbt transform process via shell command in: {str(DBT_PROJECT_DIR)}")
    
    process = subprocess.run(
        ["dbt", "run"],
        cwd=str(DBT_PROJECT_DIR),
        capture_output=True,
        text=True
    )

    context.log.info(f"dbt stdout:\n{process.stdout}")
    if process.stderr:
        context.log.error(f"dbt stderr:\n{process.stderr}")

    if process.returncode != 0:
        raise Exception("dbt command failed!")
    
    context.log.info("dbt transform process completed.")

# --- 4. define Job ---

# *** create a standalone job for Meltano ***
@job(
    name="meltano_job",
    resource_defs={"meltano": meltano_resource}
)
def meltano_job():
    """
    This Job is for Meltano，for data extract and load。
    """
    manifest_path = build_json_manifest_op()
    run_meltano_op(manifest_path=manifest_path)

# *** create a standalone job for dbt ***
@job(name="dbt_job")
def dbt_job():
    """
    This Job is for dbt，for data transform. No dpendencies on other jobs.
    """
    run_dbt_op()


# --- 5. define Schedule ---
# create two schedules separately, one for meltano_job and another for dbt_job
meltano_schedule = ScheduleDefinition(
    job=meltano_job,
    cron_schedule="0 1 * * *", # 1AM everyday run for Meltano
    execution_timezone="Asia/Singapore"
)

dbt_schedule = ScheduleDefinition(
    job=dbt_job,
    cron_schedule="0 2 * * *", # 2AM everyday run for dbt，after Meltano job
    execution_timezone="Asia/Singapore"
)

# --- 6. final Definitions ---
defs = Definitions(
    # register two Jobs to Dagster
    jobs=[meltano_job, dbt_job],
    schedules=[meltano_schedule, dbt_schedule],
    resources={"meltano": meltano_resource}
)
