"""FlowSentinel AI -- Inventory Sync Pipeline.

Refreshes warehouse inventory levels from the raw snapshot table.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "ctech-flowsentinel-ai"
DAG_ID = "inventory_sync_pipeline"

with DAG(
    dag_id=DAG_ID,
    description="Refreshes dw_analytics.inventory_levels from raw_staging.inventory_snapshot.",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    default_args={"owner": "flowsentinel-ai", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["flowsentinel", "inventory"],
) as dag:

    refresh_inventory_levels = BigQueryInsertJobOperator(
        task_id="refresh_inventory_levels",
        configuration={"query": {
            "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.dw_analytics.inventory_levels` AS
                SELECT sku, warehouse, qty_on_hand, snapshot_date
                FROM `{PROJECT_ID}.raw_staging.inventory_snapshot`
            """,
            "useLegacySql": False,
        }},
    )

    log_run = BigQueryInsertJobOperator(
        task_id="log_run",
        configuration={"query": {
            "query": f"""
                INSERT INTO `{PROJECT_ID}.de_ops_metadata.pipeline_runs` (dag_id, run_ts, status)
                VALUES ('{DAG_ID}', CURRENT_TIMESTAMP(), 'success')
            """,
            "useLegacySql": False,
        }},
    )

    refresh_inventory_levels >> log_run
