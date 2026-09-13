"""FlowSentinel AI -- Backfill Orchestrator.

Materializes a dated snapshot of fact_orders into the backfill sandbox --
the real counterpart to the Backfill Planning Agent's blast-radius plan.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "ctech-flowsentinel-ai"
DAG_ID = "backfill_orchestrator"

with DAG(
    dag_id=DAG_ID,
    description="Materializes a dated fact_orders snapshot into backfill_sandbox.",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    default_args={"owner": "flowsentinel-ai", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["flowsentinel", "backfill"],
) as dag:

    snapshot_fact_orders = BigQueryInsertJobOperator(
        task_id="snapshot_fact_orders",
        configuration={"query": {
            "query": f"""
                CREATE TABLE IF NOT EXISTS
                  `{PROJECT_ID}.backfill_sandbox.fact_orders_{{{{ ds_nodash }}}}`
                AS
                SELECT *, CURRENT_TIMESTAMP() AS backfilled_at
                FROM `{PROJECT_ID}.dw_analytics.fact_orders`
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

    snapshot_fact_orders >> log_run
