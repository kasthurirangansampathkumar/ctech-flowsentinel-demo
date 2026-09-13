"""FlowSentinel AI -- DQ Validation Suite.

Runs the data-quality checks the DQ Triage Agent's tickets refer to: null
customer ids and duplicate order ids in the fact table. Logs a failed run
(not just an alert) when a check finds a real violation.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCheckOperator

PROJECT_ID = "ctech-flowsentinel-ai"
DAG_ID = "dq_validation_suite"

with DAG(
    dag_id=DAG_ID,
    description="Checks dw_analytics.fact_orders for null cust_id and duplicate order_id.",
    schedule="@hourly",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    default_args={"owner": "flowsentinel-ai", "retries": 0},
    tags=["flowsentinel", "data-quality"],
) as dag:

    check_no_duplicate_order_ids = BigQueryCheckOperator(
        task_id="check_no_duplicate_order_ids",
        sql=f"""
            SELECT COUNT(*) = 0 FROM (
              SELECT order_id FROM `{PROJECT_ID}.dw_analytics.fact_orders`
              GROUP BY order_id HAVING COUNT(*) > 1
            )
        """,
        use_legacy_sql=False,
    )

    check_no_null_cust_id = BigQueryCheckOperator(
        task_id="check_no_null_cust_id",
        sql=f"SELECT COUNT(*) = 0 FROM `{PROJECT_ID}.dw_analytics.fact_orders` WHERE cust_id IS NULL",
        use_legacy_sql=False,
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

    [check_no_duplicate_order_ids, check_no_null_cust_id] >> log_run
