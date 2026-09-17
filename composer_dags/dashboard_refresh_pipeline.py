"""SentinelView AI -- Dashboard Refresh Pipeline.

Aggregates fact_orders into the daily sales rollup the executive dashboard
reads -- this is the pipeline the Pace Predictor agent watches for a
missed-deadline SLA.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "ctech-flowsentinel-ai"
DAG_ID = "dashboard_refresh_pipeline"

with DAG(
    dag_id=DAG_ID,
    description="Refreshes dw_analytics.agg_daily_sales from dw_analytics.fact_orders.",
    schedule="0 7 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    default_args={"owner": "flowsentinel-ai", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["flowsentinel", "reporting"],
) as dag:

    refresh_agg_daily_sales = BigQueryInsertJobOperator(
        task_id="refresh_agg_daily_sales",
        configuration={"query": {
            "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.dw_analytics.agg_daily_sales` AS
                SELECT order_date, COUNT(*) AS total_orders, SUM(amount_usd) AS total_amount_usd
                FROM `{PROJECT_ID}.dw_analytics.fact_orders`
                GROUP BY order_date
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

    refresh_agg_daily_sales >> log_run
