"""FlowSentinel AI -- Customer 360 Aggregation.

Joins customer profiles with their order history into a single
per-customer view, refreshed after the CRM sync and sales transformation.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "ctech-flowsentinel-ai"
DAG_ID = "customer_360_aggregation"

with DAG(
    dag_id=DAG_ID,
    description="Builds dw_analytics.customer_360 from customer profiles and fact orders.",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    default_args={"owner": "flowsentinel-ai", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["flowsentinel", "aggregation"],
) as dag:

    build_customer_360 = BigQueryInsertJobOperator(
        task_id="build_customer_360",
        configuration={"query": {
            "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.dw_analytics.customer_360` AS
                SELECT
                  p.cust_id, p.name, p.tier,
                  COUNT(f.order_id) AS lifetime_orders,
                  COALESCE(SUM(f.amount_usd), 0.0) AS lifetime_amount_usd,
                  MAX(f.order_date) AS last_order_date
                FROM `{PROJECT_ID}.raw_staging.customer_profiles` p
                LEFT JOIN `{PROJECT_ID}.dw_analytics.fact_orders` f USING (cust_id)
                GROUP BY p.cust_id, p.name, p.tier
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

    build_customer_360 >> log_run
