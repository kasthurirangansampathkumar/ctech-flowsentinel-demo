"""FlowSentinel AI -- Sales Transformation Pipeline.

Stages raw orders and merges new rows into the fact table. This is the DAG
every ticket-board demo scenario (schema drift, backfill) refers to.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "ctech-flowsentinel-ai"
DAG_ID = "sales_transformation_pipeline"

with DAG(
    dag_id=DAG_ID,
    description="Stages raw_staging.orders and merges into dw_analytics.fact_orders.",
    schedule="@hourly",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    default_args={"owner": "flowsentinel-ai", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["flowsentinel", "sales"],
) as dag:

    stage_orders = BigQueryInsertJobOperator(
        task_id="stage_orders",
        configuration={"query": {
            "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.dw_analytics.stg_orders` AS
                SELECT * FROM `{PROJECT_ID}.raw_staging.orders`
            """,
            "useLegacySql": False,
        }},
    )

    merge_fact_orders = BigQueryInsertJobOperator(
        task_id="merge_fact_orders",
        configuration={"query": {
            "query": f"""
                MERGE `{PROJECT_ID}.dw_analytics.fact_orders` T
                USING `{PROJECT_ID}.dw_analytics.stg_orders` S
                ON T.order_id = S.order_id
                WHEN NOT MATCHED THEN
                  INSERT (order_id, cust_id, order_date, amount_usd, status)
                  VALUES (S.order_id, S.cust_id, S.order_date, S.amount_usd, S.status)
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

    stage_orders >> merge_fact_orders >> log_run
