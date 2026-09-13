"""FlowSentinel AI -- Vendor E-Commerce Ingest.

Loads the hourly vendor orders CSV from the GCS landing zone into
raw_staging.orders. This is the ingestion stage feeding the sales
transformation pipeline.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "ctech-flowsentinel-ai"
BUCKET = "ctech-flowsentinel-ai-landing-zone"
DAG_ID = "vendor_ecom_ingest"

with DAG(
    dag_id=DAG_ID,
    description="Loads the vendor e-commerce orders feed from GCS into raw_staging.orders.",
    schedule="@hourly",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    default_args={"owner": "flowsentinel-ai", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["flowsentinel", "ingestion"],
) as dag:

    load_orders = BigQueryInsertJobOperator(
        task_id="load_orders_from_gcs",
        configuration={"load": {
            "sourceUris": [f"gs://{BUCKET}/vendor_ecom/orders_valid.csv"],
            "destinationTable": {
                "projectId": PROJECT_ID, "datasetId": "raw_staging", "tableId": "orders",
            },
            "sourceFormat": "CSV",
            "skipLeadingRows": 1,
            "writeDisposition": "WRITE_TRUNCATE",
            "autodetect": False,
            "schema": {"fields": [
                {"name": "order_id", "type": "STRING"},
                {"name": "cust_id", "type": "STRING"},
                {"name": "order_date", "type": "DATE"},
                {"name": "amount_usd", "type": "FLOAT64"},
                {"name": "status", "type": "STRING"},
            ]},
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

    load_orders >> log_run
