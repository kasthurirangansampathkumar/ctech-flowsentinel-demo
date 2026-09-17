"""SentinelView AI -- Vendor CRM Sync.

Loads the daily vendor customer-profile feed from the GCS landing zone into
raw_staging.customer_profiles -- this is what the Source Feed Sentinel
watches for its SLA guardrail.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "ctech-flowsentinel-ai"
BUCKET = "ctech-flowsentinel-ai-landing-zone"
DAG_ID = "vendor_crm_sync"

with DAG(
    dag_id=DAG_ID,
    description="Loads the vendor CRM customer-profile feed from GCS into raw_staging.customer_profiles.",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    default_args={"owner": "flowsentinel-ai", "retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["flowsentinel", "ingestion"],
) as dag:

    load_profiles = BigQueryInsertJobOperator(
        task_id="load_profiles_from_gcs",
        configuration={"load": {
            "sourceUris": [f"gs://{BUCKET}/vendor_crm/customer_profiles_2026-09-08.json"],
            "destinationTable": {
                "projectId": PROJECT_ID, "datasetId": "raw_staging", "tableId": "customer_profiles",
            },
            "sourceFormat": "NEWLINE_DELIMITED_JSON",
            "writeDisposition": "WRITE_TRUNCATE",
            "schema": {"fields": [
                {"name": "cust_id", "type": "STRING"},
                {"name": "name", "type": "STRING"},
                {"name": "tier", "type": "STRING"},
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

    load_profiles >> log_run
