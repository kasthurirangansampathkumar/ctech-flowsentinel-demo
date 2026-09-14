# dags/customer_profile_ingestion.py

from datetime import datetime, timedelta
import json
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


PROJECT_ID = "ctech-flowsentinel-ai"
BUCKET = f"{PROJECT_ID}-landing-zone"

RAW_FILE = "/tmp/customer_profiles.json"
GCS_RAW = "raw/customer_profiles/{{ ds }}/customer_profiles.json"

default_args = {
    "owner": "flowsentinel",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def extract_customer_profiles(**context):
    url = "https://api.example.com/v1/customers"

    response = requests.get(
        url,
        timeout=30,
        headers={"Accept": "application/json"},
    )

    response.raise_for_status()

    with open(RAW_FILE, "w") as f:
        json.dump(response.json(), f)


def clean_customer_profiles():
    # Production implementation can use Spark/Dataflow/BigQuery SQL.
    # This lightweight example performs basic normalization.
    with open(RAW_FILE) as f:
        data = json.load(f)

    cleaned = []

    for row in data:
        cleaned.append({
            "customer_id": str(row.get("customer_id", "")).strip(),
            "customer_name": str(row.get("customer_name", "")).strip(),
            "email": str(row.get("email", "")).lower().strip(),
            "country": str(row.get("country", "")).upper().strip(),
        })

    with open("/tmp/customer_profiles_clean.json", "w") as f:
        for row in cleaned:
            f.write(json.dumps(row) + "\n")


with DAG(
    dag_id="customer_profile_ingestion",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    tags=["etl", "customer", "flowsentinel"],
) as dag:

    extract = PythonOperator(
        task_id="extract_from_crm_api",
        python_callable=extract_customer_profiles,
    )

    raw_to_gcs = LocalFilesystemToGCSOperator(
        task_id="load_raw_to_gcs",
        src=RAW_FILE,
        dst=GCS_RAW,
        bucket=BUCKET,
    )

    clean = PythonOperator(
        task_id="clean_customer_data",
        python_callable=clean_customer_profiles,
    )

    staging = LocalFilesystemToGCSOperator(
        task_id="load_clean_staging_file",
        src="/tmp/customer_profiles_clean.json",
        dst="staging/customer_profiles/{{ ds }}/customer_profiles.json",
        bucket=BUCKET,
    )

    load_bq = GCSToBigQueryOperator(
        task_id="load_customer_dimension",
        bucket=BUCKET,
        source_objects=[
            "staging/customer_profiles/{{ ds }}/customer_profiles.json"
        ],
        destination_project_dataset_table=(
            f"{PROJECT_ID}.analytics.dim_customer"
        ),
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )

    validate = BigQueryInsertJobOperator(
        task_id="validate_customer_count",
        configuration={
            "query": {
                "query": f"""
                    SELECT COUNT(*) AS customer_count
                    FROM `{PROJECT_ID}.analytics.dim_customer`
                    WHERE DATE(_PARTITIONTIME) = DATE('{{{{ ds }}}}')
                """,
                "useLegacySql": False,
            }
        },
    )

    extract >> raw_to_gcs >> clean >> staging >> load_bq >> validate