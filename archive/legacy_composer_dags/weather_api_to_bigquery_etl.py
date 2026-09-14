import json
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import storage

# ==========================================
# ENVIRONMENT CONFIGURATION
# ==========================================
PROJECT_ID = "ctech-flowsentinel-ai"
GCS_BUCKET_NAME = "us-central1-ctech-flowsenti-34a08e5b-bucket"
BQ_DATASET = "weather_analytics"
STAGING_TABLE = "staging_weather"
FINAL_TABLE = "f_weather_daily_summary"

# Live FlowSentinel Cloud Run URL for automated incident triage
FLOWSENTINEL_URL = "https://flowsentinel-dashboard-673338764809.us-central1.run.app/api/tickets"


def notify_flowsentinel_on_failure(context):
    """
    Sends execution context and failure details to the live FlowSentinel dashboard
    when any task in this DAG fails.
    """
    ti = context.get("task_instance")
    dag_id = ti.dag_id
    task_id = ti.task_id
    exception = context.get("exception")

    payload = {
        "category": "auto",
        "pipeline": dag_id,
        "table": f"{BQ_DATASET}.{FINAL_TABLE}",
        "message": f"Airflow Task '{task_id}' failed in DAG '{dag_id}': {exception}"
    }

    try:
        response = requests.post(FLOWSENTINEL_URL, json=payload, timeout=5)
        print(f"FlowSentinel callback dispatched. HTTP Status: {response.status_code}")
    except Exception as exc:
        print(f"Failed to post failure event to FlowSentinel: {exc}")


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notify_flowsentinel_on_failure,
}


def extract_api_to_gcs(bucket_name: str, destination_blob: str):
    """
    Fetches hourly weather data from the public Open-Meteo REST API,
    converts it to Newline-Delimited JSON (NDJSON), and uploads to Cloud Storage.
    """
    api_url = (
        "https://api.open-meteo.com/v1/forecast?"
        "latitude=35.6895&longitude=139.6917&hourly=temperature_2m,relative_humidity_2m&timezone=UTC"
    )
    
    print(f"Requesting data from: {api_url}")
    response = requests.get(api_url, timeout=30)
    response.raise_for_status()
    data = response.json()

    hourly_times = data["hourly"]["time"]
    temperatures = data["hourly"]["temperature_2m"]
    humidities = data["hourly"]["relative_humidity_2m"]

    records = []
    for t, temp, hum in zip(hourly_times, temperatures, humidities):
        records.append({
            "timestamp": t,
            "temperature_c": float(temp),
            "relative_humidity": float(hum),
            "extracted_at": datetime.utcnow().isoformat()
        })

    # Prepare records as Newline-Delimited JSON for BigQuery ingestion
    ndjson_data = "\n".join([json.dumps(record) for record in records])

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_string(ndjson_data, content_type="application/x-ndjson")
    print(f"Uploaded {len(records)} rows to gs://{bucket_name}/{destination_blob}")


with DAG(
    dag_id="weather_api_to_bigquery_etl",
    default_args=default_args,
    description="Fetch weather API data, stage into GCS bucket, and transform into BigQuery",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "weather", "gcs", "bigquery"],
) as dag:

    # Task 1: Extract API payload to GCS
    extract_task = PythonOperator(
        task_id="extract_api_to_gcs",
        python_callable=extract_api_to_gcs,
        op_kwargs={
            "bucket_name": GCS_BUCKET_NAME,
            "destination_blob": "raw/weather_data.json",
        },
    )

    # Task 2: Load raw JSON records into BigQuery Staging Table
    load_gcs_to_bq_staging = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq_staging",
        bucket=GCS_BUCKET_NAME,
        source_objects=["raw/weather_data.json"],
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.{STAGING_TABLE}",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    # Task 3: SQL Transformation - Aggregate hourly stats to daily summaries
    transform_sql = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{BQ_DATASET}.{FINAL_TABLE}` AS
    SELECT 
        DATE(TIMESTAMP(timestamp)) as observation_date,
        ROUND(AVG(temperature_c), 2) as avg_temp_c,
        ROUND(MAX(temperature_c), 2) as max_temp_c,
        ROUND(MIN(temperature_c), 2) as min_temp_c,
        ROUND(AVG(relative_humidity), 2) as avg_humidity_pct,
        COUNT(1) as total_readings,
        CURRENT_TIMESTAMP() as calculated_at
    FROM `{PROJECT_ID}.{BQ_DATASET}.{STAGING_TABLE}`
    GROUP BY 1
    ORDER BY observation_date DESC;
    """

    transform_to_analytics = BigQueryInsertJobOperator(
        task_id="transform_to_analytics",
        configuration={
            "query": {
                "query": transform_sql,
                "useLegacySql": False,
            }
        },
    )

    extract_task >> load_gcs_to_bq_staging >> transform_to_analytics
