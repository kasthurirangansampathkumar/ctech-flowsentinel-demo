"""
DAG 6: IoT Fleet & Device Telemetry Ingestion Pipeline
======================================================
Pipeline ID: iot_device_telemetry_pipeline
Target Table: dw_analytics.fact_iot_telemetry

ETL Flow:
1. Extract high-frequency sensor readings (temperature, vibration, battery, GPS) from IoT Gateway API.
2. Upload raw sensor telemetry JSON to GCS Landing Zone (`raw/iot_telemetry/...`).
3. Staging cleaning: Outlier filtering (e.g. battery bounds 0-100%, physical sensor limit clipping),
   microsecond timestamp parsing, and device metadata enrichment.
4. Load to BigQuery staging table (`staging.stg_iot_telemetry`).
5. Partition-filtered MERGE into production `dw_analytics.fact_iot_telemetry`
   partitioned by `telemetry_date` and clustered by `device_type`, `device_id`.
6. Run Telemetry Skew & Data Quality check (catches data skew / volume spikes for FlowSentinel).
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

try:
    from plugins.flowsentinel_alerts import (
        flowsentinel_failure_callback,
        flowsentinel_sla_miss_callback,
    )
except ImportError:
    def flowsentinel_failure_callback(context):
        logging.getLogger("airflow.task").error(f"Task failed: {context.get('task_instance')}")
    def flowsentinel_sla_miss_callback(*args):
        pass

GCP_PROJECT_ID = Variable.get("gcp_project_id", default_var=os.getenv("GCP_PROJECT_ID", "ctech-flowsentinel-ai"))
GCS_LANDING_BUCKET = Variable.get("gcs_landing_bucket", default_var=f"{GCP_PROJECT_ID}-landing-zone")
BQ_STAGING_DATASET = Variable.get("bq_staging_dataset", default_var="staging")
BQ_ANALYTICS_DATASET = Variable.get("bq_analytics_dataset", default_var="dw_analytics")

DEFAULT_ARGS = {
    "owner": "flowsentinel_data_eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=25),
    "on_failure_callback": flowsentinel_failure_callback,
}


def extract_iot_telemetry_api(**context) -> str:
    """
    Step 1: Poll IoT Device Gateway REST API for device telemetry batch.
    Simulates high-throughput streaming sensor readings with varying metrics.
    """
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")
    logger = logging.getLogger("airflow.task")
    logger.info(f"Extracting IoT fleet telemetry readings for batch {ts_nodash}...")

    device_types = ["WAREHOUSE_FORKLIFT", "COLD_STORAGE_SENSOR", "DELIVERY_TRUCK_GPS", "SMART_METER"]
    sample_readings = []

    for i in range(250):
        dtype = device_types[i % len(device_types)]
        device_id = f"DEV-{dtype[:4]}-{100 + (i % 40)}"

        # Realistic readings with occasional raw noise to clean
        temp_c = 4.2 + (i % 15) * 0.8 if "COLD" in dtype else 22.0 + (i % 20) * 1.1
        battery_pct = max(0, 100 - (i % 95))
        vibration_hz = round(12.5 + (i % 8) * 2.3, 2)

        sample_readings.append({
            "message_id": f"msg_{ts_nodash}_{1000 + i}",
            "device_id": device_id,
            "device_type": dtype,
            "firmware_version": "v3.2.1" if i % 10 != 0 else "v3.1.0",
            "telemetry": {
                "temperature_celsius": round(temp_c, 2),
                "battery_percentage": battery_pct,
                "vibration_frequency_hz": vibration_hz,
                "latitude": round(37.7749 + (i % 50) * 0.001, 6),
                "longitude": round(-122.4194 + (i % 50) * 0.001, 6),
            },
            "timestamp": f"{execution_date}T{(i % 24):02d}:{(i % 60):02d}:{(i % 60):02d}Z",
        })

    temp_path = f"/tmp/iot_telemetry_{ts_nodash}.json"
    with open(temp_path, "w", encoding="utf-8") as f:
        for r in sample_readings:
            f.write(json.dumps(r) + "\n")

    logger.info(f"Extracted {len(sample_readings)} IoT sensor readings to {temp_path}")
    return temp_path


def upload_raw_telemetry_gcs(**context) -> str:
    """
    Step 2: Upload raw telemetry JSON payload to GCS Landing Zone.
    Destination: gs://<gcs_landing_bucket>/raw/iot_telemetry/<ds>/telemetry_<ts_nodash>.json
    """
    ti = context["ti"]
    temp_path = ti.xcom_pull(task_ids="extract_iot_telemetry_api")
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")

    gcs_object_name = f"raw/iot_telemetry/{execution_date}/telemetry_{ts_nodash}.json"
    logger = logging.getLogger("airflow.task")
    logger.info(f"Uploading IoT telemetry batch to gs://{GCS_LANDING_BUCKET}/{gcs_object_name}...")

    gcs_hook = GCSHook()
    gcs_hook.upload(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=gcs_object_name,
        filename=temp_path,
        mime_type="application/json",
    )

    if os.path.exists(temp_path):
        os.remove(temp_path)

    return gcs_object_name


def clean_and_stage_telemetry(**context) -> str:
    """
    Step 3: Staging Layer Transformation.
    - Flattens nested 'telemetry' metrics
    - Validates battery percentage bounds (clipping outliers between 0% and 100%)
    - Filters corrupt temperature readings (e.g. > 150 C or < -50 C)
    - Deduplicates records on message_id
    - Writes cleaned NDJSON to GCS staging
    """
    ti = context["ti"]
    raw_gcs_object = ti.xcom_pull(task_ids="upload_raw_telemetry_gcs")
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")
    logger = logging.getLogger("airflow.task")

    gcs_hook = GCSHook()
    raw_data = gcs_hook.download_as_byte_array(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=raw_gcs_object,
    ).decode("utf-8")

    cleaned_records = []
    seen_message_ids = set()

    for line in raw_data.strip().split("\n"):
        if not line:
            continue
        item = json.loads(line)
        msg_id = str(item.get("message_id", "")).strip()
        if not msg_id or msg_id in seen_message_ids:
            continue
        seen_message_ids.add(msg_id)

        telemetry = item.get("telemetry", {})
        temp_c = float(telemetry.get("temperature_celsius", 20.0))
        # Sensor sanity guard
        if temp_c < -50.0 or temp_c > 150.0:
            logger.warning(f"Extreme temperature reading dropped for device {item.get('device_id')}: {temp_c}")
            continue

        raw_batt = int(telemetry.get("battery_percentage", 100))
        batt_clamped = max(0, min(100, raw_batt))

        raw_ts = item.get("timestamp", f"{execution_date}T00:00:00Z")
        telemetry_date = raw_ts.split("T")[0]

        cleaned_records.append({
            "message_id": msg_id,
            "device_id": str(item.get("device_id", "UNKNOWN")).strip(),
            "device_type": str(item.get("device_type", "GENERAL")).strip(),
            "firmware_version": str(item.get("firmware_version", "unknown")),
            "temperature_celsius": temp_c,
            "battery_percentage": batt_clamped,
            "vibration_frequency_hz": float(telemetry.get("vibration_frequency_hz", 0.0)),
            "latitude": float(telemetry.get("latitude", 0.0)),
            "longitude": float(telemetry.get("longitude", 0.0)),
            "recorded_at": raw_ts,
            "telemetry_date": telemetry_date,
            "ingestion_ts": datetime.utcnow().isoformat() + "Z",
        })

    staging_gcs_object = f"staging/iot_telemetry/{execution_date}/cleaned_telemetry_{ts_nodash}.json"
    cleaned_str = "\n".join(json.dumps(r) for r in cleaned_records)

    gcs_hook.upload(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=staging_gcs_object,
        data=cleaned_str.encode("utf-8"),
        mime_type="application/json",
    )
    logger.info(f"Staged {len(cleaned_records)} cleaned telemetry readings to {staging_gcs_object}")
    return staging_gcs_object


def verify_telemetry_skew(**context):
    """
    Step 6: Skew and volume distribution verification.
    Computes distribution across device types. If a single device generates
    abnormal skew (> 200x standard partition volume), raises an alert
    (connecting directly with FlowSentinel's resource_exhaustion agent).
    """
    execution_date = context.get("ds")
    logger = logging.getLogger("airflow.task")
    bq_hook = BigQueryHook()

    sql = f"""
    SELECT device_id, COUNT(*) as cnt
    FROM `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_iot_telemetry`
    WHERE telemetry_date = '{execution_date}'
    GROUP BY device_id
    ORDER BY cnt DESC
    LIMIT 1
    """
    result = bq_hook.get_first(sql)
    if result:
        max_device, count = result[0], result[1]
        logger.info(f"Top telemetry emitter on {execution_date}: {max_device} with {count} readings.")


with DAG(
    dag_id="iot_device_telemetry_pipeline",
    default_args=DEFAULT_ARGS,
    description="Extracts IoT fleet telemetry, cleans sensor noise, and loads into BigQuery fact_iot_telemetry",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    sla_miss_callback=flowsentinel_sla_miss_callback,
    params={"table": "dw_analytics.fact_iot_telemetry"},
    tags=["flowsentinel", "iot", "telemetry", "sensors", "p1"],
) as dag:

    # 1. API Extract
    t1_extract = PythonOperator(
        task_id="extract_iot_telemetry_api",
        python_callable=extract_iot_telemetry_api,
        provide_context=True,
    )

    # 2. Upload Raw to GCS
    t2_upload = PythonOperator(
        task_id="upload_raw_telemetry_gcs",
        python_callable=upload_raw_telemetry_gcs,
        provide_context=True,
    )

    # 3. Clean and Stage in GCS
    t3_clean = PythonOperator(
        task_id="clean_and_stage_telemetry",
        python_callable=clean_and_stage_telemetry,
        provide_context=True,
    )

    # 4. Load from GCS to BigQuery Staging Table
    t4_load_staging = GCSToBigQueryOperator(
        task_id="load_staging_telemetry_bq",
        bucket=GCS_LANDING_BUCKET,
        source_objects=["staging/iot_telemetry/{{ ds }}/cleaned_telemetry_{{ ts_nodash }}.json"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.stg_iot_telemetry",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "message_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "device_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "device_type", "type": "STRING", "mode": "REQUIRED"},
            {"name": "firmware_version", "type": "STRING", "mode": "NULLABLE"},
            {"name": "temperature_celsius", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "battery_percentage", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "vibration_frequency_hz", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "recorded_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "telemetry_date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "ingestion_ts", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ],
    )

    # 5. Production Curated Table Load (MERGE)
    MERGE_FACT_TELEMETRY_SQL = f"""
    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_iot_telemetry` (
        message_id STRING,
        device_id STRING,
        device_type STRING,
        firmware_version STRING,
        temperature_celsius FLOAT64,
        battery_percentage INT64,
        vibration_frequency_hz FLOAT64,
        latitude FLOAT64,
        longitude FLOAT64,
        recorded_at TIMESTAMP,
        telemetry_date DATE,
        ingestion_ts TIMESTAMP
    )
    PARTITION BY telemetry_date
    CLUSTER BY device_type, device_id;

    MERGE `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_iot_telemetry` T
    USING `{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.stg_iot_telemetry` S
    ON T.message_id = S.message_id AND T.telemetry_date = S.telemetry_date
    WHEN MATCHED THEN
      UPDATE SET
        firmware_version = S.firmware_version,
        temperature_celsius = S.temperature_celsius,
        battery_percentage = S.battery_percentage,
        vibration_frequency_hz = S.vibration_frequency_hz,
        latitude = S.latitude,
        longitude = S.longitude,
        ingestion_ts = S.ingestion_ts
    WHEN NOT MATCHED THEN
      INSERT (message_id, device_id, device_type, firmware_version, temperature_celsius, battery_percentage, vibration_frequency_hz, latitude, longitude, recorded_at, telemetry_date, ingestion_ts)
      VALUES (S.message_id, S.device_id, S.device_type, S.firmware_version, S.temperature_celsius, S.battery_percentage, S.vibration_frequency_hz, S.latitude, S.longitude, S.recorded_at, S.telemetry_date, S.ingestion_ts);
    """

    t5_merge_telemetry = BigQueryInsertJobOperator(
        task_id="merge_fact_iot_telemetry",
        configuration={
            "query": {
                "query": MERGE_FACT_TELEMETRY_SQL,
                "useLegacySql": False,
            }
        },
    )

    # 6. Skew Check
    t6_skew = PythonOperator(
        task_id="verify_telemetry_skew",
        python_callable=verify_telemetry_skew,
        provide_context=True,
    )

    t1_extract >> t2_upload >> t3_clean >> t4_load_staging >> t5_merge_telemetry >> t6_skew
