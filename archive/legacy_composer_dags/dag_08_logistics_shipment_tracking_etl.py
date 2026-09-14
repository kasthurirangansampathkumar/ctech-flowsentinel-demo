"""
DAG 8: Logistics & Carrier Shipment Tracking Pipeline
=====================================================
Pipeline ID: logistics_shipment_tracking_pipeline
Target Table: dw_analytics.fact_shipment_tracking

ETL Flow:
1. Extract delivery status, checkpoints, and ETAs from 3PL / Carrier APIs (FedEx/UPS/DHL).
2. Upload raw shipment tracking JSON to GCS Landing Zone (`raw/logistics_shipments/...`).
3. Staging transformation: Maps disparate carrier codes into standardized delivery stages,
   computes transit delay variance against estimated delivery dates, and resolves out-of-order events.
4. Load to BigQuery staging table (`staging.stg_shipment_tracking`).
5. Partition-filtered MERGE into production `dw_analytics.fact_shipment_tracking`
   partitioned by `shipment_date` and clustered by `carrier_code`, `current_status`.
6. Run Out-of-Order / Late-Arriving Records audit (maps to FlowSentinel's late_arriving_records agent).
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

STATUS_MAP = {
    "PU": "PICKED_UP",
    "IT": "IN_TRANSIT",
    "OD": "OUT_FOR_DELIVERY",
    "DL": "DELIVERED",
    "EX": "EXCEPTION",
}


def extract_carrier_tracking_api(**context) -> str:
    """
    Step 1: Poll carrier logistics tracking API for shipment milestone updates.
    Simulates real-world carrier status responses.
    """
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")
    logger = logging.getLogger("airflow.task")
    logger.info(f"Extracting carrier shipment milestones for date {execution_date}...")

    carriers = ["FEDEX", "UPS", "DHL_EXPRESS", "USPS"]
    raw_status_codes = ["PU", "IT", "OD", "DL", "EX"]
    sample_shipments = []

    for i in range(160):
        carrier = carriers[i % len(carriers)]
        raw_code = raw_status_codes[i % len(raw_status_codes)]
        tracking_num = f"TRK{carrier[:3]}{(i * 7391):08d}"

        est_delivery = f"{execution_date}T18:00:00Z"
        act_delivery = f"{execution_date}T20:15:00Z" if raw_code == "DL" else None

        sample_shipments.append({
            "tracking_number": tracking_num,
            "order_id": f"ORD-{execution_date}-{1000 + (i % 120)}",
            "carrier": carrier,
            "carrier_status_code": raw_code,
            "origin_hub": "ORD_AIRPORT_IL" if i % 2 == 0 else "LAX_AIRPORT_CA",
            "destination_city": "New York" if i % 3 == 0 else "Austin",
            "weight_kg": round(1.2 + (i % 15) * 0.45, 2),
            "estimated_delivery": est_delivery,
            "actual_delivery": act_delivery,
            "last_event_timestamp": f"{execution_date}T{(i % 24):02d}:30:00Z",
        })

    temp_path = f"/tmp/carrier_shipments_{ts_nodash}.json"
    with open(temp_path, "w", encoding="utf-8") as f:
        for s in sample_shipments:
            f.write(json.dumps(s) + "\n")

    logger.info(f"Extracted {len(sample_shipments)} carrier shipment records to {temp_path}")
    return temp_path


def upload_raw_shipments_gcs(**context) -> str:
    """
    Step 2: Upload raw shipment events to GCS Landing Zone.
    Destination: gs://<gcs_landing_bucket>/raw/logistics_shipments/<ds>/shipments_<ts_nodash>.json
    """
    ti = context["ti"]
    temp_path = ti.xcom_pull(task_ids="extract_carrier_tracking_api")
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")

    gcs_object_name = f"raw/logistics_shipments/{execution_date}/shipments_{ts_nodash}.json"
    logger = logging.getLogger("airflow.task")
    logger.info(f"Uploading carrier tracking payload to gs://{GCS_LANDING_BUCKET}/{gcs_object_name}...")

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


def clean_and_standardize_shipments(**context) -> str:
    """
    Step 3: Staging Layer Transformation.
    - Normalizes proprietary carrier status codes to standard lifecycle statuses
    - Calculates delivery delay variance in hours
    - Deduplicates records on tracking_number
    """
    ti = context["ti"]
    raw_gcs_object = ti.xcom_pull(task_ids="upload_raw_shipments_gcs")
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")
    logger = logging.getLogger("airflow.task")

    gcs_hook = GCSHook()
    raw_data = gcs_hook.download_as_byte_array(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=raw_gcs_object,
    ).decode("utf-8")

    cleaned = []
    seen_tracking = set()

    for line in raw_data.strip().split("\n"):
        if not line:
            continue
        item = json.loads(line)
        tracking_num = str(item.get("tracking_number", "")).strip()
        if not tracking_num or tracking_num in seen_tracking:
            continue
        seen_tracking.add(tracking_num)

        raw_code = item.get("carrier_status_code", "PU")
        canonical_status = STATUS_MAP.get(raw_code, "UNKNOWN")

        # Compute delay variance if delivered
        delay_hours = 0.0
        est_str = item.get("estimated_delivery")
        act_str = item.get("actual_delivery")
        if est_str and act_str:
            try:
                est_dt = datetime.fromisoformat(est_str.replace("Z", "+00:00"))
                act_dt = datetime.fromisoformat(act_str.replace("Z", "+00:00"))
                delay_hours = round((act_dt - est_dt).total_seconds() / 3600.0, 2)
            except Exception:
                delay_hours = 0.0

        cleaned.append({
            "tracking_number": tracking_num,
            "order_id": str(item.get("order_id", "UNKNOWN")).strip(),
            "carrier_code": str(item.get("carrier", "OTHER")).upper().strip(),
            "current_status": canonical_status,
            "origin_hub": str(item.get("origin_hub", "N/A")),
            "destination_city": str(item.get("destination_city", "N/A")),
            "weight_kg": float(item.get("weight_kg", 0.0)),
            "estimated_delivery": est_str,
            "actual_delivery": act_str,
            "delay_hours": delay_hours,
            "is_delayed": delay_hours > 0.0,
            "last_scan_timestamp": item.get("last_event_timestamp", f"{execution_date}T00:00:00Z"),
            "shipment_date": execution_date,
            "synced_at": datetime.utcnow().isoformat() + "Z",
        })

    staging_gcs_object = f"staging/logistics_shipments/{execution_date}/cleaned_shipments_{ts_nodash}.json"
    cleaned_payload = "\n".join(json.dumps(r) for r in cleaned)

    gcs_hook.upload(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=staging_gcs_object,
        data=cleaned_payload.encode("utf-8"),
        mime_type="application/json",
    )
    logger.info(f"Staged {len(cleaned)} normalized shipment records to {staging_gcs_object}")
    return staging_gcs_object


def verify_late_arriving_records(**context):
    """
    Step 6: Late-Arriving Records audit.
    Identifies scans belonging to historical partitions that landed out-of-order,
    reporting partition count for FlowSentinel's backfill agent.
    """
    execution_date = context.get("ds")
    logger = logging.getLogger("airflow.task")
    bq_hook = BigQueryHook()

    sql = f"""
    SELECT COUNT(*) as late_records
    FROM `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_shipment_tracking`
    WHERE shipment_date < DATE_SUB('{execution_date}', INTERVAL 2 DAY)
      AND DATE(synced_at) = '{execution_date}'
    """
    result = bq_hook.get_first(sql)
    late_count = result[0] if result else 0
    logger.info(f"Late-arriving shipment records detected on {execution_date}: {late_count}")


with DAG(
    dag_id="logistics_shipment_tracking_pipeline",
    default_args=DEFAULT_ARGS,
    description="Extracts 3PL carrier tracking, maps delivery milestones, and reconciles shipping delays",
    schedule_interval="0 7 * * *",  # Runs daily at 07:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    sla_miss_callback=flowsentinel_sla_miss_callback,
    params={"table": "dw_analytics.fact_shipment_tracking"},
    tags=["flowsentinel", "logistics", "shipping", "carriers", "p2"],
) as dag:

    # 1. API Extract
    t1_extract = PythonOperator(
        task_id="extract_carrier_tracking_api",
        python_callable=extract_carrier_tracking_api,
        provide_context=True,
    )

    # 2. Upload Raw to GCS
    t2_upload = PythonOperator(
        task_id="upload_raw_shipments_gcs",
        python_callable=upload_raw_shipments_gcs,
        provide_context=True,
    )

    # 3. Clean and Standardize in Staging
    t3_clean = PythonOperator(
        task_id="clean_and_standardize_shipments",
        python_callable=clean_and_standardize_shipments,
        provide_context=True,
    )

    # 4. Load from GCS Staging to BigQuery Staging Table
    t4_load_staging = GCSToBigQueryOperator(
        task_id="load_staging_shipments_bq",
        bucket=GCS_LANDING_BUCKET,
        source_objects=["staging/logistics_shipments/{{ ds }}/cleaned_shipments_{{ ts_nodash }}.json"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.stg_shipment_tracking",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "tracking_number", "type": "STRING", "mode": "REQUIRED"},
            {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "carrier_code", "type": "STRING", "mode": "REQUIRED"},
            {"name": "current_status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "origin_hub", "type": "STRING", "mode": "NULLABLE"},
            {"name": "destination_city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "weight_kg", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "estimated_delivery", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "actual_delivery", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "delay_hours", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "is_delayed", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "last_scan_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "shipment_date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "synced_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ],
    )

    # 5. Production Curated Table Load (MERGE)
    MERGE_FACT_SHIPMENT_SQL = f"""
    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_shipment_tracking` (
        tracking_number STRING,
        order_id STRING,
        carrier_code STRING,
        current_status STRING,
        origin_hub STRING,
        destination_city STRING,
        weight_kg FLOAT64,
        estimated_delivery TIMESTAMP,
        actual_delivery TIMESTAMP,
        delay_hours FLOAT64,
        is_delayed BOOLEAN,
        last_scan_timestamp TIMESTAMP,
        shipment_date DATE,
        synced_at TIMESTAMP
    )
    PARTITION BY shipment_date
    CLUSTER BY carrier_code, current_status;

    MERGE `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_shipment_tracking` T
    USING `{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.stg_shipment_tracking` S
    ON T.tracking_number = S.tracking_number AND T.shipment_date = S.shipment_date
    WHEN MATCHED THEN
      UPDATE SET
        current_status = S.current_status,
        destination_city = S.destination_city,
        weight_kg = S.weight_kg,
        estimated_delivery = S.estimated_delivery,
        actual_delivery = S.actual_delivery,
        delay_hours = S.delay_hours,
        is_delayed = S.is_delayed,
        last_scan_timestamp = S.last_scan_timestamp,
        synced_at = S.synced_at
    WHEN NOT MATCHED THEN
      INSERT (tracking_number, order_id, carrier_code, current_status, origin_hub, destination_city, weight_kg, estimated_delivery, actual_delivery, delay_hours, is_delayed, last_scan_timestamp, shipment_date, synced_at)
      VALUES (S.tracking_number, S.order_id, S.carrier_code, S.current_status, S.origin_hub, S.destination_city, S.weight_kg, S.estimated_delivery, S.actual_delivery, S.delay_hours, S.is_delayed, S.last_scan_timestamp, S.shipment_date, S.synced_at);
    """

    t5_merge_shipment = BigQueryInsertJobOperator(
        task_id="merge_fact_shipment_tracking",
        configuration={
            "query": {
                "query": MERGE_FACT_SHIPMENT_SQL,
                "useLegacySql": False,
            }
        },
    )

    # 6. Audit Late Records
    t6_audit_late = PythonOperator(
        task_id="verify_late_arriving_records",
        python_callable=verify_late_arriving_records,
        provide_context=True,
    )

    t1_extract >> t2_upload >> t3_clean >> t4_load_staging >> t5_merge_shipment >> t6_audit_late
