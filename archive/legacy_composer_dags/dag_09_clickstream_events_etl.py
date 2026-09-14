"""
DAG 9: Web & Mobile Clickstream Ingestion Pipeline
==================================================
Pipeline ID: clickstream_events_pipeline
Target Table: dw_analytics.fact_user_clickstream

ETL Flow:
1. Extract user interaction events (pageviews, searches, cart additions) from Event Collector API.
2. Upload raw event JSON batch to GCS Landing Zone (`raw/clickstream_events/...`).
3. Staging transformation: Anonymizes IP addresses (GDPR/privacy compliance), parses User-Agents
   into device category/browser, and standardizes event action taxonomy.
4. Load to BigQuery staging table (`staging.stg_clickstream_events`).
5. Partition-filtered MERGE into production `dw_analytics.fact_user_clickstream`
   partitioned by `event_date` and clustered by `device_category`, `event_type`.
6. Run Storage & Slot Quota usage monitor (maps to FlowSentinel's storage_quota agent).
"""

import json
import logging
import os
import re
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
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": flowsentinel_failure_callback,
}


def extract_clickstream_api(**context) -> str:
    """
    Step 1: Poll Event Collector API for user clickstream batch.
    Simulates high-volume web and mobile interaction telemetry.
    """
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")
    logger = logging.getLogger("airflow.task")
    logger.info(f"Extracting clickstream events for partition {execution_date}...")

    event_types = ["PAGE_VIEW", "PRODUCT_CLICK", "SEARCH_QUERY", "ADD_TO_CART", "CHECKOUT_INITIATED"]
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/128.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) Mobile/15E148",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) Safari/605.1.15",
    ]
    sample_events = []

    for i in range(300):
        etype = event_types[i % len(event_types)]
        ua = user_agents[i % len(user_agents)]
        user_id = f"USR-{500 + (i % 80)}" if i % 10 != 0 else None
        session_id = f"SES-{execution_date.replace('-', '')}-{100 + (i % 40)}"

        sample_events.append({
            "event_id": f"evt_{ts_nodash}_{2000 + i}",
            "session_id": session_id,
            "user_id": user_id,
            "event_type": etype,
            "url_path": f"/products/item-{(i % 25) + 1}" if "PRODUCT" in etype else "/search?q=wireless",
            "ip_address": f"198.51.100.{(i % 254) + 1}",
            "user_agent": ua,
            "event_timestamp": f"{execution_date}T{(i % 24):02d}:{(i % 60):02d}:{(i % 60):02d}Z",
        })

    temp_path = f"/tmp/clickstream_{ts_nodash}.json"
    with open(temp_path, "w", encoding="utf-8") as f:
        for ev in sample_events:
            f.write(json.dumps(ev) + "\n")

    logger.info(f"Extracted {len(sample_events)} clickstream events to {temp_path}")
    return temp_path


def upload_raw_clickstream_gcs(**context) -> str:
    """
    Step 2: Upload raw events batch to GCS Landing Zone.
    Destination: gs://<gcs_landing_bucket>/raw/clickstream_events/<ds>/events_<ts_nodash>.json
    """
    ti = context["ti"]
    temp_path = ti.xcom_pull(task_ids="extract_clickstream_api")
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")

    gcs_object_name = f"raw/clickstream_events/{execution_date}/events_{ts_nodash}.json"
    logger = logging.getLogger("airflow.task")
    logger.info(f"Uploading clickstream batch to gs://{GCS_LANDING_BUCKET}/{gcs_object_name}...")

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


def clean_and_sessionize_clickstream(**context) -> str:
    """
    Step 3: Staging Layer Transformation.
    - Anonymizes IP address (GDPR compliance: masks last octet)
    - Extracts device category (mobile vs desktop) from user-agent
    - Normalizes event types
    - Deduplicates on event_id
    """
    ti = context["ti"]
    raw_gcs_object = ti.xcom_pull(task_ids="upload_raw_clickstream_gcs")
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")
    logger = logging.getLogger("airflow.task")

    gcs_hook = GCSHook()
    raw_data = gcs_hook.download_as_byte_array(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=raw_gcs_object,
    ).decode("utf-8")

    cleaned = []
    seen_events = set()

    for line in raw_data.strip().split("\n"):
        if not line:
            continue
        item = json.loads(line)
        evt_id = str(item.get("event_id", "")).strip()
        if not evt_id or evt_id in seen_events:
            continue
        seen_events.add(evt_id)

        # Anonymize IP (mask last octet: 198.51.100.45 -> 198.51.100.0)
        raw_ip = str(item.get("ip_address", "0.0.0.0"))
        ip_parts = raw_ip.split(".")
        anonymized_ip = f"{ip_parts[0]}.{ip_parts[1]}.{ip_parts[2]}.0" if len(ip_parts) == 4 else "0.0.0.0"

        # Categorize device
        ua = str(item.get("user_agent", "")).lower()
        if "iphone" in ua or "android" in ua or "mobile" in ua:
            device_category = "mobile"
        elif "ipad" in ua or "tablet" in ua:
            device_category = "tablet"
        else:
            device_category = "desktop"

        raw_ts = item.get("event_timestamp", f"{execution_date}T00:00:00Z")
        event_date = raw_ts.split("T")[0]

        cleaned.append({
            "event_id": evt_id,
            "session_id": str(item.get("session_id", "ANON_SESSION")).strip(),
            "user_id": str(item.get("user_id") or "GUEST").strip(),
            "event_type": str(item.get("event_type", "UNKNOWN")).upper().strip(),
            "url_path": str(item.get("url_path", "/")),
            "anonymized_ip": anonymized_ip,
            "device_category": device_category,
            "event_timestamp": raw_ts,
            "event_date": event_date,
            "ingested_at": datetime.utcnow().isoformat() + "Z",
        })

    staging_gcs_object = f"staging/clickstream_events/{execution_date}/cleaned_events_{ts_nodash}.json"
    cleaned_payload = "\n".join(json.dumps(r) for r in cleaned)

    gcs_hook.upload(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=staging_gcs_object,
        data=cleaned_payload.encode("utf-8"),
        mime_type="application/json",
    )
    logger.info(f"Staged {len(cleaned)} sanitized clickstream events to {staging_gcs_object}")
    return staging_gcs_object


def verify_storage_quota_baseline(**context):
    """
    Step 6: Storage and partition volume check.
    Monitors daily clickstream volume to prevent unexpected slot spikes or storage quota breaches.
    """
    execution_date = context.get("ds")
    logger = logging.getLogger("airflow.task")
    bq_hook = BigQueryHook()

    sql = f"""
    SELECT COUNT(*) as event_count, COUNT(DISTINCT session_id) as session_count
    FROM `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_user_clickstream`
    WHERE event_date = '{execution_date}'
    """
    result = bq_hook.get_first(sql)
    if result:
        events, sessions = result[0], result[1]
        logger.info(f"Clickstream volume for {execution_date}: {events} events across {sessions} sessions.")


with DAG(
    dag_id="clickstream_events_pipeline",
    default_args=DEFAULT_ARGS,
    description="Extracts web/app clickstream, anonymizes IPs, classifies devices, and loads BigQuery",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    sla_miss_callback=flowsentinel_sla_miss_callback,
    params={"table": "dw_analytics.fact_user_clickstream"},
    tags=["flowsentinel", "clickstream", "web", "analytics", "p1"],
) as dag:

    # 1. API Extract
    t1_extract = PythonOperator(
        task_id="extract_clickstream_api",
        python_callable=extract_clickstream_api,
        provide_context=True,
    )

    # 2. Upload Raw to GCS
    t2_upload = PythonOperator(
        task_id="upload_raw_clickstream_gcs",
        python_callable=upload_raw_clickstream_gcs,
        provide_context=True,
    )

    # 3. Clean and Anonymize in Staging
    t3_clean = PythonOperator(
        task_id="clean_and_sessionize_clickstream",
        python_callable=clean_and_sessionize_clickstream,
        provide_context=True,
    )

    # 4. Load from GCS Staging to BigQuery Staging Table
    t4_load_staging = GCSToBigQueryOperator(
        task_id="load_staging_clickstream_bq",
        bucket=GCS_LANDING_BUCKET,
        source_objects=["staging/clickstream_events/{{ ds }}/cleaned_events_{{ ts_nodash }}.json"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.stg_clickstream_events",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "session_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "event_type", "type": "STRING", "mode": "REQUIRED"},
            {"name": "url_path", "type": "STRING", "mode": "NULLABLE"},
            {"name": "anonymized_ip", "type": "STRING", "mode": "NULLABLE"},
            {"name": "device_category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "event_date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ],
    )

    # 5. Production Curated Table Load (MERGE)
    MERGE_FACT_CLICKSTREAM_SQL = f"""
    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_user_clickstream` (
        event_id STRING,
        session_id STRING,
        user_id STRING,
        event_type STRING,
        url_path STRING,
        anonymized_ip STRING,
        device_category STRING,
        event_timestamp TIMESTAMP,
        event_date DATE,
        ingested_at TIMESTAMP
    )
    PARTITION BY event_date
    CLUSTER BY device_category, event_type;

    MERGE `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_user_clickstream` T
    USING `{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.stg_clickstream_events` S
    ON T.event_id = S.event_id AND T.event_date = S.event_date
    WHEN MATCHED THEN
      UPDATE SET
        session_id = S.session_id,
        user_id = S.user_id,
        event_type = S.event_type,
        url_path = S.url_path,
        anonymized_ip = S.anonymized_ip,
        device_category = S.device_category,
        ingested_at = S.ingested_at
    WHEN NOT MATCHED THEN
      INSERT (event_id, session_id, user_id, event_type, url_path, anonymized_ip, device_category, event_timestamp, event_date, ingestion_at)
      VALUES (S.event_id, S.session_id, S.user_id, S.event_type, S.url_path, S.anonymized_ip, S.device_category, S.event_timestamp, S.event_date, S.ingested_at);
    """

    t5_merge_clickstream = BigQueryInsertJobOperator(
        task_id="merge_fact_user_clickstream",
        configuration={
            "query": {
                "query": MERGE_FACT_CLICKSTREAM_SQL,
                "useLegacySql": False,
            }
        },
    )

    # 6. Monitor Quota Baseline
    t6_quota = PythonOperator(
        task_id="verify_storage_quota_baseline",
        python_callable=verify_storage_quota_baseline,
        provide_context=True,
    )

    t1_extract >> t2_upload >> t3_clean >> t4_load_staging >> t5_merge_clickstream >> t6_quota
