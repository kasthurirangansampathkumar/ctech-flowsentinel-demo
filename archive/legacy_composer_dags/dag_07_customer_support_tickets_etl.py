"""
DAG 7: Customer Support Tickets & CSAT Ingestion Pipeline
=========================================================
Pipeline ID: customer_support_tickets_pipeline
Target Table: dw_analytics.fact_support_tickets

ETL Flow:
1. Extract customer support tickets, status updates, and CSAT scores from Helpdesk REST API (Zendesk/Freshdesk).
2. Upload raw support ticket JSON to GCS Landing Zone (`raw/support_tickets/...`).
3. Staging transformation: Mask PII from ticket descriptions, calculate resolution time in minutes,
   validate CSAT score ranges (1-5), and standardize ticket priority levels.
4. Load to BigQuery staging table (`staging.stg_support_tickets`).
5. Partition-filtered MERGE into production `dw_analytics.fact_support_tickets`
   partitioned by `ticket_created_date` and clustered by `priority`, `channel`.
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


def extract_support_tickets_api(**context) -> str:
    """
    Step 1: Extract customer support tickets from Helpdesk REST API.
    Simulates export of open and closed tickets for the logical execution date.
    """
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")
    logger = logging.getLogger("airflow.task")
    logger.info(f"Extracting helpdesk customer tickets for {execution_date}...")

    channels = ["email", "live_chat", "phone", "web_portal"]
    priorities = ["low", "normal", "high", "urgent"]
    sample_tickets = []

    for i in range(130):
        chan = channels[i % len(channels)]
        prio = priorities[i % len(priorities)]
        created_hour = (i % 18) + 6
        resolved_hour = min(23, created_hour + ((i % 5) + 1))

        sample_tickets.append({
            "ticket_id": f"TCK-{execution_date.replace('-', '')}-{2000 + i}",
            "customer_id": f"CUST-{200 + (i % 60)}",
            "agent_id": f"AGT-{10 + (i % 12)}",
            "channel": chan,
            "priority": prio,
            "subject": f"Inquiry regarding shipment and billing ref #{1000 + i}",
            "raw_text": f"Customer called regarding order {1000 + i}. Contact phone 555-019-{i:02d} was verified.",
            "status": "CLOSED" if i % 6 != 0 else "PENDING",
            "csat_score": (i % 5) + 1 if i % 6 != 0 else None,
            "created_at": f"{execution_date}T{created_hour:02d}:10:00Z",
            "resolved_at": f"{execution_date}T{resolved_hour:02d}:45:00Z" if i % 6 != 0 else None,
        })

    temp_path = f"/tmp/support_tickets_{ts_nodash}.json"
    with open(temp_path, "w", encoding="utf-8") as f:
        for t in sample_tickets:
            f.write(json.dumps(t) + "\n")

    logger.info(f"Extracted {len(sample_tickets)} helpdesk tickets to {temp_path}")
    return temp_path


def upload_raw_tickets_gcs(**context) -> str:
    """
    Step 2: Upload raw tickets payload to GCS Landing Zone.
    Destination: gs://<gcs_landing_bucket>/raw/support_tickets/<ds>/tickets_<ts_nodash>.json
    """
    ti = context["ti"]
    temp_path = ti.xcom_pull(task_ids="extract_support_tickets_api")
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")

    gcs_object_name = f"raw/support_tickets/{execution_date}/tickets_{ts_nodash}.json"
    logger = logging.getLogger("airflow.task")
    logger.info(f"Uploading tickets payload to gs://{GCS_LANDING_BUCKET}/{gcs_object_name}...")

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


def clean_and_mask_support_tickets(**context) -> str:
    """
    Step 3: Staging Layer Transformation.
    - Masks PII (redacts phone number patterns)
    - Computes resolution_time_minutes between created_at and resolved_at
    - Validates CSAT bounds
    - Normalizes status and priority codes
    """
    ti = context["ti"]
    raw_gcs_object = ti.xcom_pull(task_ids="upload_raw_tickets_gcs")
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")
    logger = logging.getLogger("airflow.task")

    gcs_hook = GCSHook()
    raw_data = gcs_hook.download_as_byte_array(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=raw_gcs_object,
    ).decode("utf-8")

    cleaned = []
    seen_ids = set()

    for line in raw_data.strip().split("\n"):
        if not line:
            continue
        item = json.loads(line)
        ticket_id = str(item.get("ticket_id", "")).strip()
        if not ticket_id or ticket_id in seen_ids:
            continue
        seen_ids.add(ticket_id)

        # Mask PII (phone numbers)
        raw_text = str(item.get("raw_text", ""))
        sanitized_text = re.sub(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b", "[REDACTED_PHONE]", raw_text)

        # Compute resolution duration
        created_str = item.get("created_at")
        resolved_str = item.get("resolved_at")
        resolution_mins = None
        if created_str and resolved_str:
            try:
                c_dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
                r_dt = datetime.fromisoformat(resolved_str.replace("Z", "+00:00"))
                resolution_mins = max(0, int((r_dt - c_dt).total_seconds() / 60))
            except Exception:
                resolution_mins = None

        raw_csat = item.get("csat_score")
        csat_score = int(raw_csat) if raw_csat and 1 <= int(raw_csat) <= 5 else None

        cleaned.append({
            "ticket_id": ticket_id,
            "customer_id": str(item.get("customer_id", "UNKNOWN")).strip(),
            "agent_id": str(item.get("agent_id", "UNASSIGNED")).strip(),
            "channel": str(item.get("channel", "web_portal")).lower().strip(),
            "priority": str(item.get("priority", "NORMAL")).upper().strip(),
            "sanitized_notes": sanitized_text,
            "status": str(item.get("status", "OPEN")).upper().strip(),
            "csat_score": csat_score,
            "resolution_time_minutes": resolution_mins,
            "created_at": created_str,
            "resolved_at": resolved_str,
            "ticket_created_date": created_str.split("T")[0] if created_str else execution_date,
            "synced_at": datetime.utcnow().isoformat() + "Z",
        })

    staging_gcs_object = f"staging/support_tickets/{execution_date}/cleaned_tickets_{ts_nodash}.json"
    cleaned_payload = "\n".join(json.dumps(r) for r in cleaned)

    gcs_hook.upload(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=staging_gcs_object,
        data=cleaned_payload.encode("utf-8"),
        mime_type="application/json",
    )
    logger.info(f"Staged {len(cleaned)} sanitized support tickets to {staging_gcs_object}")
    return staging_gcs_object


with DAG(
    dag_id="customer_support_tickets_pipeline",
    default_args=DEFAULT_ARGS,
    description="Extracts helpdesk tickets, redacts PII, computes resolution time, and syncs BigQuery",
    schedule_interval="0 3 * * *",  # Runs daily at 03:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    sla_miss_callback=flowsentinel_sla_miss_callback,
    params={"table": "dw_analytics.fact_support_tickets"},
    tags=["flowsentinel", "support", "zendesk", "csat", "p2"],
) as dag:

    # 1. Extract API
    t1_extract = PythonOperator(
        task_id="extract_support_tickets_api",
        python_callable=extract_support_tickets_api,
        provide_context=True,
    )

    # 2. Upload Raw to GCS
    t2_upload = PythonOperator(
        task_id="upload_raw_tickets_gcs",
        python_callable=upload_raw_tickets_gcs,
        provide_context=True,
    )

    # 3. Clean and Mask in Staging
    t3_clean = PythonOperator(
        task_id="clean_and_mask_support_tickets",
        python_callable=clean_and_mask_support_tickets,
        provide_context=True,
    )

    # 4. Load from GCS Staging to BigQuery Staging Table
    t4_load_staging = GCSToBigQueryOperator(
        task_id="load_staging_tickets_bq",
        bucket=GCS_LANDING_BUCKET,
        source_objects=["staging/support_tickets/{{ ds }}/cleaned_tickets_{{ ts_nodash }}.json"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.stg_support_tickets",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "ticket_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "agent_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "channel", "type": "STRING", "mode": "NULLABLE"},
            {"name": "priority", "type": "STRING", "mode": "NULLABLE"},
            {"name": "sanitized_notes", "type": "STRING", "mode": "NULLABLE"},
            {"name": "status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "csat_score", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "resolution_time_minutes", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "resolved_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "ticket_created_date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "synced_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ],
    )

    # 5. Production Curated Table Load (MERGE)
    MERGE_FACT_SUPPORT_SQL = f"""
    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_support_tickets` (
        ticket_id STRING,
        customer_id STRING,
        agent_id STRING,
        channel STRING,
        priority STRING,
        sanitized_notes STRING,
        status STRING,
        csat_score INT64,
        resolution_time_minutes INT64,
        created_at TIMESTAMP,
        resolved_at TIMESTAMP,
        ticket_created_date DATE,
        synced_at TIMESTAMP
    )
    PARTITION BY ticket_created_date
    CLUSTER BY priority, channel;

    MERGE `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_support_tickets` T
    USING `{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.stg_support_tickets` S
    ON T.ticket_id = S.ticket_id AND T.ticket_created_date = S.ticket_created_date
    WHEN MATCHED THEN
      UPDATE SET
        agent_id = S.agent_id,
        channel = S.channel,
        priority = S.priority,
        sanitized_notes = S.sanitized_notes,
        status = S.status,
        csat_score = S.csat_score,
        resolution_time_minutes = S.resolution_time_minutes,
        resolved_at = S.resolved_at,
        synced_at = S.synced_at
    WHEN NOT MATCHED THEN
      INSERT (ticket_id, customer_id, agent_id, channel, priority, sanitized_notes, status, csat_score, resolution_time_minutes, created_at, resolved_at, ticket_created_date, synced_at)
      VALUES (S.ticket_id, S.customer_id, S.agent_id, S.channel, S.priority, S.sanitized_notes, S.status, S.csat_score, S.resolution_time_minutes, S.created_at, S.resolved_at, S.ticket_created_date, S.synced_at);
    """

    t5_merge_support = BigQueryInsertJobOperator(
        task_id="merge_fact_support_tickets",
        configuration={
            "query": {
                "query": MERGE_FACT_SUPPORT_SQL,
                "useLegacySql": False,
            }
        },
    )

    t1_extract >> t2_upload >> t3_clean >> t4_load_staging >> t5_merge_support
