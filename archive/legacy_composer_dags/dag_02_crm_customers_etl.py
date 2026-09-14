"""
DAG 2: Vendor CRM Customer Profiles Sync Pipeline
=================================================
Pipeline ID: crm_customer_sync_pipeline
Target Table: dw_analytics.dim_customers
GCS Landing Path: vendor_crm/customer_profiles_{{ ds }}.json

ETL Flow:
1. Extract daily customer profiles from CRM Vendor API.
2. Store raw payload at `gs://<bucket>/vendor_crm/customer_profiles_{{ ds }}.json`
   (matching FlowSentinel's watched feed SLA).
3. Staging schema reconciliation: Reconciles schema drift (e.g. `cust_id` vs
   `customer_identifier_v2`), normalizes emails, formats phone numbers.
4. Load to BigQuery staging table (`staging.stg_crm_customers`).
5. Execute an idempotent MERGE into dimensional table (`dw_analytics.dim_customers`).
6. Run Data Quality uniqueness test (detects duplicate IDs for DQ triage agent).
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
    "retry_delay": timedelta(minutes=3),
    "sla": timedelta(hours=2),  # Must complete within 2 hours (08:00 UTC SLA)
    "on_failure_callback": flowsentinel_failure_callback,
}


def extract_crm_profiles_api(**context) -> str:
    """
    Step 1: Extract customer profiles from vendor CRM API.
    Simulates export endpoint return with pagination.
    """
    execution_date = context.get("ds")
    logger = logging.getLogger("airflow.task")
    logger.info(f"Connecting to CRM API endpoint for customer batch date {execution_date}...")

    # Simulated profile records reflecting real enterprise CRM records
    # Also includes schema variations to exercise schema drift resilience
    sample_profiles = [
        {
            # Handles both legacy and drifted schema field names
            "customer_identifier_v2": f"CUST-{200 + i}",
            "cust_id": None,
            "full_name": f"Customer Name {i}",
            "email_address": f"user_{i}@example.com" if i % 15 != 0 else f"USER_{i}@EXAMPLE.COM  ",
            "tier_level": "PLATINUM" if i % 10 == 0 else ("GOLD" if i % 3 == 0 else "STANDARD"),
            "signup_country": "US" if i % 2 == 0 else "GB",
            "lifetime_value": 150.0 + (i * 25.5),
            "updated_at": f"{execution_date}T04:30:00Z",
        }
        for i in range(150)
    ]

    temp_file = f"/tmp/crm_export_{execution_date}.json"
    with open(temp_file, "w", encoding="utf-8") as f:
        for profile in sample_profiles:
            f.write(json.dumps(profile) + "\n")

    logger.info(f"Extracted {len(sample_profiles)} CRM customer profiles to {temp_file}")
    return temp_file


def upload_raw_crm_to_gcs(**context) -> str:
    """
    Step 2: Upload raw feed to watched GCS location.
    Destination: gs://<gcs_landing_bucket>/vendor_crm/customer_profiles_<ds>.json
    Matches FlowSentinel's feed_sentinel.py SLA monitor.
    """
    ti = context["ti"]
    temp_file = ti.xcom_pull(task_ids="extract_crm_profiles_api")
    execution_date = context.get("ds")

    gcs_object_name = f"vendor_crm/customer_profiles_{execution_date}.json"
    logger = logging.getLogger("airflow.task")
    logger.info(f"Uploading CRM raw feed to gs://{GCS_LANDING_BUCKET}/{gcs_object_name}...")

    gcs_hook = GCSHook()
    gcs_hook.upload(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=gcs_object_name,
        filename=temp_file,
        mime_type="application/json",
    )

    if os.path.exists(temp_file):
        os.remove(temp_file)

    return gcs_object_name


def clean_and_reconcile_schema(**context) -> str:
    """
    Step 3: Staging schema reconciliation.
    Applies defensive transformation:
    - Coalesces legacy 'cust_id' with drifted 'customer_identifier_v2'
    - Lowercases and strips email whitespace
    - Formats tier level and defaults
    """
    ti = context["ti"]
    raw_gcs_object = ti.xcom_pull(task_ids="upload_raw_crm_to_gcs")
    execution_date = context.get("ds")
    logger = logging.getLogger("airflow.task")

    gcs_hook = GCSHook()
    raw_bytes = gcs_hook.download_as_byte_array(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=raw_gcs_object,
    )
    lines = raw_bytes.decode("utf-8").strip().split("\n")

    cleaned_profiles = []
    for line in lines:
        if not line:
            continue
        data = json.loads(line)

        # Reconcile customer ID drift
        customer_id = (
            data.get("customer_identifier_v2")
            or data.get("cust_id")
            or data.get("customer_id")
        )
        if not customer_id:
            continue

        raw_email = str(data.get("email_address", "")).strip().lower()
        cleaned_profiles.append({
            "customer_id": str(customer_id).strip(),
            "full_name": str(data.get("full_name", "")).strip(),
            "email_address": raw_email,
            "tier_level": str(data.get("tier_level", "STANDARD")).upper(),
            "signup_country": str(data.get("signup_country", "US")).upper(),
            "lifetime_value": float(data.get("lifetime_value", 0.0)),
            "updated_at": data.get("updated_at", f"{execution_date}T00:00:00Z"),
            "sync_date": execution_date,
        })

    staging_gcs_object = f"staging/crm_customers/{execution_date}/cleaned_profiles.json"
    out_payload = "\n".join(json.dumps(p) for p in cleaned_profiles)

    gcs_hook.upload(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=staging_gcs_object,
        data=out_payload.encode("utf-8"),
        mime_type="application/json",
    )
    logger.info(f"Staged {len(cleaned_profiles)} reconciled customer profiles to {staging_gcs_object}")
    return staging_gcs_object


def validate_customer_uniqueness(**context):
    """
    Step 6: Data Quality uniqueness assertion.
    Asserts no duplicate customer_id records exist in staging.
    If duplicates appear (e.g. Kafka replay or vendor bug), raises DQ exception.
    """
    execution_date = context.get("ds")
    logger = logging.getLogger("airflow.task")
    bq_hook = BigQueryHook()

    sql = f"""
    SELECT customer_id, COUNT(*) as cnt
    FROM `{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.stg_crm_customers`
    GROUP BY customer_id
    HAVING cnt > 1
    LIMIT 5
    """
    records = bq_hook.get_records(sql)
    if records:
        duplicate_count = len(records)
        example_dup = records[0][0]
        raise ValueError(
            f"Data Quality check failed: found duplicate customer IDs in stg_crm_customers "
            f"(found {duplicate_count} duplicates, e.g. '{example_dup}')."
        )
    logger.info("DQ Check passed: All customer IDs are unique.")


with DAG(
    dag_id="crm_customer_sync_pipeline",
    default_args=DEFAULT_ARGS,
    description="Synchronizes CRM customer profiles, reconciles schema drift, and upserts dim_customers",
    schedule_interval="0 6 * * *",  # Runs daily at 06:00 UTC (SLA deadline 08:00 UTC)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    sla_miss_callback=flowsentinel_sla_miss_callback,
    params={"table": "dw_analytics.dim_customers"},
    tags=["flowsentinel", "crm", "customers", "dimension", "p2"],
) as dag:

    # 1. API Extract
    t1_extract = PythonOperator(
        task_id="extract_crm_profiles_api",
        python_callable=extract_crm_profiles_api,
        provide_context=True,
    )

    # 2. Upload Raw to GCS watched SLA path
    t2_upload = PythonOperator(
        task_id="upload_raw_crm_to_gcs",
        python_callable=upload_raw_crm_to_gcs,
        provide_context=True,
    )

    # 3. Clean and Reconcile Schema in GCS Staging
    t3_clean = PythonOperator(
        task_id="clean_and_reconcile_schema",
        python_callable=clean_and_reconcile_schema,
        provide_context=True,
    )

    # 4. Load from GCS Staging into BigQuery Staging Table
    t4_load_staging = GCSToBigQueryOperator(
        task_id="load_staging_crm_bq",
        bucket=GCS_LANDING_BUCKET,
        source_objects=["staging/crm_customers/{{ ds }}/cleaned_profiles.json"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.stg_crm_customers",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "full_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "email_address", "type": "STRING", "mode": "NULLABLE"},
            {"name": "tier_level", "type": "STRING", "mode": "NULLABLE"},
            {"name": "signup_country", "type": "STRING", "mode": "NULLABLE"},
            {"name": "lifetime_value", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "sync_date", "type": "DATE", "mode": "REQUIRED"},
        ],
    )

    # 5. DQ uniqueness verification before merge
    t5_validate_dq = PythonOperator(
        task_id="validate_customer_uniqueness",
        python_callable=validate_customer_uniqueness,
        provide_context=True,
    )

    # 6. Upsert Dimension in BigQuery (Type 1 SCD)
    UPSERT_DIM_CUSTOMERS_SQL = f"""
    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.dim_customers` (
        customer_id STRING,
        full_name STRING,
        email_address STRING,
        tier_level STRING,
        signup_country STRING,
        lifetime_value FLOAT64,
        updated_at TIMESTAMP,
        effective_start_date DATE,
        is_active BOOLEAN
    )
    CLUSTER BY customer_id, tier_level;

    MERGE `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.dim_customers` T
    USING `{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.stg_crm_customers` S
    ON T.customer_id = S.customer_id
    WHEN MATCHED THEN
      UPDATE SET
        full_name = S.full_name,
        email_address = S.email_address,
        tier_level = S.tier_level,
        signup_country = S.signup_country,
        lifetime_value = S.lifetime_value,
        updated_at = S.updated_at,
        effective_start_date = S.sync_date,
        is_active = TRUE
    WHEN NOT MATCHED THEN
      INSERT (customer_id, full_name, email_address, tier_level, signup_country, lifetime_value, updated_at, effective_start_date, is_active)
      VALUES (S.customer_id, S.full_name, S.email_address, S.tier_level, S.signup_country, S.lifetime_value, S.updated_at, S.sync_date, TRUE);
    """

    t6_merge_dimension = BigQueryInsertJobOperator(
        task_id="merge_dim_customers",
        configuration={
            "query": {
                "query": UPSERT_DIM_CUSTOMERS_SQL,
                "useLegacySql": False,
            }
        },
    )

    t1_extract >> t2_upload >> t3_clean >> t4_load_staging >> t5_validate_dq >> t6_merge_dimension
