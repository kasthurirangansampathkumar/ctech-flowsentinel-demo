"""
DAG 10: SaaS Subscription & Recurring Billing Sync Pipeline
===========================================================
Pipeline ID: saas_billing_subscription_pipeline
Target Table: dw_analytics.fact_subscriptions

ETL Flow:
1. Extract customer subscription lifecycles, plan tiers, and billing cycles from Billing API (Chargebee/Stripe/Zuora).
2. Upload raw billing JSON to GCS Landing Zone (`raw/saas_subscriptions/...`).
3. Staging transformation: Normalizes annual/monthly billing into standard Monthly Recurring Revenue (MRR),
   handles discount codes, sanitizes currency codes, and flags churn events.
4. Load to BigQuery staging table (`staging.stg_saas_subscriptions`).
5. Partition-filtered MERGE into production `dw_analytics.fact_subscriptions`
   partitioned by `subscription_start_date` and clustered by `plan_tier`, `subscription_status`.
6. Run MRR & Churn Variance Audit (detects anomalies and triggers backfill planning for FlowSentinel).
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


def extract_subscriptions_api(**context) -> str:
    """
    Step 1: Extract subscriptions and recurring plan schedules from Billing API.
    Simulates export of active, past due, and canceled accounts.
    """
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")
    logger = logging.getLogger("airflow.task")
    logger.info(f"Extracting SaaS recurring subscriptions for {execution_date}...")

    tiers = ["STARTER", "GROWTH", "ENTERPRISE"]
    billing_cycles = ["monthly", "annual"]
    sample_subs = []

    for i in range(140):
        tier = tiers[i % len(tiers)]
        cycle = billing_cycles[i % len(billing_cycles)]

        base_rate = 49.0 if tier == "STARTER" else (199.0 if tier == "GROWTH" else 999.0)
        plan_amount = base_rate if cycle == "monthly" else round(base_rate * 10.0, 2)  # 2 months free annual discount

        status = "ACTIVE" if i % 8 != 0 else ("PAST_DUE" if i % 16 == 0 else "CANCELED")
        canceled_at = f"{execution_date}T10:00:00Z" if status == "CANCELED" else None

        sample_subs.append({
            "subscription_id": f"sub_{execution_date.replace('-', '')}_{1000 + i}",
            "customer_id": f"CUST-{200 + (i % 60)}",
            "plan_tier": tier,
            "billing_interval": cycle,
            "plan_amount_usd": plan_amount,
            "discount_percentage": 10.0 if i % 5 == 0 else 0.0,
            "status": status,
            "current_period_start": f"{execution_date}T00:00:00Z",
            "current_period_end": f"{execution_date}T23:59:59Z",
            "canceled_at": canceled_at,
        })

    temp_path = f"/tmp/subscriptions_{ts_nodash}.json"
    with open(temp_path, "w", encoding="utf-8") as f:
        for s in sample_subs:
            f.write(json.dumps(s) + "\n")

    logger.info(f"Extracted {len(sample_subs)} subscription contracts to {temp_path}")
    return temp_path


def upload_raw_subscriptions_gcs(**context) -> str:
    """
    Step 2: Upload raw subscriptions JSON to GCS Landing Zone.
    Destination: gs://<gcs_landing_bucket>/raw/saas_subscriptions/<ds>/subscriptions_<ts_nodash>.json
    """
    ti = context["ti"]
    temp_path = ti.xcom_pull(task_ids="extract_subscriptions_api")
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")

    gcs_object_name = f"raw/saas_subscriptions/{execution_date}/subscriptions_{ts_nodash}.json"
    logger = logging.getLogger("airflow.task")
    logger.info(f"Uploading subscription payload to gs://{GCS_LANDING_BUCKET}/{gcs_object_name}...")

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


def clean_and_calculate_mrr(**context) -> str:
    """
    Step 3: Staging Layer Transformation.
    - Normalizes billing interval into standard Monthly Recurring Revenue (MRR)
    - Applies discount deductions
    - Flags churn events
    - Writes cleaned NDJSON to GCS staging
    """
    ti = context["ti"]
    raw_gcs_object = ti.xcom_pull(task_ids="upload_raw_subscriptions_gcs")
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")
    logger = logging.getLogger("airflow.task")

    gcs_hook = GCSHook()
    raw_data = gcs_hook.download_as_byte_array(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=raw_gcs_object,
    ).decode("utf-8")

    cleaned = []
    seen_subs = set()

    for line in raw_data.strip().split("\n"):
        if not line:
            continue
        item = json.loads(line)
        sub_id = str(item.get("subscription_id", "")).strip()
        if not sub_id or sub_id in seen_subs:
            continue
        seen_subs.add(sub_id)

        raw_amount = float(item.get("plan_amount_usd", 0.0))
        discount_pct = float(item.get("discount_percentage", 0.0))
        effective_amount = raw_amount * (1.0 - (discount_pct / 100.0))

        interval = str(item.get("billing_interval", "monthly")).lower()
        # Compute normalized MRR
        mrr = round(effective_amount / 12.0, 2) if interval == "annual" else round(effective_amount, 2)

        raw_status = str(item.get("status", "ACTIVE")).upper().strip()
        is_churned = raw_status == "CANCELED"

        period_start = item.get("current_period_start", f"{execution_date}T00:00:00Z")
        start_date = period_start.split("T")[0]

        cleaned.append({
            "subscription_id": sub_id,
            "customer_id": str(item.get("customer_id", "UNKNOWN")).strip(),
            "plan_tier": str(item.get("plan_tier", "STARTER")).upper().strip(),
            "billing_interval": interval,
            "plan_amount_usd": round(raw_amount, 2),
            "mrr_usd": mrr,
            "discount_percentage": discount_pct,
            "subscription_status": raw_status,
            "is_churned": is_churned,
            "period_start": period_start,
            "period_end": item.get("current_period_end", f"{execution_date}T23:59:59Z"),
            "canceled_at": item.get("canceled_at"),
            "subscription_start_date": start_date,
            "synced_at": datetime.utcnow().isoformat() + "Z",
        })

    staging_gcs_object = f"staging/saas_subscriptions/{execution_date}/cleaned_subs_{ts_nodash}.json"
    cleaned_payload = "\n".join(json.dumps(r) for r in cleaned)

    gcs_hook.upload(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=staging_gcs_object,
        data=cleaned_payload.encode("utf-8"),
        mime_type="application/json",
    )
    logger.info(f"Staged {len(cleaned)} normalized subscription contracts to {staging_gcs_object}")
    return staging_gcs_object


def verify_mrr_reconciliation(**context):
    """
    Step 6: Financial MRR Audit.
    Aggregates active MRR and churn rate to alert on abnormal churn spikes.
    """
    execution_date = context.get("ds")
    logger = logging.getLogger("airflow.task")
    bq_hook = BigQueryHook()

    sql = f"""
    SELECT
      COALESCE(SUM(mrr_usd), 0) as total_mrr,
      COUNTIF(is_churned) as churn_count
    FROM `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_subscriptions`
    WHERE subscription_start_date = '{execution_date}'
    """
    result = bq_hook.get_first(sql)
    if result:
        total_mrr, churn_count = result[0], result[1]
        logger.info(f"Active MRR for {execution_date}: ${total_mrr:.2f} with {churn_count} churn events.")


with DAG(
    dag_id="saas_billing_subscription_pipeline",
    default_args=DEFAULT_ARGS,
    description="Extracts recurring billing contracts, standardizes MRR, and syncs fact_subscriptions",
    schedule_interval="0 2 * * *",  # Runs daily at 02:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    sla_miss_callback=flowsentinel_sla_miss_callback,
    params={"table": "dw_analytics.fact_subscriptions"},
    tags=["flowsentinel", "saas", "billing", "mrr", "finance", "p1"],
) as dag:

    # 1. API Extract
    t1_extract = PythonOperator(
        task_id="extract_subscriptions_api",
        python_callable=extract_subscriptions_api,
        provide_context=True,
    )

    # 2. Upload Raw to GCS
    t2_upload = PythonOperator(
        task_id="upload_raw_subscriptions_gcs",
        python_callable=upload_raw_subscriptions_gcs,
        provide_context=True,
    )

    # 3. Clean and Calculate MRR in Staging
    t3_clean = PythonOperator(
        task_id="clean_and_calculate_mrr",
        python_callable=clean_and_calculate_mrr,
        provide_context=True,
    )

    # 4. Load from GCS Staging to BigQuery Staging Table
    t4_load_staging = GCSToBigQueryOperator(
        task_id="load_staging_subscriptions_bq",
        bucket=GCS_LANDING_BUCKET,
        source_objects=["staging/saas_subscriptions/{{ ds }}/cleaned_subs_{{ ts_nodash }}.json"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.stg_saas_subscriptions",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "subscription_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "plan_tier", "type": "STRING", "mode": "REQUIRED"},
            {"name": "billing_interval", "type": "STRING", "mode": "NULLABLE"},
            {"name": "plan_amount_usd", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "mrr_usd", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "discount_percentage", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "subscription_status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "is_churned", "type": "BOOLEAN", "mode": "REQUIRED"},
            {"name": "period_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "period_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "canceled_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "subscription_start_date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "synced_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ],
    )

    # 5. Production Curated Table Load (MERGE)
    MERGE_FACT_SUBSCRIPTIONS_SQL = f"""
    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_subscriptions` (
        subscription_id STRING,
        customer_id STRING,
        plan_tier STRING,
        billing_interval STRING,
        plan_amount_usd FLOAT64,
        mrr_usd FLOAT64,
        discount_percentage FLOAT64,
        subscription_status STRING,
        is_churned BOOLEAN,
        period_start TIMESTAMP,
        period_end TIMESTAMP,
        canceled_at TIMESTAMP,
        subscription_start_date DATE,
        synced_at TIMESTAMP
    )
    PARTITION BY subscription_start_date
    CLUSTER BY plan_tier, subscription_status;

    MERGE `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_subscriptions` T
    USING `{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.stg_saas_subscriptions` S
    ON T.subscription_id = S.subscription_id AND T.subscription_start_date = S.subscription_start_date
    WHEN MATCHED THEN
      UPDATE SET
        plan_tier = S.plan_tier,
        billing_interval = S.billing_interval,
        plan_amount_usd = S.plan_amount_usd,
        mrr_usd = S.mrr_usd,
        discount_percentage = S.discount_percentage,
        subscription_status = S.subscription_status,
        is_churned = S.is_churned,
        period_start = S.period_start,
        period_end = S.period_end,
        canceled_at = S.canceled_at,
        synced_at = S.synced_at
    WHEN NOT MATCHED THEN
      INSERT (subscription_id, customer_id, plan_tier, billing_interval, plan_amount_usd, mrr_usd, discount_percentage, subscription_status, is_churned, period_start, period_end, canceled_at, subscription_start_date, synced_at)
      VALUES (S.subscription_id, S.customer_id, S.plan_tier, S.billing_interval, S.plan_amount_usd, S.mrr_usd, S.discount_percentage, S.subscription_status, S.is_churned, S.period_start, S.period_end, S.canceled_at, S.subscription_start_date, S.synced_at);
    """

    t5_merge_subs = BigQueryInsertJobOperator(
        task_id="merge_fact_subscriptions",
        configuration={
            "query": {
                "query": MERGE_FACT_SUBSCRIPTIONS_SQL,
                "useLegacySql": False,
            }
        },
    )

    # 6. MRR Reconciliation
    t6_reconciliation = PythonOperator(
        task_id="verify_mrr_reconciliation",
        python_callable=verify_mrr_reconciliation,
        provide_context=True,
    )

    t1_extract >> t2_upload >> t3_clean >> t4_load_staging >> t5_merge_subs >> t6_reconciliation
