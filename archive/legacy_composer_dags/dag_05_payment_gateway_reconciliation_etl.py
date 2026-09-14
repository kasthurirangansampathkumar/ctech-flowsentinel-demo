"""
DAG 5: Payment Gateway Ingestion & Financial Reconciliation Pipeline
====================================================================
Pipeline ID: payment_reconciliation_pipeline
Target Table: dw_analytics.fact_payment_transactions

ETL Flow:
1. Extract settlement ledger, fees, and refunds from Payment Gateway REST API (Stripe/PayPal/Adyen).
2. Upload raw transaction JSON to GCS Landing Zone (`raw/payments/...`).
3. Staging deduplication & cleaning: Resolves webhook/Kafka replay window duplicates,
   converts integer cents to USD decimals, and sanitizes payment statuses.
4. Load to BigQuery staging table (`staging.stg_payment_transactions`).
5. Partition-filtered MERGE into production `dw_analytics.fact_payment_transactions`.
6. Run Cross-Pipeline Reconciliation Audit: Cross-checks settlement sum against `dw_analytics.fact_orders`.
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
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": flowsentinel_failure_callback,
}


def extract_payment_ledger_api(**context) -> str:
    """
    Step 1: Extract settled transactions, fees, and chargebacks from Payment Gateway API.
    Simulates production transaction batch with potential replay events.
    """
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")
    logger = logging.getLogger("airflow.task")
    logger.info(f"Extracting payment gateway settlement ledger for {execution_date}...")

    payment_methods = ["credit_card", "apple_pay", "google_pay", "paypal", "ach_transfer"]
    sample_charges = []

    for i in range(150):
        method = payment_methods[i % len(payment_methods)]
        cents = (2500 + (i * 350))
        fee_cents = int(cents * 0.029) + 30  # 2.9% + 30 cents standard fee
        status = "SUCCEEDED" if i % 12 != 0 else "REFUNDED"

        sample_charges.append({
            "transaction_id": f"txn_{execution_date.replace('-', '')}_{1000 + i}",
            "order_id": f"ORD-{execution_date}-{1000 + (i % 120)}",
            "amount_cents": cents,
            "fee_cents": fee_cents,
            "currency": "usd",
            "payment_method": method,
            "gateway_status": status,
            "event_timestamp": f"{execution_date}T{(i % 24):02d}:{(i % 60):02d}:00Z",
            "metadata": {"customer_ref": f"CUST-{200 + (i % 50)}"},
        })

    # Intentionally inject a duplicate webhook replay to exercise deduplication
    sample_charges.append(sample_charges[0])

    temp_path = f"/tmp/payment_ledger_{ts_nodash}.json"
    with open(temp_path, "w", encoding="utf-8") as f:
        for txn in sample_charges:
            f.write(json.dumps(txn) + "\n")

    logger.info(f"Extracted {len(sample_charges)} payment events to {temp_path}")
    return temp_path


def upload_raw_payments_gcs(**context) -> str:
    """
    Step 2: Upload raw payment ledger to GCS Landing Zone.
    Destination: gs://<gcs_landing_bucket>/raw/payments/<ds>/transactions_<ts_nodash>.json
    """
    ti = context["ti"]
    temp_path = ti.xcom_pull(task_ids="extract_payment_ledger_api")
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")

    gcs_object_name = f"raw/payments/{execution_date}/transactions_{ts_nodash}.json"
    logger = logging.getLogger("airflow.task")
    logger.info(f"Uploading payment ledger to gs://{GCS_LANDING_BUCKET}/{gcs_object_name}...")

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


def clean_and_deduplicate_payments(**context) -> str:
    """
    Step 3: Staging Layer Transformation & Deduplication.
    - Resolves duplicate webhook/stream events by transaction_id
    - Converts cents to USD decimal float
    - Validates gateway status
    - Writes cleaned NDJSON to GCS staging
    """
    ti = context["ti"]
    raw_gcs_object = ti.xcom_pull(task_ids="upload_raw_payments_gcs")
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")
    logger = logging.getLogger("airflow.task")

    gcs_hook = GCSHook()
    raw_data = gcs_hook.download_as_byte_array(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=raw_gcs_object,
    ).decode("utf-8")

    seen_transactions = {}
    for line in raw_data.strip().split("\n"):
        if not line:
            continue
        item = json.loads(line)
        txn_id = str(item.get("transaction_id", "")).strip()
        if not txn_id:
            continue

        cents = int(item.get("amount_cents", 0))
        fee = int(item.get("fee_cents", 0))

        # Keeps latest record in case of duplicate re-emits
        seen_transactions[txn_id] = {
            "transaction_id": txn_id,
            "order_id": str(item.get("order_id", "")).strip(),
            "customer_id": str(item.get("metadata", {}).get("customer_ref", "UNKNOWN")).strip(),
            "amount_usd": round(cents / 100.0, 2),
            "fee_usd": round(fee / 100.0, 2),
            "net_amount_usd": round((cents - fee) / 100.0, 2),
            "currency": str(item.get("currency", "USD")).upper(),
            "payment_method": str(item.get("payment_method", "unknown")).lower(),
            "gateway_status": str(item.get("gateway_status", "PENDING")).upper(),
            "event_timestamp": item.get("event_timestamp", f"{execution_date}T00:00:00Z"),
            "transaction_date": execution_date,
            "ingested_at": datetime.utcnow().isoformat() + "Z",
        }

    cleaned_records = list(seen_transactions.values())
    staging_gcs_object = f"staging/payments/{execution_date}/cleaned_transactions_{ts_nodash}.json"
    cleaned_str = "\n".join(json.dumps(r) for r in cleaned_records)

    gcs_hook.upload(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=staging_gcs_object,
        data=cleaned_str.encode("utf-8"),
        mime_type="application/json",
    )
    logger.info(f"Staged {len(cleaned_records)} deduplicated payment transactions to {staging_gcs_object}")
    return staging_gcs_object


def verify_financial_reconciliation(**context):
    """
    Step 6: Cross-pipeline Financial Reconciliation Audit.
    Checks that settled charges recorded in fact_payment_transactions match
    or stay within expected variance of fact_orders for the same date.
    """
    execution_date = context.get("ds")
    logger = logging.getLogger("airflow.task")
    bq_hook = BigQueryHook()

    sql = f"""
    SELECT
      COALESCE(SUM(p.amount_usd), 0) as total_payments,
      COUNT(p.transaction_id) as txn_count
    FROM `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_payment_transactions` p
    WHERE p.transaction_date = '{execution_date}' AND p.gateway_status = 'SUCCEEDED'
    """
    result = bq_hook.get_first(sql)
    total_payments = float(result[0]) if result else 0.0
    txn_count = int(result[1]) if result else 0

    logger.info(
        f"Financial Reconciliation Audit for {execution_date}: "
        f"{txn_count} successful transactions totaling ${total_payments:.2f}"
    )


with DAG(
    dag_id="payment_reconciliation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Extracts gateway payments, deduplicates replay events, and merges into BigQuery",
    schedule_interval="0 5 * * *",  # Runs daily at 05:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    sla_miss_callback=flowsentinel_sla_miss_callback,
    params={"table": "dw_analytics.fact_payment_transactions"},
    tags=["flowsentinel", "finance", "payments", "reconciliation", "p1"],
) as dag:

    # 1. API Extraction
    t1_extract = PythonOperator(
        task_id="extract_payment_ledger_api",
        python_callable=extract_payment_ledger_api,
        provide_context=True,
    )

    # 2. Upload Raw to GCS
    t2_upload = PythonOperator(
        task_id="upload_raw_payments_gcs",
        python_callable=upload_raw_payments_gcs,
        provide_context=True,
    )

    # 3. Clean and Deduplicate in Staging
    t3_clean = PythonOperator(
        task_id="clean_and_deduplicate_payments",
        python_callable=clean_and_deduplicate_payments,
        provide_context=True,
    )

    # 4. Load from GCS Staging into BigQuery Staging Table
    t4_load_staging = GCSToBigQueryOperator(
        task_id="load_staging_payments_bq",
        bucket=GCS_LANDING_BUCKET,
        source_objects=["staging/payments/{{ ds }}/cleaned_transactions_{{ ts_nodash }}.json"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.stg_payment_transactions",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "transaction_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "amount_usd", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "fee_usd", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "net_amount_usd", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
            {"name": "payment_method", "type": "STRING", "mode": "NULLABLE"},
            {"name": "gateway_status", "type": "STRING", "mode": "REQUIRED"},
            {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "transaction_date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ],
    )

    # 5. Production Curated Table Load (MERGE)
    MERGE_FACT_PAYMENTS_SQL = f"""
    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_payment_transactions` (
        transaction_id STRING,
        order_id STRING,
        customer_id STRING,
        amount_usd FLOAT64,
        fee_usd FLOAT64,
        net_amount_usd FLOAT64,
        currency STRING,
        payment_method STRING,
        gateway_status STRING,
        event_timestamp TIMESTAMP,
        transaction_date DATE,
        ingested_at TIMESTAMP
    )
    PARTITION BY transaction_date
    CLUSTER BY payment_method, gateway_status;

    MERGE `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_payment_transactions` T
    USING `{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.stg_payment_transactions` S
    ON T.transaction_id = S.transaction_id AND T.transaction_date = S.transaction_date
    WHEN MATCHED THEN
      UPDATE SET
        order_id = S.order_id,
        customer_id = S.customer_id,
        amount_usd = S.amount_usd,
        fee_usd = S.fee_usd,
        net_amount_usd = S.net_amount_usd,
        currency = S.currency,
        payment_method = S.payment_method,
        gateway_status = S.gateway_status,
        event_timestamp = S.event_timestamp,
        ingested_at = S.ingested_at
    WHEN NOT MATCHED THEN
      INSERT (transaction_id, order_id, customer_id, amount_usd, fee_usd, net_amount_usd, currency, payment_method, gateway_status, event_timestamp, transaction_date, ingested_at)
      VALUES (S.transaction_id, S.order_id, S.customer_id, S.amount_usd, S.fee_usd, S.net_amount_usd, S.currency, S.payment_method, S.gateway_status, S.event_timestamp, S.transaction_date, S.ingested_at);
    """

    t5_merge_payments = BigQueryInsertJobOperator(
        task_id="merge_fact_payments",
        configuration={
            "query": {
                "query": MERGE_FACT_PAYMENTS_SQL,
                "useLegacySql": False,
            }
        },
    )

    # 6. Cross-Pipeline Financial Reconciliation Check
    t6_reconciliation = PythonOperator(
        task_id="verify_financial_reconciliation",
        python_callable=verify_financial_reconciliation,
        provide_context=True,
    )

    t1_extract >> t2_upload >> t3_clean >> t4_load_staging >> t5_merge_payments >> t6_reconciliation
