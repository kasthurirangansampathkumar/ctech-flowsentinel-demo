#!/usr/bin/env python3
"""
Demo 1: Controlled Failure Injector (Schema Drift Anomaly)
Prepared by: CTech Data Engineers
"""
import os
import csv
import sys
import json
import time
try:
    from google.cloud import bigquery, storage, pubsub_v1
    HAS_GCP_SDK = True
except ImportError:
    HAS_GCP_SDK = False

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ctech-flowsentinel-demo-dev")
BUCKET_NAME = os.getenv("GCS_LANDING_BUCKET", f"{PROJECT_ID}-landing-zone")
TOPIC_NAME = "de-incidents-topic"

CORRUPT_ORDERS_CSV = "orders_corrupt_schema.csv"
# ANOMALY: Header 'cust_id' renamed to 'customer_identifier_v2', amount formatted as '$150.50' (string)
CORRUPT_ROWS = [
    ["order_id", "customer_identifier_v2", "order_date", "amount_usd", "status"],
    ["ORD-2001", "CUST-901", "2026-09-08", "$150.50", "COMPLETED"],
    ["ORD-2002", "CUST-902", "2026-09-08", "$89.99", "COMPLETED"],
    ["ORD-2003", "CUST-903", "2026-09-08", "$249.00", "PENDING"],
]

FAILURE_LOG_PAYLOAD = {
    "severity": "ERROR",
    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "resource": {
        "type": "cloud_composer_environment",
        "labels": {"environment_name": "prod-de-composer-v3", "location": "us-central1"}
    },
    "jsonPayload": {
        "dag_id": "sales_transformation_pipeline",
        "task_id": "load_raw_staging_orders",
        "execution_date": time.strftime("%Y-%m-%dT00:00:00Z"),
        "error_type": "google.api_core.exceptions.BadRequest",
        "error_message": "400 Error while reading data, error message: Could not parse field 'amount_usd' as FLOAT. Value '$150.50'. Schema mismatch: Missing expected column 'cust_id', found unknown field 'customer_identifier_v2'.",
        "faulty_file": "gs://ctech-flowsentinel-demo-dev-landing-zone/vendor_ecom/orders_corrupt_schema.csv",
        "lineage_node": "raw_staging.orders -> dw_analytics.fact_orders"
    }
}

def inject_failure():
    print(f"🧪 [Demo 1] Injecting Schema Drift Anomaly into GCS & Cloud Logging...")
    
    # 1. Write corrupted CSV
    with open(CORRUPT_ORDERS_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(CORRUPT_ROWS)
    print(f"❌ Injected schema anomaly into local CSV: '{CORRUPT_ORDERS_CSV}'")

    if not HAS_GCP_SDK:
        print("ℹ️ [Offline Mode] Simulated failure log payload generated:")
        print(json.dumps(FAILURE_LOG_PAYLOAD, indent=2))
        return

    # 2. Upload corrupted CSV & publish log event to Pub/Sub
    try:
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"vendor_ecom/{CORRUPT_ORDERS_CSV}")
        blob.upload_from_filename(CORRUPT_ORDERS_CSV)
        print(f"❌ Uploaded corrupt file to 'gs://{BUCKET_NAME}/vendor_ecom/{CORRUPT_ORDERS_CSV}'")

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
        data = json.dumps(FAILURE_LOG_PAYLOAD).encode("utf-8")
        future = publisher.publish(topic_path, data)
        print(f"📡 Published failure log to Pub/Sub topic '{TOPIC_NAME}' (Message ID: {future.result()})")

    except Exception as e:
        print(f"⚠️ [Dry-Run Notice]: {e}")
        print(f"📄 Local failure log payload generated:\n{json.dumps(FAILURE_LOG_PAYLOAD, indent=2)}")

if __name__ == "__main__":
    inject_failure()
