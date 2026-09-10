#!/usr/bin/env python3
"""
Demo 1: Seed Initial Valid Data
Prepared by: CTech Data Engineers
"""
import os
import csv
import sys
try:
    from google.cloud import bigquery, storage
    HAS_GCP_SDK = True
except ImportError:
    HAS_GCP_SDK = False

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ctech-flowsentinel-demo-dev")
BUCKET_NAME = os.getenv("GCS_LANDING_BUCKET", f"{PROJECT_ID}-landing-zone")

VALID_ORDERS_CSV = "orders_valid.csv"
VALID_ROWS = [
    ["order_id", "cust_id", "order_date", "amount_usd", "status"],
    ["ORD-1001", "CUST-501", "2026-09-01", "150.50", "COMPLETED"],
    ["ORD-1002", "CUST-502", "2026-09-01", "89.99", "COMPLETED"],
    ["ORD-1003", "CUST-503", "2026-09-02", "249.00", "PENDING"],
    ["ORD-1004", "CUST-504", "2026-09-02", "12.50", "COMPLETED"],
    ["ORD-1005", "CUST-505", "2026-09-03", "450.00", "COMPLETED"],
]

def seed_data():
    print(f"🌱 [Demo 1] Generating valid e-commerce CSV data...")
    with open(VALID_ORDERS_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(VALID_ROWS)
    print(f"✅ Generated local file '{VALID_ORDERS_CSV}'")

    if not HAS_GCP_SDK:
        print("ℹ️ [Offline Mode] Local CSV seeded. Install 'google-cloud-storage' & 'google-cloud-bigquery' for live GCP upload.")
        return

    # Upload to GCS if clients are available
    try:
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
        if not bucket.exists():
            print(f"📌 Creating GCS Bucket 'gs://{BUCKET_NAME}'...")
            bucket = storage_client.create_bucket(BUCKET_NAME, location="US")

        blob = bucket.blob(f"vendor_ecom/{VALID_ORDERS_CSV}")
        blob.upload_from_filename(VALID_ORDERS_CSV)
        print(f"✅ Uploaded to 'gs://{BUCKET_NAME}/vendor_ecom/{VALID_ORDERS_CSV}'")

        # Load to BigQuery
        bq_client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.raw_staging.orders"
        
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("order_id", "STRING"),
                bigquery.SchemaField("cust_id", "STRING"),
                bigquery.SchemaField("order_date", "DATE"),
                bigquery.SchemaField("amount_usd", "FLOAT"),
                bigquery.SchemaField("status", "STRING"),
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        uri = f"gs://{BUCKET_NAME}/vendor_ecom/{VALID_ORDERS_CSV}"
        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
        print(f"✅ Loaded {load_job.output_rows} valid rows into BigQuery table '{table_id}'")

    except Exception as e:
        print(f"⚠️ [Dry-Run Mode] GCS/BigQuery client notice: {e}")
        print("📁 Local CSV file successfully created for offline demonstration.")

if __name__ == "__main__":
    seed_data()
