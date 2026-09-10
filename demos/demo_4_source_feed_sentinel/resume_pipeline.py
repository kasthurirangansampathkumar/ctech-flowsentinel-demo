#!/usr/bin/env python3
"""
Demo 4: File Landing Detector & Pipeline Auto-Resumer
Prepared by: CTech Data Engineers
"""
import os
import sys
import json
import time
try:
    from google.cloud import storage
    HAS_GCP_SDK = True
except ImportError:
    HAS_GCP_SDK = False

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ctech-flowsentinel-demo-dev")
BUCKET_NAME = os.getenv("GCS_LANDING_BUCKET", f"{PROJECT_ID}-landing-zone")
EXPECTED_FEED_FILE = "vendor_crm/customer_profiles_2026-09-08.json"

DUMMY_JSON_FEED = [
    {"cust_id": "CUST-901", "name": "Acme Corp", "tier": "ENTERPRISE"},
    {"cust_id": "CUST-902", "name": "Global Logistics", "tier": "PREMIUM"},
    {"cust_id": "CUST-903", "name": "Apex Digital", "tier": "STANDARD"}
]

def simulate_file_landing():
    print(f"📥 [Vendor Delivery] Delayed vendor file landing in GCS 'gs://{BUCKET_NAME}/{EXPECTED_FEED_FILE}'...")
    
    local_file = "customer_profiles_temp.json"
    with open(local_file, "w") as f:
        json.dump(DUMMY_JSON_FEED, f, indent=2)

    if not HAS_GCP_SDK:
        print("ℹ️ [Offline Mode] Simulated file landing in GCS confirmed.")
        return

    try:
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(EXPECTED_FEED_FILE)
        blob.upload_from_filename(local_file)
        print(f"✅ Uploaded 'gs://{BUCKET_NAME}/{EXPECTED_FEED_FILE}'")
    except Exception as e:
        print(f"⚠️ [Dry-run Upload Notice]: {e}")
        print("📄 Local JSON payload generated for demonstration.")

def resume_downstream_pipelines():
    print("\n🔍 [Sentinel File Landing Event] Detected file arrival in GCS landing bucket!")
    print("🔓 [Guardrail Action] Unpausing Airflow DAG 'sales_transformation_pipeline'...")
    time.sleep(1)
    print("🚀 Triggering DAG execution run 'manual__2026-09-08T08:15:00'...")
    print("🟢 Pipeline Health Restored: 100% Compliant across all downstream datasets.")

if __name__ == "__main__":
    simulate_file_landing()
    resume_downstream_pipelines()
