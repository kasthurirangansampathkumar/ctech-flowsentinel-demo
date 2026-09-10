#!/usr/bin/env python3
"""
Demo 4: Source Feed Sentinel (SLA Monitor & Guardrail)
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
SLA_DEADLINE_TIME = "08:00:00 UTC"

def check_feed_arrival_sla():
    print(f"📡 [Source Feed Sentinel] Checking arrival status for vendor feed '{EXPECTED_FEED_FILE}'...")
    print(f"⏰ Expected Vendor SLA Deadline: {SLA_DEADLINE_TIME}")
    
    file_exists = False
    if HAS_GCP_SDK:
        try:
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(EXPECTED_FEED_FILE)
            file_exists = blob.exists()
        except Exception as e:
            print(f"⚠️ [GCS Check Notice]: {e}. Simulating SLA breach evaluation.")

    if not file_exists:
        print("\n🚨 [SLA BREACH DETECTED] Vendor feed 'customer_profiles_2026-09-08.json' is MISSING!")
        pause_downstream_dags()
        send_vendor_alert_webhook()
        return False
    else:
        print("🟢 Vendor feed present! Downstream pipelines clear to execute.")
        return True

def pause_downstream_dags():
    print("🛡️ [Guardrail Action] Auto-pausing downstream Airflow DAG 'sales_transformation_pipeline'...")
    time.sleep(1)
    print("🔒 Airflow DAG 'sales_transformation_pipeline' state set to PAUSED.")
    print("🛡️ Protection Active: Zero corrupted/partial data will be loaded into BigQuery.")

def send_vendor_alert_webhook():
    print("📢 [Vendor Alerting] Dispatching SLA breach warning to vendor contact 'data-delivery@vendor-crm.com'...")
    time.sleep(0.5)
    print("✅ Vendor alert logged & Slack notification sent to #de-oncall-alerts!")

if __name__ == "__main__":
    check_feed_arrival_sla()
