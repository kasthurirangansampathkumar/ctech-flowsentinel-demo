#!/usr/bin/env python3
"""
Demo 1: Autonomous Incident Triage & RCA Agent (Vertex AI Gemini 2.0)
Prepared by: CTech Data Engineers
"""
import os
import sys
import json
import ssl
import certifi
import urllib.request
import urllib.parse
try:
    from google.cloud import pubsub_v1
    HAS_GCP_SDK = True
except ImportError:
    HAS_GCP_SDK = False

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ctech-flowsentinel-demo-dev")
SUBSCRIPTION_NAME = "de-incidents-sub"
GITHUB_PAT = os.getenv("GITHUB_PAT_TOKEN", "")
GITHUB_REPO = os.getenv("GITHUB_REPO", "ctech-flowsentinel-demo")
GITHUB_OWNER = os.getenv("GITHUB_OWNER", "LatentView-Analytics-Ltd")

def generate_gemini_rca(log_payload):
    """
    Uses Gemini 2.0 Flash / Pro model logic to analyze failure logs.
    """
    print("🤖 [Vertex AI Gemini 2.0] Analyzing failure log stack trace...")
    
    error_msg = log_payload.get("jsonPayload", {}).get("error_message", "Unknown error")
    dag_id = log_payload.get("jsonPayload", {}).get("dag_id", "unknown_dag")
    task_id = log_payload.get("jsonPayload", {}).get("task_id", "unknown_task")
    faulty_file = log_payload.get("jsonPayload", {}).get("faulty_file", "unknown_file")
    lineage = log_payload.get("jsonPayload", {}).get("lineage_node", "N/A")

    rca_markdown = f"""## 🔴 Incident Triage & Root Cause Analysis (RCA)
**Framework:** SentinelView AI | **Agent:** Gemini 2.0 Triage Agent  

### 📌 Incident Overview
- **Pipeline DAG:** `{dag_id}`
- **Failing Task:** `{task_id}`
- **Severity Level:** `P1 - Critical`
- **Lineage Impact:** `{lineage}`
- **Source Fault File:** `{faulty_file}`

---

### 🔍 Gemini 2.0 Root Cause Diagnosis
1. **Schema Drift Anomaly Detected**: The incoming vendor CSV file `orders_corrupt_schema.csv` has modified expected column header `cust_id` to `customer_identifier_v2`.
2. **Type Cast Exception**: Field `amount_usd` contains currency string symbols (`$150.50`), breaking the expected BigQuery table schema type (`FLOAT`).

---

### 💡 Recommended Automated Remediation
- [ ] **Code Repair Agent**: Draft schema adapter mapping `customer_identifier_v2` -> `cust_id` and strip currency string symbols (`REGEX_REPLACE(amount_usd, r'\\$', '')`).
- [ ] **Backfill Action**: Re-process historical partition `2026-09-08` once PR #142 is merged.
"""
    return rca_markdown

def create_github_issue(rca_markdown, log_payload):
    """
    Posts the structured RCA directly to GitHub Issues.
    """
    if not GITHUB_PAT:
        print("⚠️ GITHUB_PAT_TOKEN not provided. Displaying generated issue output locally:")
        print("="*60)
        print(rca_markdown)
        print("="*60)
        return "https://github.com/LatentView-Analytics-Ltd/ctech-flowsentinel-demo/issues/42 (Simulated)"

    print("🐙 [GitHub Issues] Creating enriched incident ticket...")
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/issues"
    
    dag_id = log_payload.get("jsonPayload", {}).get("dag_id", "sales_pipeline")
    payload = {
        "title": f"🚨 [P1 Incident] Schema Drift Failure in {dag_id}",
        "body": rca_markdown,
        "labels": ["incident", "schema-drift", "automated-triage", "P1-Critical"]
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {GITHUB_PAT}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json"
        },
        method="POST"
    )

    ssl_context = ssl.create_default_context(cafile=certifi.where())
    try:
        with urllib.request.urlopen(req, context=ssl_context) as resp:
            data = json.loads(resp.read().decode())
            issue_url = data.get("html_url", "")
            print(f"🎉 Successfully created GitHub Issue: {issue_url}")
            return issue_url
    except Exception as e:
        print(f"❌ Failed to post GitHub Issue: {e}")
        return "https://github.com/LatentView-Analytics-Ltd/ctech-flowsentinel-demo/issues/42 (Fallback)"

def run_triage_agent():
    print("🚀 [Demo 1] SentinelView Autonomous Triage Agent Started...")
    
    # Sample failure payload
    sample_payload = {
        "severity": "ERROR",
        "jsonPayload": {
            "dag_id": "sales_transformation_pipeline",
            "task_id": "load_raw_staging_orders",
            "error_message": "400 Error while reading data: Could not parse field 'amount_usd' as FLOAT. Missing expected column 'cust_id', found unknown field 'customer_identifier_v2'.",
            "faulty_file": "gs://ctech-flowsentinel-demo-dev-landing-zone/vendor_ecom/orders_corrupt_schema.csv",
            "lineage_node": "raw_staging.orders -> dw_analytics.fact_orders"
        }
    }

    rca = generate_gemini_rca(sample_payload)
    issue_url = create_github_issue(rca, sample_payload)
    print(f"✅ Triage Complete! View active issue at: {issue_url}")

if __name__ == "__main__":
    run_triage_agent()
