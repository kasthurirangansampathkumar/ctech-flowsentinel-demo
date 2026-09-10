# Demo 1: Autonomous Incident Triage & RCA

**Prepared by:** CTech Data Engineers  
**Framework:** FlowSentinel AI  

---

## 🎯 Demo Goal
Demonstrate how FlowSentinel AI automatically detects a pipeline failure in Cloud Composer / BigQuery, intercepts the stack trace via Cloud Logging & Pub/Sub, uses **Vertex AI Gemini 2.0** to perform Root Cause Analysis (RCA), and generates an enriched ticket in **GitHub Issues** within 5 seconds.

---

## 📁 File Breakdown

| File Name | Purpose |
| :--- | :--- |
| `setup_services.sh` | Shell script to enable GCP APIs, create Pub/Sub topic `de-incidents-topic`, BigQuery datasets, and Cloud Logging failure sinks. |
| `seed_data.py` | Python script to seed valid e-commerce orders data into GCS & BigQuery. |
| `inject_failure.py` | Failure injector script simulating schema drift (renamed column `cust_id` -> `customer_identifier_v2`). |
| `triage_agent.py` | Gemini 2.0 Triage Agent that parses stack traces, extracts RCA, and creates an enriched GitHub Issue. |
| `run_demo.sh` | One-click launcher executing the full end-to-end Demo 1 procedure. |

---

## 🚀 Step-by-Step Procedure

### 1. Environment Setup
Export your GCP project and GitHub token (optional for live GitHub posting):
```bash
export GCP_PROJECT_ID="ctech-flowsentinel-demo-dev"
export GITHUB_PAT_TOKEN="ghp_your_personal_access_token"
export GITHUB_REPO="ctech-flowsentinel-demo"
export GITHUB_OWNER="kasthurirangan"
```

### 2. Run the Full Demo
Execute the one-click launcher:
```bash
chmod +x run_demo.sh setup_services.sh
./run_demo.sh
```

### 3. Step-by-Step Manual Execution (Alternative)
If you prefer running individual steps manually:

```bash
# Step A: Provision GCP Pub/Sub & Logging Sinks
./setup_services.sh

# Step B: Seed Valid Data
python3 seed_data.py

# Step C: Inject Schema Drift Anomaly
python3 inject_failure.py

# Step D: Trigger Autonomous Gemini 2.0 Triage Agent
python3 triage_agent.py
```

---

## 📊 Expected Demo Output
1. Corrupt CSV file `orders_corrupt_schema.csv` is uploaded to `gs://<project-id>-landing-zone/vendor_ecom/`.
2. Cloud Logging captures the schema drift failure and publishes an event payload to Pub/Sub topic `de-incidents-topic`.
3. Gemini 2.0 Agent parses the error stack trace, identifies the fault line, and creates an enriched ticket in **GitHub Issues** labeled `P1-Critical`, `schema-drift`, and `automated-triage`.
