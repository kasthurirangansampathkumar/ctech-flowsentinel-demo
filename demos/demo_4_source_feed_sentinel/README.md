# Demo 4: Upstream Source Feed Sentinel & SLA Guardrail

**Prepared by:** CTech Data Engineers  
**Framework:** FlowSentinel AI  

---

## 🎯 Demo Goal
Demonstrate how FlowSentinel AI continuously monitors incoming 3rd-party vendor file arrival windows in GCS/SFTP. When a vendor file is delayed past its 8:00 AM SLA window, the **Source Feed Sentinel** automatically pauses downstream Cloud Composer / Airflow DAGs to prevent partial data loading, dispatches a vendor alert, and automatically unpauses and executes the pipeline the instant the file lands.

---

## 📁 File Breakdown

| File Name | Purpose |
| :--- | :--- |
| `setup_services.sh` | Shell script to create GCS landing zone subdirectories & enable Monitoring APIs. |
| `feed_sentinel.py` | SLA Monitor script detecting missing vendor feed files and auto-pausing Airflow DAGs. |
| `resume_pipeline.py` | File landing detector script uploading delayed CSV/JSON file and auto-resuming DAG execution. |
| `run_demo.sh` | One-click launcher executing the full Demo 4 procedure. |

---

## 🚀 Step-by-Step Procedure

### 1. Environment Setup
```bash
export GCP_PROJECT_ID="ctech-flowsentinel-demo-dev"
export GCS_LANDING_BUCKET="ctech-flowsentinel-demo-dev-landing-zone"
```

### 2. Run the Full Demo
```bash
chmod +x run_demo.sh setup_services.sh
./run_demo.sh
```

### 3. Manual Step-by-Step Execution
```bash
# Step A: Provision Infrastructure
./setup_services.sh

# Step B: Trigger SLA Monitor (Detects missing file & pauses DAG)
python3 feed_sentinel.py

# Step C: Simulate Delayed File Landing & Auto-Resume
python3 resume_pipeline.py
```

---

## 📊 Expected Demo Output
1. The **Source Feed Sentinel** checks GCS for `vendor_crm/customer_profiles_2026-09-08.json` at 8:05 AM and flags an SLA breach.
2. Downstream Airflow DAG `sales_transformation_pipeline` is set to `PAUSED` to protect BigQuery datasets from partial/corrupted data.
3. An automated vendor warning notification is dispatched to `#de-oncall-alerts`.
4. As soon as `customer_profiles_2026-09-08.json` lands in GCS, the Sentinel detects the file event, unpauses the DAG, and triggers execution.
