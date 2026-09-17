# Demo 3: Code-Change Driven Precision Backfill Engine

**Prepared by:** CTech Data Engineers  
**Framework:** SentinelView AI  

---

## 🎯 Demo Goal
Demonstrate how SentinelView AI automatically parses Airflow/Dataform lineage graphs following a code change, maps affected historical partition dates, estimates BigQuery compute slot costs ($1.20), and executes safe, quota-controlled partition backfills without risking GCP quota limits or corrupting live analytics tables.

---

## 📁 File Breakdown

| File Name | Purpose |
| :--- | :--- |
| `setup_services.sh` | Shell script to create the BigQuery sandbox dataset `backfill_sandbox`. |
| `backfill_agent.py` | Lineage & blast-radius calculator estimating partition impact & BigQuery slot cost. |
| `execute_backfill.py` | Sequential backfill execution engine running quota-safe BigQuery partition queries. |
| `run_demo.sh` | One-click launcher executing the full Demo 3 procedure. |

---

## 🚀 Step-by-Step Procedure

### 1. Environment Setup
```bash
export GCP_PROJECT_ID="ctech-flowsentinel-demo-dev"
```

### 2. Run the Full Demo
```bash
chmod +x run_demo.sh setup_services.sh
./run_demo.sh
```

### 3. Manual Step-by-Step Execution
```bash
# Step A: Provision Sandbox Infrastructure
./setup_services.sh

# Step B: Calculate Blast Radius & Compute Cost Matrix
python3 backfill_agent.py

# Step C: Execute Sequential Partition Backfills
python3 execute_backfill.py
```

---

## 📊 Expected Demo Output
1. The **Backfill Agent** analyzes the DAG lineage graph and identifies 7 affected historical partition dates (`2026-09-01` to `2026-09-07`).
2. An estimated BigQuery slot compute cost matrix ($1.20 USD) is generated and posted to the Ops Dashboard.
3. Upon 1-click approval, the **Backfill Engine** processes all 7 partitions sequentially with concurrency limits (max 2 parallel queries) to prevent GCP quota limits from tripping.
