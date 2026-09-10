# FlowSentinel AI: Step-by-Step GCP Setup & Execution Guide

**Prepared by:** CTech Data Engineers  
**Target Audience:** Data Engineers, Cloud Admins, & Demo Presenters  

---

## 📌 Executive Summary & Dashboard Options

### Do I Need a Dashboard to Monitor?
**Yes.** A dashboard is essential for the demo because it visually demonstrates **Human-in-the-Loop (HITL) 1-Click Governance**, real-time pipeline status (🟢 Compliant / 🟡 Degraded / 🔴 Failed), and source feed SLA timers.

### What Dashboard Tools Can You Use?

| Dashboard Option | Setup Difficulty | Best For | Included in FlowSentinel AI? |
| :--- | :--- | :--- | :--- |
| **1. Built-in Custom Web App (React/Vite + FastAPI)** ⭐ *(Primary)* | Low (`./run_dashboard.sh`) | **Interactive 1-Click Approval Deck** for On-Call Engineers (approve PRs, trigger backfills, retry DAGs). | **Yes** (`/dashboard` folder) |
| **2. GCP Looker Studio** | Zero (`100% Free`) | **Executive Compliance & SLA Scorecard** connecting directly to BigQuery dataset `de_ops_metadata`. | **Yes** (Template URL provided) |
| **3. GitHub Projects v2 Kanban Board** | Zero (`100% Free`) | **Code-Native Ticket Tracking** moving cards from `🔴 Incident Triaged` ➔ `🟢 Resolved`. | **Yes** (Built-in) |

---

## 🛠️ Complete Step-by-Step Provisioning & Demo Instructions

Follow this sequential checklist once your system administrator grants access to your GCP project and GitHub repository.

```
┌─────────────────────────┐      ┌─────────────────────────┐      ┌─────────────────────────┐
│ Phase 1: Environment    │ ──►  │ Phase 2: GCP & GitHub   │ ──►  │ Phase 3: Launch Demo    │
│    Configuration        │      │    Provisioning         │      │  Suite & Dashboard      │
└─────────────────────────┘      └─────────────────────────┘      └─────────────────────────┘
```

---

### Phase 1: Environment Configuration (5 Mins)

1. Open your terminal and set your environment variables:
   ```bash
   export GCP_PROJECT_ID="ctech-flowsentinel-demo-dev"
   export GCP_REGION="us-central1"
   export GCS_LANDING_BUCKET="ctech-flowsentinel-demo-dev-landing-zone"
   
   # GitHub Integration (Optional for live GitHub PR/Issue creation)
   export GITHUB_PAT_TOKEN="ghp_your_generated_personal_access_token"
   export GITHUB_OWNER="kasthurirangan"
   export GITHUB_REPO="ctech-flowsentinel-demo"
   ```

2. Clone or navigate to the project directory:
   ```bash
   cd "/Users/kasthurirangansampathkumar/Documents/DE Oncall Demo"
   ```

---

### Phase 2: One-Click Infrastructure Provisioning (5 Mins)

1. Run the master provisioning script to enable all GCP APIs, create BigQuery datasets, configure Pub/Sub topics, and set up Cloud Logging sinks:
   ```bash
   chmod +x ./setup/gcp_provisioning.sh
   ./setup/gcp_provisioning.sh
   ```

2. Store your GitHub token safely in GCP Secret Manager:
   ```bash
   gcloud secrets create github-pat-token --replication-policy="automatic" --project="${GCP_PROJECT_ID}"
   echo -n "${GITHUB_PAT_TOKEN}" | gcloud secrets versions add github-pat-token --data-file=- --project="${GCP_PROJECT_ID}"
   ```

---

### Phase 3: Launching the Interactive Dashboard & Running Demos

#### Step A: Launch the Ops Dashboard
Launch the interactive React/FastAPI Ops Dashboard locally or on Cloud Run:
```bash
# Option 1: Run local FastAPI/React Dashboard on http://localhost:3000
python3 -m http.server 3000 --directory dashboard/

# Option 2: Deploy to GCP Cloud Run
gcloud run deploy flowsentinel-dashboard \
  --source=./dashboard \
  --region="${GCP_REGION}" \
  --allow-unauthenticated \
  --project="${GCP_PROJECT_ID}"
```

#### Step B: Run the Demo Scenarios

You can execute the master interactive launcher to run all 4 demo scenarios sequentially or individually:

```bash
# Launch Master Demo Suite
./run_all_demos.sh A
```

Alternatively, run each demo script manually step-by-step:

##### 🟢 **Demo 1: Autonomous Triage & RCA**
```bash
cd demos/demo_1_autonomous_triage
./run_demo.sh
```
*What happens:* Seeds data ➔ Injects schema drift ➔ Gemini 2.0 parses stack trace ➔ Generates RCA ➔ Creates GitHub Issue #42.

##### 🟡 **Demo 2: Code Patch & 1-Click Approval**
```bash
cd demos/demo_2_code_patch_and_approval
./run_demo.sh
```
*What happens:* Gemini 2.0 generates fixed SQL ➔ Opens GitHub PR #142 ➔ Simulates 1-click human approval on Dashboard ➔ Merges PR & redeploys Airflow DAG.

##### 🔵 **Demo 3: Precision Code-Change Backfill**
```bash
cd demos/demo_3_blast_radius_backfill
./run_demo.sh
```
*What happens:* Parses lineage graph ➔ Computes 7-day partition impact ($1.20 BQ cost) ➔ Runs quota-safe sequential partition backfills.

##### 🟣 **Demo 4: Upstream Source Feed Sentinel**
```bash
cd demos/demo_4_source_feed_sentinel
./run_demo.sh
```
*What happens:* Detects delayed vendor GCS file past 8:00 AM SLA ➔ Auto-pauses downstream Airflow DAGs ➔ Detects file landing ➔ Auto-resumes DAG.

---

## 📋 Pre-Demo Verification Checklist

- [ ] All 13 GCP APIs enabled (`gcloud services list --enabled`)
- [ ] GCS Bucket `gs://ctech-flowsentinel-demo-dev-landing-zone` created
- [ ] BigQuery datasets `raw_staging`, `dw_analytics`, `de_ops_metadata`, `backfill_sandbox` created
- [ ] Pub/Sub topic `de-incidents-topic` & Cloud Logging sink `de-pipeline-failure-sink` created
- [ ] Dashboard accessible at `http://localhost:3000` or Cloud Run URL
