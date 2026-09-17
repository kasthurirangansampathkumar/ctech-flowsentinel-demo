# Demo 2: Autonomous Code Patch & 1-Click Approval

**Prepared by:** CTech Data Engineers  
**Framework:** SentinelView AI  

---

## 🎯 Demo Goal
Demonstrate how SentinelView AI takes the Root Cause Analysis (RCA) from Demo 1, uses **Gemini 2.0** to generate an automated SQL schema patch, opens a Pull Request on GitHub, and presents a 1-click approval card on the **Human-in-the-Loop (HITL) Dashboard** to merge the fix and redeploy the Airflow DAG.

---

## 📁 File Breakdown

| File Name | Purpose |
| :--- | :--- |
| `setup_services.sh` | Shell script to check Secret Manager tokens and Cloud Run API enablements. |
| `code_repair_agent.py` | AI agent generating fixed SQL (with schema adapter & regex currency sanitizer) and opening GitHub PR #142. |
| `approve_pr_trigger.py` | 1-Click approval simulator merging PR #142 and triggering Airflow DAG redeployment. |
| `run_demo.sh` | One-click launcher executing the full Demo 2 procedure. |

---

## 🚀 Step-by-Step Procedure

### 1. Environment Setup
```bash
export GCP_PROJECT_ID="ctech-flowsentinel-demo-dev"
export GITHUB_PAT_TOKEN="ghp_your_personal_access_token"
export GITHUB_REPO="ctech-flowsentinel-demo"
export GITHUB_OWNER="LatentView-Analytics-Ltd"
```

### 2. Run the Full Demo
```bash
chmod +x run_demo.sh setup_services.sh
./run_demo.sh
```

### 3. Manual Step-by-Step Execution
```bash
# Step A: Provision dependencies
./setup_services.sh

# Step B: Run Code Repair Agent (Drafts PR)
python3 code_repair_agent.py

# Step C: Simulate 1-Click Human Approval from Ops Dashboard
python3 approve_pr_trigger.py
```

---

## 📊 Expected Demo Output
1. Gemini 2.0 generates SQL schema adapter: `COALESCE(cust_id, customer_identifier_v2)` + `REGEXP_REPLACE(amount_usd, r'\$', '')`.
2. A new branch `fix/schema-drift-orders` is pushed and PR #142 is opened on GitHub.
3. The Ops Dashboard displays the PR diff card. Upon clicking `[Approve & Auto-Deploy]`, PR #142 is merged into `main` and the Airflow DAG is redeployed.
