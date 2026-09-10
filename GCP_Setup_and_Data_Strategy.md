# FlowSentinel AI: GCP Project Setup, GitHub Issues/Projects Provisioning & Data Strategy Guide

**Prepared by:** CTech Data Engineers  
**Target Audience:** System Administrators, GCP Cloud Admins, & GitHub Org Administrators  

---

## 📋 1. GCP Project & Required APIs Checklist

Please create a dedicated GCP project (e.g., `ctech-flowsentinel-demo-dev`) and enable the following 13 Google Cloud APIs:

### A. Core Orchestration & Compute APIs
| API Name | Service Identifier | Purpose in FlowSentinel AI |
| :--- | :--- | :--- |
| **Cloud Composer API** | `composer.googleapis.com` | Apache Airflow 3 pipeline execution & DAG orchestration |
| **Cloud Run API** | `run.googleapis.com` | Hosting the interactive React/FastAPI Dashboard web app |
| **Cloud Build API** | `cloudbuild.googleapis.com` | Building container images for Cloud Run & custom Airflow operators |
| **Artifact Registry API** | `artifactregistry.googleapis.com` | Storing Docker container images & Python packages |

### B. Data & Analytics Engine APIs
| API Name | Service Identifier | Purpose in FlowSentinel AI |
| :--- | :--- | :--- |
| **BigQuery API** | `bigquery.googleapis.com` | Enterprise Data Warehouse, staging, & analytical queries |
| **BigQuery Storage API** | `bigquerystorage.googleapis.com` | High-performance streaming & reading for AI agents |
| **Dataform API** | `dataform.googleapis.com` | SQL pipeline transformations & lineage graph parsing |
| **Cloud Storage API** | `storage.googleapis.com` | Raw data landing zone (GCS buckets for CSV/JSON vendor feeds) |
| **Cloud Pub/Sub API** | `pubsub.googleapis.com` | Real-time incident event broker & alert log streaming |

### C. AI, Observability & Secrets APIs
| API Name | Service Identifier | Purpose in FlowSentinel AI |
| :--- | :--- | :--- |
| **Vertex AI API** | `aiplatform.googleapis.com` | Model invocation (Gemini 2.0 Flash / Pro) for agentic RCA & code generation |
| **Generative Language API** | `generativelanguage.googleapis.com` | Gemini API alternative endpoint for agent reasoning |
| **Cloud Logging API** | `logging.googleapis.com` | Failure log aggregation & Pub/Sub sink triggers |
| **Cloud Monitoring API** | `monitoring.googleapis.com` | Pipeline metric collection & SLA latency tracking |
| **Secret Manager API** | `secretmanager.googleapis.com` | Storing GitHub PAT tokens & Slack webhooks safely |

---

## 🐙 2. GitHub Issues & Projects v2 Setup & Unblock Provisions

To replace Jira with a 100% free, code-native ticketing solution, we utilize **GitHub Issues + GitHub Projects v2**. 

### A. Required GitHub Access Scopes & Tokens
System/GitHub Admin must issue a **Fine-Grained Personal Access Token (PAT)** or **GitHub App Token** for the `flowsentinel-agent` with the following permissions:

| Permission Area | Required Access | Reason / Unblock Requirement |
| :--- | :--- | :--- |
| **Issues** | `Read & Write` | Auto-create incident tickets, apply labels (`incident`, `schema-drift`, `feed-delay`), post RCA summaries, and close issues on resolution. |
| **Pull Requests** | `Read & Write` | Open PRs with automated code fixes, link `#Issue-ID`, and request human review. |
| **Projects (v2)** | `Read & Write` | Add incident items to GitHub Projects board, update status columns, and set custom field metadata. |
| **Repository Contents** | `Read & Write` | Commit code patches to fix broken SQL or schema definitions. |
| **Workflows** | `Read & Write` | Trigger GitHub Actions workflows for automated backfill runs. |

### B. GitHub Organization / Repository Administrative Unblock Checklist
Please ask your GitHub Admin to unblock the following settings in your organization:

- [ ] **Unblock PAT Token Access**: Ensure `Personal Access Tokens (v2)` are allowed for the target repository (`ctech-flowsentinel-demo`).
- [ ] **Enable GitHub Projects v2**: Verify Projects v2 is enabled under Organization/Repository Settings.
- [ ] **Store Secret in GCP Secret Manager**: Add the generated token to GCP Secret Manager:
  ```bash
  gcloud secrets create github-pat-token --replication-policy="automatic"
  echo -n "ghp_your_generated_pat_token" | gcloud secrets versions add github-pat-token --data-file=-
  ```

### C. GitHub Projects v2 Board Structure
We will configure a GitHub Project Board (`FlowSentinel AI - Incident & Backfill Command Board`) with the following column workflow and custom fields:

```
┌────────────────────────┐  ┌────────────────────────┐  ┌────────────────────────┐  ┌────────────────────────┐
│  🔴 Incident Triaged  │  │ 🟡 PR Drafted (Awaiting)│  │ 🔵 Backfill Scheduled  │  │ 🟢 Resolved & Closed   │
├────────────────────────┤  ├────────────────────────┤  ├────────────────────────┤  ├────────────────────────┤
│ Auto-populated by AI   │  │ Code Repair Agent PR   │  │ Backfill Engine Matrix │  │ Auto-closed via merged │
│ with Gemini 2.0 RCA    │  │ linked to #Issue-ID    │  │ waiting 1-click run    │  │ Pull Request           │
└────────────────────────┘  └────────────────────────┘  └────────────────────────┘  └────────────────────────┘
```

- **Custom Fields Configured**:
  - `Incident Severity`: Single select (`P1 - Critical`, `P2 - High`, `P3 - Moderate`)
  - `Pipeline Name`: Text field (e.g. `dag_sales_transformation`)
  - `Fault Line`: Text field (e.g. `models/staging/stg_orders.sql:L42`)
  - `Est. Backfill Cost ($)`: Number field (e.g. `$1.20`)

---

## 🔑 3. GCP IAM Roles & Service Account Setup

Create Service Account: `flowsentinel-agent-sa@<project-id>.iam.gserviceaccount.com` and assign these 6 IAM roles:

| IAM Role Name | IAM Role Identifier | Purpose |
| :--- | :--- | :--- |
| **BigQuery Admin** | `roles/bigquery.admin` | Querying logs, checking schema, executing backfill queries |
| **Storage Object Admin** | `roles/storage.objectAdmin` | Managing GCS landing buckets for vendor feeds |
| **Cloud Composer User** | `roles/composer.user` | Retrying Airflow tasks, checking XCom states |
| **Vertex AI User** | `roles/aiplatform.user` | Invoking Gemini 2.0 agentic triage & RCA models |
| **Pub/Sub Editor** | `roles/pubsub.editor` | Publishing & subscribing to incident notification events |
| **Secret Accessor** | `roles/secretmanager.secretAccessor` | Fetching `github-pat-token` and Slack Webhooks |

---

## 📦 4. Data Provisioning & Seeding Strategy

We establish a 3-tier data architecture to demonstrate full autonomous recovery:

1. **Raw GCS Landing Buckets**:
   - `gs://<project-id>-landing-zone/vendor_ecom/` (Hourly CSV sales feeds).
   - `gs://<project-id>-landing-zone/vendor_crm/` (Daily JSON customer profile feeds).
2. **BigQuery Dataset Hierarchy**: `raw_staging`, `dw_analytics`, `de_ops_metadata`, and `backfill_sandbox`.
3. **Data Anomaly Injector Utility (`data_injector.py`)**:
   - 🧪 **Schema Drift**: Injects altered CSV headers to test Autonomous Triage & PR creation.
   - 🧪 **Feed SLA Delay**: Delays file arrival past 8:00 AM to test Source Feed Sentinel auto-pause.
   - 🧪 **Logic Code Diff**: Merges a metric formula change to demonstrate the Blast-Radius Backfill Engine.

---

## 📌 Complete Admin Action Checklist

- [ ] Create GCP Project: `ctech-flowsentinel-demo-dev`
- [ ] Enable 13 listed GCP APIs (Composer, BigQuery, Vertex AI, Cloud Run, Pub/Sub, etc.)
- [ ] Create GitHub Repository (`ctech-flowsentinel-demo`) & enable Projects v2
- [ ] Issue GitHub PAT with `issues`, `pull_requests`, `projects`, `contents`, `workflows` permissions
- [ ] Store GitHub PAT in GCP Secret Manager as `github-pat-token`
- [ ] Create GCP Service Account `flowsentinel-agent-sa` & assign 6 IAM roles
- [ ] Provision GCS Landing Bucket `gs://ctech-flowsentinel-demo-dev-landing-zone`
