> **Archive note (2026-09-14):** This folder is a backup of files that were sitting directly in the
> `ctech-flowsentinel-demo-dev` Composer environment's GCS `dags/` bucket but were never committed to
> this repo (a separate contributor's DAGs, not the ones this repo's own `composer_dags/` builds and
> deploys). Pulled here before the DAG bucket was wiped to cut Cloud Composer cost. Not wired into the
> `deploy-dags.yml` CI/CD workflow -- it only ever syncs `composer_dags/`, so nothing here will be
> redeployed automatically. To bring any of these back into service, copy the file into
> `composer_dags/` and push to `main`.

# FlowSentinel AI - Cloud Composer ETL Pipelines Suite (10 Production DAGs)

This directory contains a suite of **10 production-grade Apache Airflow ETL DAGs** designed for **Google Cloud Composer (Airflow 2.x)** and integrated with the **FlowSentinel AI Autonomous On-Call Framework**.

---

## 🏗 Standard 4-Tier Pipeline Architecture

Every DAG follows the strict enterprise Lakehouse ingestion pattern:

```
[ External / Vendor REST API ]
          │  (1. Extract with Retries & Rate-Limit Backoff)
          ▼
[ GCS Landing Bucket: Raw Layer ]
          │  (2. Store raw multi-layered JSON/CSV in partitioned paths)
          ▼
[ Staging Layer (GCS & BigQuery Staging Table) ]
          │  (3. Cleanse, mask PII, normalize schemas, strip symbols, deduplicate)
          ▼
[ BigQuery Analytical Curated Layer ]
             (4. Idempotent MERGE partitioned & clustered)
```

---

## 📋 Full Catalog of 10 Production ETL DAGs

| # | DAG File | Pipeline ID | Target Table | Schedule | FlowSentinel Failure Modes Covered |
| :- | :--- | :--- | :--- | :--- | :--- |
| **1** | [`dag_01_sales_orders_etl.py`](file:///C:/Users/ankush.kawale.lv/.gemini/antigravity/scratch/composer_dags/dag_01_sales_orders_etl.py) | `sales_transformation_pipeline` | `dw_analytics.fact_orders` | `@hourly` | `silent_corruption` (row drop), `schema_drift` (currency symbol), `orchestration_stall` |
| **2** | [`dag_02_crm_customers_etl.py`](file:///C:/Users/ankush.kawale.lv/.gemini/antigravity/scratch/composer_dags/dag_02_crm_customers_etl.py) | `crm_customer_sync_pipeline` | `dw_analytics.dim_customers` | `06:00 UTC` (SLA: `08:00 UTC`) | `delayed_missing_data` (watched CRM feed in Demo 4), `schema_drift`, `dq_check_failure` |
| **3** | [`dag_03_inventory_pricing_etl.py`](file:///C:/Users/ankush.kawale.lv/.gemini/antigravity/scratch/composer_dags/dag_03_inventory_pricing_etl.py) | `vendor_pricing_ingestion_pipeline` | `dw_analytics.fact_inventory_pricing` | `@hourly` | `api_failure` (HTTP 429 rate limit backoff), `long_running_query` |
| **4** | [`dag_04_marketing_attribution_etl.py`](file:///C:/Users/ankush.kawale.lv/.gemini/antigravity/scratch/composer_dags/dag_04_marketing_attribution_etl.py) | `marketing_attribution_pipeline` | `dw_analytics.fact_marketing_campaigns` | `04:00 UTC` | `missed_deadline`, `resource_exhaustion` |
| **5** | [`dag_05_payment_gateway_reconciliation_etl.py`](file:///C:/Users/ankush.kawale.lv/.gemini/antigravity/scratch/composer_dags/dag_05_payment_gateway_reconciliation_etl.py) | `payment_reconciliation_pipeline` | `dw_analytics.fact_payment_transactions` | `05:00 UTC` | `dq_check_failure` (webhook replay dedup), `silent_corruption` (audit variance) |
| **6** | [`dag_06_iot_telemetry_etl.py`](file:///C:/Users/ankush.kawale.lv/.gemini/antigravity/scratch/composer_dags/dag_06_iot_telemetry_etl.py) | `iot_device_telemetry_pipeline` | `dw_analytics.fact_iot_telemetry` | `@hourly` | `resource_exhaustion` (sensor skew/partition hotspotting), `silent_corruption` |
| **7** | [`dag_07_customer_support_tickets_etl.py`](file:///C:/Users/ankush.kawale.lv/.gemini/antigravity/scratch/composer_dags/dag_07_customer_support_tickets_etl.py) | `customer_support_tickets_pipeline` | `dw_analytics.fact_support_tickets` | `03:00 UTC` | `schema_drift` (dynamic ticket attributes), `api_failure` |
| **8** | [`dag_08_logistics_shipment_tracking_etl.py`](file:///C:/Users/ankush.kawale.lv/.gemini/antigravity/scratch/composer_dags/dag_08_logistics_shipment_tracking_etl.py) | `logistics_shipment_tracking_pipeline` | `dw_analytics.fact_shipment_tracking` | `07:00 UTC` | `late_arriving_records` (out-of-order scans), `urgent_backfill` |
| **9** | [`dag_09_clickstream_events_etl.py`](file:///C:/Users/ankush.kawale.lv/.gemini/antigravity/scratch/composer_dags/dag_09_clickstream_events_etl.py) | `clickstream_events_pipeline` | `dw_analytics.fact_user_clickstream` | `@hourly` | `storage_quota` (BigQuery slot/storage limits), `connection_pool` |
| **10** | [`dag_10_saas_billing_subscription_etl.py`](file:///C:/Users/ankush.kawale.lv/.gemini/antigravity/scratch/composer_dags/dag_10_saas_billing_subscription_etl.py) | `saas_billing_subscription_pipeline` | `dw_analytics.fact_subscriptions` | `02:00 UTC` | `urgent_backfill` (historical MRR recalculation), `long_running_query` |

---

## 🤖 Direct FlowSentinel AI Integration

Every single DAG imports and wires the shared alert plugin [`flowsentinel_alerts.py`](file:///C:/Users/ankush.kawale.lv/.gemini/antigravity/scratch/composer_dags/plugins/flowsentinel_alerts.py):

```python
from plugins.flowsentinel_alerts import (
    flowsentinel_failure_callback,
    flowsentinel_sla_miss_callback,
)

DEFAULT_ARGS = {
    "owner": "flowsentinel_data_eng",
    "on_failure_callback": flowsentinel_failure_callback,
}

with DAG(..., sla_miss_callback=flowsentinel_sla_miss_callback):
    ...
```

### What Happens On Failure:
1. Airflow captures the DAG ID, task ID, logical execution date, target BigQuery table, and full exception traceback.
2. It sends an HTTP `POST /api/tickets` request to FlowSentinel's backend.
3. FlowSentinel's **Issue Segment Agent** classifies the incident into one of the 14 categories (`schema_drift`, `api_failure`, `silent_corruption`, etc.).
4. The ticket is immediately assigned to either:
   - **Autonomous Fix Agent:** Performs self-healing without human intervention (e.g. exponential backoff, clearing zombie task states, quarantining corrupt partitions, or killing runaway queries).
   - **AI-Assisted Recommendation Agent:** Proposes a code patch, SQL adapter, or backfill blast-radius plan with 1-click human approval.

---

## 🚀 Deployment to Cloud Composer

### 1. Sync DAGs and Plugins to Composer GCS Bucket
```bash
# Obtain your Composer DAGs GCS bucket path
gcloud composer environments describe <COMPOSER_ENV_NAME> \
    --location <REGION> \
    --format="value(config.dagGcsPrefix)"

# Upload DAGs and plugins
gsutil cp -r composer_dags/*.py gs://<COMPOSER_DAGS_BUCKET>/
gsutil cp -r composer_dags/plugins/ gs://<COMPOSER_DAGS_BUCKET>/plugins/
```

### 2. Configure Airflow Variables (Admin -> Variables)
| Variable Key | Recommended Value | Purpose |
| :--- | :--- | :--- |
| `gcp_project_id` | `ctech-flowsentinel-ai` | GCP Project ID hosting BigQuery & Composer |
| `gcs_landing_bucket` | `ctech-flowsentinel-ai-landing-zone` | GCS Bucket for Raw & Staging datasets |
| `bq_staging_dataset` | `staging` | BigQuery dataset for staging tables |
| `bq_analytics_dataset` | `dw_analytics` | BigQuery dataset for production analytical tables |
| `flowsentinel_api_url` | `http://<flowsentinel-host>:8000` | FlowSentinel FastAPI backend URL |
| `flowsentinel_live_mode` | `live` or `visual` | Live GCP execution vs Demo visual mode |
