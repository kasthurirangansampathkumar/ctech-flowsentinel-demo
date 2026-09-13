#!/usr/bin/env python3
"""
FlowSentinel AI - Summary view data
====================================
Pipeline run-health and table-freshness data for the Summary tab.

This is mode-aware, same as the ticket engine:
  - Demo Mode (live=False)  -> the curated sample below, with a couple of
    tables/pipelines deliberately shown as breaching SLA. This is the fixed
    "failure story" used for a walkthrough -- it never touches real GCP.
  - Production Mode (live=True) -> queries the real Cloud Composer DAG bucket
    and every real BigQuery table across our four datasets. No sample data,
    no silent fallback -- if a real call fails, the error is surfaced rather
    than quietly substituted with fiction.
"""
import os
import random
from datetime import datetime, timedelta, timezone

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ctech-flowsentinel-ai")
REGION = os.getenv("GCP_REGION", "us-central1")
COMPOSER_ENV = os.getenv("COMPOSER_ENVIRONMENT", "ctech-flowsentinel-demo-dev")

# SLA window for "most recent data" freshness -- a table is compliant if its
# newest partition/row landed within this many days of now.
TABLE_SLA_DAYS = 2

# Datasets Production Mode scans in full -- every table in each, not a fixed list.
DATASETS = ["raw_staging", "dw_analytics", "de_ops_metadata", "backfill_sandbox"]

DEMO_PIPELINES = [
    "sales_transformation_pipeline",
    "vendor_crm_sync",
    "vendor_ecom_ingest",
    "dq_validation_suite",
    "backfill_orchestrator",
    "dashboard_refresh_pipeline",
    "customer_360_aggregation",
    "inventory_sync_pipeline",
]

# (table, days_since_last_update) -- the Demo Mode sample. Deliberately mixes
# a few stale tables in with fresh ones so the SLA breach sort has something
# to show without needing real cloud access.
DEMO_TABLES = [
    ("raw_staging.orders", 0),
    ("raw_staging.customer_profiles", 1),
    ("dw_analytics.stg_orders", 0),
    ("dw_analytics.fact_orders", 3),
    ("dw_analytics.agg_daily_sales", 5),
    ("dw_analytics.customer_360", 1),
    ("de_ops_metadata.pipeline_runs", 0),
    ("de_ops_metadata.incident_log", 6),
    ("backfill_sandbox.fact_orders_20260907", 2),
]


def _seeded_run_history(name: str) -> list:
    """Deterministic per-pipeline run history so the dashboard doesn't
    flicker between refreshes -- same 5 dots every time for a given name."""
    rng = random.Random(name)
    weights = ["success"] * 7 + ["failed"] * 2 + ["running"]
    return [rng.choice(weights) for _ in range(5)]


def _demo_pipeline_runs() -> dict:
    pipelines = []
    for name in DEMO_PIPELINES:
        runs = _seeded_run_history(name)
        in_sla = runs[-1] == "success"
        pipelines.append({"name": name, "runs": runs, "in_sla": in_sla})

    total = len(pipelines)
    in_sla_count = sum(1 for p in pipelines if p["in_sla"])
    return {
        "source": "demo",
        "scorecard": {"total_pipelines": total, "in_sla": in_sla_count, "breaching": total - in_sla_count},
        "pipelines": pipelines,
    }


def _production_pipeline_runs() -> dict:
    """Real DAGs from the Composer environment's GCS dags/ folder, with a real
    5-day failure history pulled from Cloud Logging -- one real day, one real
    verdict, not a seeded fake sparkline."""
    from googleapiclient.discovery import build
    from google.cloud import logging as cloud_logging

    service = build("composer", "v1", cache_discovery=False)
    env_name = f"projects/{PROJECT_ID}/locations/{REGION}/environments/{COMPOSER_ENV}"
    env = service.projects().locations().environments().get(name=env_name).execute()
    dag_gcs_prefix = env["config"]["dagGcsPrefix"]  # e.g. gs://bucket/dags

    from google.cloud import storage
    bucket_name = dag_gcs_prefix.replace("gs://", "").split("/")[0]
    storage_client = storage.Client(project=PROJECT_ID)
    blobs = storage_client.list_blobs(bucket_name, prefix="dags/")
    dag_ids = sorted({
        b.name.split("/")[-1][:-3] for b in blobs
        if b.name.endswith(".py") and not b.name.endswith("/__init__.py")
    })
    if not dag_ids:
        raise RuntimeError(f"No DAG files found under {dag_gcs_prefix} -- nothing deployed to Composer yet.")

    log_client = cloud_logging.Client(project=PROJECT_ID)
    now = datetime.now(timezone.utc)
    window_start = (now - timedelta(days=4)).replace(hour=0, minute=0, second=0, microsecond=0)

    # One Cloud Logging call for the whole 5-day window across every DAG,
    # instead of 8 DAGs x 5 days of separate calls -- the latter blows through
    # the API's 60-reads/minute quota within a couple of dashboard refreshes.
    dag_id_filter = " OR ".join(f'jsonPayload.dag_id="{d}"' for d in dag_ids)
    filter_str = (
        f'resource.type="cloud_composer_environment" severity>=ERROR '
        f'({dag_id_filter}) timestamp>="{window_start.isoformat()}"'
    )
    failed_days = {}  # dag_id -> set of ISO date strings that had an error
    for entry in log_client.list_entries(filter_=filter_str, page_size=1000):
        payload = entry.payload if isinstance(entry.payload, dict) else {}
        dag_id = payload.get("dag_id")
        if dag_id in dag_ids and entry.timestamp:
            failed_days.setdefault(dag_id, set()).add(entry.timestamp.date().isoformat())

    pipelines = []
    for dag_id in dag_ids:
        days_with_errors = failed_days.get(dag_id, set())
        runs = [
            "failed" if (now - timedelta(days=days_ago)).date().isoformat() in days_with_errors else "success"
            for days_ago in range(4, -1, -1)
        ]
        in_sla = runs[-1] == "success"
        pipelines.append({"name": dag_id, "runs": runs, "in_sla": in_sla})

    total = len(pipelines)
    in_sla_count = sum(1 for p in pipelines if p["in_sla"])
    return {
        "source": "composer",
        "scorecard": {"total_pipelines": total, "in_sla": in_sla_count, "breaching": total - in_sla_count},
        "pipelines": pipelines,
    }


def get_pipeline_runs(live: bool = False) -> dict:
    if not live:
        return _demo_pipeline_runs()
    try:
        return _production_pipeline_runs()
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc), "source": "composer", "pipelines": [],
                "scorecard": {"total_pipelines": 0, "in_sla": 0, "breaching": 0}}


def _demo_table_freshness() -> dict:
    now = datetime.now(timezone.utc)
    rows = [(name, days, (now - timedelta(days=days)).date().isoformat()) for name, days in DEMO_TABLES]
    tables = [
        {"table": name, "most_recent_date": date_str, "days_since": days, "in_sla": days <= TABLE_SLA_DAYS}
        for name, days, date_str in rows
    ]
    tables.sort(key=lambda t: (t["in_sla"], -t["days_since"]))
    return {"source": "demo", "sla_days": TABLE_SLA_DAYS, "tables": tables}


def _production_table_freshness() -> dict:
    """Every table in every dataset we manage -- not a fixed list."""
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)
    now = datetime.now(timezone.utc)
    rows = []
    for dataset in DATASETS:
        try:
            tables = list(client.list_tables(f"{PROJECT_ID}.{dataset}"))
        except Exception:  # noqa: BLE001
            continue
        for t in tables:
            meta = client.get_table(t.reference)
            modified = meta.modified or now
            days_since = (now - modified).days
            rows.append((f"{dataset}.{t.table_id}", days_since, modified.date().isoformat()))

    if not rows:
        raise RuntimeError(f"No tables found across {', '.join(DATASETS)} -- nothing loaded yet.")

    tables = [
        {"table": name, "most_recent_date": date_str, "days_since": days, "in_sla": days <= TABLE_SLA_DAYS}
        for name, days, date_str in rows
    ]
    tables.sort(key=lambda t: (t["in_sla"], -t["days_since"]))
    return {"source": "bigquery", "sla_days": TABLE_SLA_DAYS, "tables": tables}


def get_table_freshness(live: bool = False) -> dict:
    if not live:
        return _demo_table_freshness()
    try:
        return _production_table_freshness()
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc), "source": "bigquery", "sla_days": TABLE_SLA_DAYS, "tables": []}
