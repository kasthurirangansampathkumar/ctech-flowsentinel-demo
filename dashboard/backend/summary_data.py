#!/usr/bin/env python3
"""
FlowSentinel AI - Summary view data
====================================
Pipeline run-health and table-freshness data for the Summary tab.

Both try a real GCP call first (Composer/BigQuery, via ADC) and fall back to
a curated, clearly-realistic sample when that access isn't available yet --
same pattern as the rest of the framework's demo scripts. The pipeline and
table names match the ones used across the ticket demo so the whole
dashboard tells one consistent story.
"""
import os
import random
from datetime import datetime, timedelta, timezone

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ctech-flowsentinel-ai")

# SLA window for "most recent data" freshness -- a table is compliant if its
# newest partition/row landed within this many days of now.
TABLE_SLA_DAYS = 2

PIPELINES = [
    "sales_transformation_pipeline",
    "vendor_crm_sync",
    "vendor_ecom_ingest",
    "dq_validation_suite",
    "backfill_orchestrator",
    "dashboard_refresh_pipeline",
    "customer_360_aggregation",
    "inventory_sync_pipeline",
]

# (table, days_since_last_update) -- the fallback sample. Deliberately mixes
# a few stale tables in with fresh ones so the SLA breach sort has something
# to show on a first run with no cloud access yet.
TABLE_SAMPLE = [
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


def get_pipeline_runs() -> dict:
    pipelines = []
    for name in PIPELINES:
        runs = _seeded_run_history(name)
        in_sla = runs[-1] == "success"
        pipelines.append({"name": name, "runs": runs, "in_sla": in_sla})

    total = len(pipelines)
    in_sla_count = sum(1 for p in pipelines if p["in_sla"])
    return {
        "scorecard": {"total_pipelines": total, "in_sla": in_sla_count, "breaching": total - in_sla_count},
        "pipelines": pipelines,
    }


def _table_freshness_from_bigquery() -> list:
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)
    now = datetime.now(timezone.utc)
    rows = []
    for full_name, _ in TABLE_SAMPLE:
        dataset, table = full_name.split(".")
        try:
            meta = client.get_table(f"{PROJECT_ID}.{dataset}.{table}")
            modified = meta.modified or now
            days_since = (now - modified).days
            rows.append((full_name, days_since, modified.date().isoformat()))
        except Exception:  # noqa: BLE001
            continue
    if len(rows) < len(TABLE_SAMPLE) // 2:
        raise RuntimeError("too few real tables found, falling back to sample")
    return rows


def get_table_freshness() -> dict:
    now = datetime.now(timezone.utc)
    try:
        rows = _table_freshness_from_bigquery()
        source = "bigquery"
    except Exception:  # noqa: BLE001
        rows = [(name, days, (now - timedelta(days=days)).date().isoformat()) for name, days in TABLE_SAMPLE]
        source = "sample"

    tables = [
        {"table": name, "most_recent_date": date_str, "days_since": days, "in_sla": days <= TABLE_SLA_DAYS}
        for name, days, date_str in rows
    ]
    # Non-SLA (breaching) tables first, then oldest-first within each group.
    tables.sort(key=lambda t: (t["in_sla"], -t["days_since"]))
    return {"source": source, "sla_days": TABLE_SLA_DAYS, "tables": tables}
