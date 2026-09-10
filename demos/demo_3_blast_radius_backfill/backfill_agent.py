#!/usr/bin/env python3
"""
Demo 3: Blast Radius & Backfill Plan Calculator Agent
Prepared by: CTech Data Engineers
"""
import os
import sys
import json
import time

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ctech-flowsentinel-demo-dev")

SAMPLE_LINEAGE_GRAPH = {
    "root_model": "raw_staging.orders",
    "downstream_models": [
        {"table": "dw_analytics.stg_orders", "partition_key": "order_date"},
        {"table": "dw_analytics.fact_orders", "partition_key": "order_date"},
        {"table": "dw_analytics.agg_daily_sales", "partition_key": "order_date"}
    ]
}

def calculate_blast_radius(start_date="2026-09-01", end_date="2026-09-07"):
    print("🎯 [Backfill Agent] Tracing DAG lineage & computing blast radius...")
    time.sleep(1)

    affected_partitions = [
        "2026-09-01", "2026-09-02", "2026-09-03", 
        "2026-09-04", "2026-09-05", "2026-09-06", "2026-09-07"
    ]
    
    bytes_per_partition_mb = 350
    total_data_gb = (len(affected_partitions) * bytes_per_partition_mb * len(SAMPLE_LINEAGE_GRAPH["downstream_models"])) / 1024.0
    estimated_cost_usd = total_data_gb * 0.005 # $5 per TB scanned in BigQuery

    backfill_plan = {
        "start_date": start_date,
        "end_date": end_date,
        "partition_count": len(affected_partitions),
        "affected_tables": [m["table"] for m in SAMPLE_LINEAGE_GRAPH["downstream_models"]],
        "estimated_data_scanned_gb": round(total_data_gb, 2),
        "estimated_bq_cost_usd": max(0.05, round(estimated_cost_usd, 2)),
        "recommended_concurrency": 2
    }

    print("\n========================================================================")
    print("📋 BACKFILL EXECUTION PLAN MATRIX")
    print("========================================================================")
    print(f"🗓️ Partition Range:       {start_date} to {end_date} ({len(affected_partitions)} Days)")
    print(f"📊 Downstream Tables:     {', '.join(backfill_plan['affected_tables'])}")
    print(f"💾 Total Data Scanned:   ~{backfill_plan['estimated_data_scanned_gb']} GB")
    print(f"💰 Estimated Compute Cost: ${backfill_plan['estimated_bq_cost_usd']} USD")
    print(f"⚙️ Safe Concurrency Limit: {backfill_plan['recommended_concurrency']} Parallel Workers")
    print("========================================================================\n")
    
    return backfill_plan

if __name__ == "__main__":
    calculate_blast_radius()
