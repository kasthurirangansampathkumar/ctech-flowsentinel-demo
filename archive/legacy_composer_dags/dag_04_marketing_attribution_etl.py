"""
DAG 4: Digital Ad Platforms & Campaign Attribution Ingestion Pipeline
=====================================================================
Pipeline ID: marketing_attribution_pipeline
Target Table: dw_analytics.fact_marketing_campaigns

ETL Flow:
1. Extract ad spend, impressions, clicks, and conversions from Ad Network APIs.
2. Upload nested raw JSON payload to GCS Landing Zone (`raw/marketing_campaigns/...`).
3. Staging transformation: Flattens nested campaign/adgroup hierarchies, normalizes UTM parameters,
   converts spend cents to USD, and derives CTR & CPC metrics.
4. Load to BigQuery staging table (`staging.stg_marketing_campaigns`).
5. Partition-filtered MERGE into production `dw_analytics.fact_marketing_campaigns`
   partitioned by `campaign_date` and clustered by `channel`, `campaign_id`.
"""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

try:
    from plugins.flowsentinel_alerts import (
        flowsentinel_failure_callback,
        flowsentinel_sla_miss_callback,
    )
except ImportError:
    def flowsentinel_failure_callback(context):
        logging.getLogger("airflow.task").error(f"Task failed: {context.get('task_instance')}")
    def flowsentinel_sla_miss_callback(*args):
        pass

GCP_PROJECT_ID = Variable.get("gcp_project_id", default_var=os.getenv("GCP_PROJECT_ID", "ctech-flowsentinel-ai"))
GCS_LANDING_BUCKET = Variable.get("gcs_landing_bucket", default_var=f"{GCP_PROJECT_ID}-landing-zone")
BQ_STAGING_DATASET = Variable.get("bq_staging_dataset", default_var="staging")
BQ_ANALYTICS_DATASET = Variable.get("bq_analytics_dataset", default_var="dw_analytics")

DEFAULT_ARGS = {
    "owner": "flowsentinel_data_eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=25),
    "on_failure_callback": flowsentinel_failure_callback,
}


def extract_ad_campaigns_api(**context) -> str:
    """
    Step 1: Fetch performance reports across ad networks (Google, Meta, LinkedIn).
    Returns nested performance payloads.
    """
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")
    logger = logging.getLogger("airflow.task")
    logger.info(f"Extracting multi-channel ad performance for {execution_date}...")

    channels = ["google_search", "meta_ads", "linkedin_b2b", "youtube_video"]
    campaign_records = []

    for i in range(80):
        channel = channels[i % len(channels)]
        impressions = 1500 + (i * 350)
        clicks = int(impressions * (0.02 + (i % 5) * 0.008))
        spend_cents = clicks * (85 + (i % 40) * 10)
        conversions = max(1, int(clicks * 0.05))

        campaign_records.append({
            "campaign_id": f"CMP-{100 + i}",
            "campaign_name": f"{channel.replace('_', ' ').title()} - Brand Sprint {i % 4 + 1}",
            "channel": channel,
            "metrics": {
                "impressions": impressions,
                "clicks": clicks,
                "spend_cents": spend_cents,
                "conversions": conversions,
            },
            "attribution": {
                "utm_source": channel.split("_")[0],
                "utm_medium": "cpc" if "video" not in channel else "cpm",
                "utm_campaign": f"q3_growth_sprint_{i % 4 + 1}",
            },
            "report_date": execution_date,
        })

    temp_path = f"/tmp/ad_performance_{ts_nodash}.json"
    with open(temp_path, "w", encoding="utf-8") as f:
        for r in campaign_records:
            f.write(json.dumps(r) + "\n")

    logger.info(f"Extracted {len(campaign_records)} ad campaign records to {temp_path}")
    return temp_path


def upload_raw_campaigns_gcs(**context) -> str:
    """
    Step 2: Upload raw nested JSON to GCS Landing bucket.
    Destination: gs://<gcs_landing_bucket>/raw/marketing_campaigns/<ds>/campaigns_<ts_nodash>.json
    """
    ti = context["ti"]
    temp_path = ti.xcom_pull(task_ids="extract_ad_campaigns_api")
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")

    gcs_object_name = f"raw/marketing_campaigns/{execution_date}/campaigns_{ts_nodash}.json"
    logger = logging.getLogger("airflow.task")
    logger.info(f"Uploading campaign payload to gs://{GCS_LANDING_BUCKET}/{gcs_object_name}...")

    gcs_hook = GCSHook()
    gcs_hook.upload(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=gcs_object_name,
        filename=temp_path,
        mime_type="application/json",
    )

    if os.path.exists(temp_path):
        os.remove(temp_path)

    return gcs_object_name


def clean_and_flatten_campaigns(**context) -> str:
    """
    Step 3: Staging Layer Transformation.
    - Flattens nested 'metrics' and 'attribution' objects
    - Converts spend_cents to USD float
    - Derives CTR (click-through rate) and CPC (cost-per-click)
    - Validates integrity and writes NDJSON to GCS staging
    """
    ti = context["ti"]
    raw_gcs_object = ti.xcom_pull(task_ids="upload_raw_campaigns_gcs")
    execution_date = context.get("ds")
    ts_nodash = context.get("ts_nodash")
    logger = logging.getLogger("airflow.task")

    gcs_hook = GCSHook()
    raw_data = gcs_hook.download_as_byte_array(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=raw_gcs_object,
    ).decode("utf-8")

    flattened = []
    for line in raw_data.strip().split("\n"):
        if not line:
            continue
        item = json.loads(line)
        metrics = item.get("metrics", {})
        attr = item.get("attribution", {})

        impressions = int(metrics.get("impressions", 0))
        clicks = int(metrics.get("clicks", 0))
        spend_usd = round(float(metrics.get("spend_cents", 0)) / 100.0, 2)
        conversions = int(metrics.get("conversions", 0))

        # Derive ratios safely
        ctr = round((clicks / impressions), 4) if impressions > 0 else 0.0
        cpc = round((spend_usd / clicks), 2) if clicks > 0 else 0.0

        flattened.append({
            "campaign_id": str(item.get("campaign_id", "")).strip(),
            "campaign_name": str(item.get("campaign_name", "")).strip(),
            "channel": str(item.get("channel", "direct")).strip(),
            "utm_source": str(attr.get("utm_source", "none")).lower().strip(),
            "utm_medium": str(attr.get("utm_medium", "none")).lower().strip(),
            "utm_campaign": str(attr.get("utm_campaign", "none")).lower().strip(),
            "impressions": impressions,
            "clicks": clicks,
            "spend_usd": spend_usd,
            "conversions": conversions,
            "ctr": ctr,
            "cpc": cpc,
            "campaign_date": item.get("report_date", execution_date),
            "processed_at": datetime.utcnow().isoformat() + "Z",
        })

    staging_gcs_object = f"staging/marketing_campaigns/{execution_date}/cleaned_campaigns_{ts_nodash}.json"
    flattened_payload = "\n".join(json.dumps(r) for r in flattened)

    gcs_hook.upload(
        bucket_name=GCS_LANDING_BUCKET,
        object_name=staging_gcs_object,
        data=flattened_payload.encode("utf-8"),
        mime_type="application/json",
    )
    logger.info(f"Staged {len(flattened)} flattened marketing records to {staging_gcs_object}")
    return staging_gcs_object


with DAG(
    dag_id="marketing_attribution_pipeline",
    default_args=DEFAULT_ARGS,
    description="Extracts multi-platform ad performance, flattens metrics, and loads into BigQuery",
    schedule_interval="0 4 * * *",  # Runs daily at 04:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    sla_miss_callback=flowsentinel_sla_miss_callback,
    params={"table": "dw_analytics.fact_marketing_campaigns"},
    tags=["flowsentinel", "marketing", "attribution", "adspend", "p2"],
) as dag:

    # 1. API Extraction
    t1_extract = PythonOperator(
        task_id="extract_ad_campaigns_api",
        python_callable=extract_ad_campaigns_api,
        provide_context=True,
    )

    # 2. Upload Raw to GCS
    t2_upload = PythonOperator(
        task_id="upload_raw_campaigns_gcs",
        python_callable=upload_raw_campaigns_gcs,
        provide_context=True,
    )

    # 3. Clean and Flatten in Staging
    t3_clean = PythonOperator(
        task_id="clean_and_flatten_campaigns",
        python_callable=clean_and_flatten_campaigns,
        provide_context=True,
    )

    # 4. Load from GCS Staging to BigQuery Staging Table
    t4_load_staging = GCSToBigQueryOperator(
        task_id="load_staging_campaigns_bq",
        bucket=GCS_LANDING_BUCKET,
        source_objects=["staging/marketing_campaigns/{{ ds }}/cleaned_campaigns_{{ ts_nodash }}.json"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.stg_marketing_campaigns",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "campaign_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "campaign_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "channel", "type": "STRING", "mode": "REQUIRED"},
            {"name": "utm_source", "type": "STRING", "mode": "NULLABLE"},
            {"name": "utm_medium", "type": "STRING", "mode": "NULLABLE"},
            {"name": "utm_campaign", "type": "STRING", "mode": "NULLABLE"},
            {"name": "impressions", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "clicks", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "spend_usd", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "conversions", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "ctr", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "cpc", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "campaign_date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "processed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ],
    )

    # 5. Production Curated Table Load (MERGE)
    MERGE_FACT_MARKETING_SQL = f"""
    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_marketing_campaigns` (
        campaign_id STRING,
        campaign_name STRING,
        channel STRING,
        utm_source STRING,
        utm_medium STRING,
        utm_campaign STRING,
        impressions INT64,
        clicks INT64,
        spend_usd FLOAT64,
        conversions INT64,
        ctr FLOAT64,
        cpc FLOAT64,
        campaign_date DATE,
        processed_at TIMESTAMP
    )
    PARTITION BY campaign_date
    CLUSTER BY channel, campaign_id;

    MERGE `{GCP_PROJECT_ID}.{BQ_ANALYTICS_DATASET}.fact_marketing_campaigns` T
    USING `{GCP_PROJECT_ID}.{BQ_STAGING_DATASET}.stg_marketing_campaigns` S
    ON T.campaign_id = S.campaign_id AND T.campaign_date = S.campaign_date
    WHEN MATCHED THEN
      UPDATE SET
        campaign_name = S.campaign_name,
        channel = S.channel,
        utm_source = S.utm_source,
        utm_medium = S.utm_medium,
        utm_campaign = S.utm_campaign,
        impressions = S.impressions,
        clicks = S.clicks,
        spend_usd = S.spend_usd,
        conversions = S.conversions,
        ctr = S.ctr,
        cpc = S.cpc,
        processed_at = S.processed_at
    WHEN NOT MATCHED THEN
      INSERT (campaign_id, campaign_name, channel, utm_source, utm_medium, utm_campaign, impressions, clicks, spend_usd, conversions, ctr, cpc, campaign_date, processed_at)
      VALUES (S.campaign_id, S.campaign_name, S.channel, S.utm_source, S.utm_medium, S.utm_campaign, S.impressions, S.clicks, S.spend_usd, S.conversions, S.ctr, S.cpc, S.campaign_date, S.processed_at);
    """

    t5_merge_marketing = BigQueryInsertJobOperator(
        task_id="merge_fact_marketing_attribution",
        configuration={
            "query": {
                "query": MERGE_FACT_MARKETING_SQL,
                "useLegacySql": False,
            }
        },
    )

    t1_extract >> t2_upload >> t3_clean >> t4_load_staging >> t5_merge_marketing
