#!/usr/bin/env bash
# ==============================================================================
# Demo 1: Setup GCP Services & Pub/Sub Logging Sink
# Prepared by: CTech Data Engineers
# ==============================================================================
set -eo pipefail

export PATH="/Users/kasthurirangansampathkumar/y/google-cloud-sdk/bin:$PATH"

PROJECT_ID="${GCP_PROJECT_ID:-ctech-flowsentinel-ai}"
REGION="${GCP_REGION:-us-central1}"
TOPIC_NAME="de-incidents-topic"
SUB_NAME="de-incidents-sub"
LOG_SINK_NAME="de-pipeline-failure-sink"
BQ_DATASET="de_ops_metadata"

echo "🚀 [Demo 1] Provisioning GCP Infrastructure for ${PROJECT_ID}..."

# 1. Enable Required GCP APIs
echo "📌 Enabling GCP APIs..."
gcloud services enable \
  composer.googleapis.com \
  bigquery.googleapis.com \
  pubsub.googleapis.com \
  logging.googleapis.com \
  aiplatform.googleapis.com \
  secretmanager.googleapis.com \
  --project="${PROJECT_ID}"

# 2. Create BigQuery Staging Datasets
echo "📌 Creating BigQuery Datasets..."
bq mk -d --location=US "${PROJECT_ID}:raw_staging" || true
bq mk -d --location=US "${PROJECT_ID}:dw_analytics" || true
bq mk -d --location=US "${PROJECT_ID}:${BQ_DATASET}" || true

# 3. Create Pub/Sub Topic & Subscription
echo "📌 Creating Pub/Sub Topic '${TOPIC_NAME}' and Subscription '${SUB_NAME}'..."
if ! gcloud pubsub topics describe "${TOPIC_NAME}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud pubsub topics create "${TOPIC_NAME}" --project="${PROJECT_ID}"
fi

if ! gcloud pubsub subscriptions describe "${SUB_NAME}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud pubsub subscriptions create "${SUB_NAME}" \
    --topic="${TOPIC_NAME}" \
    --project="${PROJECT_ID}"
fi

# 4. Create Cloud Logging Sink for Pipeline Failures
echo "📌 Configuring Cloud Logging Sink '${LOG_SINK_NAME}'..."
SINK_FILTER='resource.type="cloud_composer_environment" OR resource.type="bigquery_resource" severity>=ERROR'

if ! gcloud logging sinks describe "${LOG_SINK_NAME}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud logging sinks create "${LOG_SINK_NAME}" \
    "pubsub.googleapis.com/projects/${PROJECT_ID}/topics/${TOPIC_NAME}" \
    --log-filter="${SINK_FILTER}" \
    --project="${PROJECT_ID}"
fi

# Grant Pub/Sub Publisher role to Logging Sink Service Account
SINK_SA=$(gcloud logging sinks describe "${LOG_SINK_NAME}" --project="${PROJECT_ID}" --format="value(writerIdentity)")
echo "📌 Granting Pub/Sub Publisher role to sink SA: ${SINK_SA}"
gcloud pubsub topics add-iam-policy-binding "${TOPIC_NAME}" \
  --member="${SINK_SA}" \
  --role="roles/pubsub.publisher" \
  --project="${PROJECT_ID}"

echo "✅ [Demo 1] GCP Infrastructure Provisioned Successfully!"
