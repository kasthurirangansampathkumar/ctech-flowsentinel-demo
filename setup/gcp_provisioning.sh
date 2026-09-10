#!/usr/bin/env bash
# ==============================================================================
# FlowSentinel AI - GCP Infrastructure Provisioning Script
# Prepared by: CTech Data Engineers
# ==============================================================================
set -eo pipefail

export PATH="/Users/kasthurirangansampathkumar/y/google-cloud-sdk/bin:$PATH"

PROJECT_ID="${GCP_PROJECT_ID:-ctech-flowsentinel-ai}"
PROJECT_NUMBER="${GCP_PROJECT_NUMBER:-673338764809}"
REGION="${GCP_REGION:-us-central1}"
BUCKET_NAME="${GCS_LANDING_BUCKET:-${PROJECT_ID}-landing-zone}"
TOPIC_NAME="de-incidents-topic"
SUB_NAME="de-incidents-sub"
LOG_SINK_NAME="de-pipeline-failure-sink"

echo "========================================================================"
echo "🚀 Provisioning GCP Infrastructure for Project: ${PROJECT_ID}"
echo "   Project Number: ${PROJECT_NUMBER}"
echo "========================================================================"

# 1. Set Active GCP Project
echo "📌 Setting active GCP project to '${PROJECT_ID}'..."
gcloud config set project "${PROJECT_ID}"

# 2. Enable All 13 Required GCP Services & APIs
echo "📌 Enabling 13 GCP APIs..."
gcloud services enable \
  composer.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  bigquery.googleapis.com \
  bigquerystorage.googleapis.com \
  dataform.googleapis.com \
  storage.googleapis.com \
  pubsub.googleapis.com \
  aiplatform.googleapis.com \
  generativelanguage.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com \
  secretmanager.googleapis.com \
  --project="${PROJECT_ID}"

# 3. Create BigQuery Datasets
echo "📌 Creating BigQuery Datasets..."
bq mk -d --location=US "${PROJECT_ID}:raw_staging" || true
bq mk -d --location=US "${PROJECT_ID}:dw_analytics" || true
bq mk -d --location=US "${PROJECT_ID}:de_ops_metadata" || true
bq mk -d --location=US "${PROJECT_ID}:backfill_sandbox" || true

# 4. Create GCS Landing Zone Buckets
echo "📌 Creating GCS Landing Bucket 'gs://${BUCKET_NAME}'..."
gsutil mb -p "${PROJECT_ID}" -c standard -l US "gs://${BUCKET_NAME}/" 2>/dev/null || true

# 5. Create Pub/Sub Topic & Subscription
echo "📌 Creating Pub/Sub Topic '${TOPIC_NAME}'..."
if ! gcloud pubsub topics describe "${TOPIC_NAME}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud pubsub topics create "${TOPIC_NAME}" --project="${PROJECT_ID}"
fi

if ! gcloud pubsub subscriptions describe "${SUB_NAME}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud pubsub subscriptions create "${SUB_NAME}" \
    --topic="${TOPIC_NAME}" \
    --project="${PROJECT_ID}"
fi

# 6. Create Cloud Logging Sink for Pipeline Failures
echo "📌 Configuring Cloud Logging Sink '${LOG_SINK_NAME}'..."
SINK_FILTER='resource.type="cloud_composer_environment" OR resource.type="bigquery_resource" severity>=ERROR'

if ! gcloud logging sinks describe "${LOG_SINK_NAME}" --project="${PROJECT_ID}" &>/dev/null; then
  gcloud logging sinks create "${LOG_SINK_NAME}" \
    "pubsub.googleapis.com/projects/${PROJECT_ID}/topics/${TOPIC_NAME}" \
    --log-filter="${SINK_FILTER}" \
    --project="${PROJECT_ID}" || echo "⚠️ Cloud Logging Sink creation notice (Requires Logging Admin role on GCP)"
fi

# Grant Pub/Sub Publisher role to Logging Sink SA
SINK_SA=$(gcloud logging sinks describe "${LOG_SINK_NAME}" --project="${PROJECT_ID}" --format="value(writerIdentity)" 2>/dev/null || echo "")
if [ -n "${SINK_SA}" ]; then
  gcloud pubsub topics add-iam-policy-binding "${TOPIC_NAME}" \
    --member="${SINK_SA}" \
    --role="roles/pubsub.publisher" \
    --project="${PROJECT_ID}" || true
fi

echo "========================================================================"
echo "✅ GCP Infrastructure Provisioning Complete for ${PROJECT_ID}!"
echo "========================================================================"
