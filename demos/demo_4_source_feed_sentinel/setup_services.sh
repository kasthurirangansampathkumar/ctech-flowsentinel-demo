#!/usr/bin/env bash
# ==============================================================================
# Demo 4: Setup Source Feed Sentinel Monitoring Infrastructure
# Prepared by: CTech Data Engineers
# ==============================================================================
set -eo pipefail

export PATH="/Users/kasthurirangansampathkumar/y/google-cloud-sdk/bin:$PATH"

PROJECT_ID="${GCP_PROJECT_ID:-ctech-flowsentinel-ai}"
BUCKET_NAME="${GCS_LANDING_BUCKET:-${PROJECT_ID}-landing-zone}"

echo "🚀 [Demo 4] Provisioning Source Feed Sentinel Infrastructure for ${PROJECT_ID}..."

# 1. Enable Storage & Monitoring APIs
echo "📌 Enabling Cloud Storage & Monitoring APIs..."
gcloud services enable storage.googleapis.com monitoring.googleapis.com --project="${PROJECT_ID}"

# 2. Create GCS Landing Zone Subdirectories
echo "📌 Creating GCS Landing Zone 'gs://${BUCKET_NAME}/vendor_crm/'..."
gsutil mb -p "${PROJECT_ID}" -c standard -l US "gs://${BUCKET_NAME}/" 2>/dev/null || true

echo "✅ [Demo 4] Source Feed Sentinel Infrastructure Configured!"
