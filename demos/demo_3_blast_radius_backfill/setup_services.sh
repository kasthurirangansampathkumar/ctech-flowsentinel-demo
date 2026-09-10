#!/usr/bin/env bash
# ==============================================================================
# Demo 3: Setup Backfill Sandbox & Dataform Dependencies
# Prepared by: CTech Data Engineers
# ==============================================================================
set -eo pipefail

export PATH="/Users/kasthurirangansampathkumar/y/google-cloud-sdk/bin:$PATH"

PROJECT_ID="${GCP_PROJECT_ID:-ctech-flowsentinel-ai}"
SANDBOX_DATASET="backfill_sandbox"

echo "🚀 [Demo 3] Provisioning Backfill Engine Infrastructure for ${PROJECT_ID}..."

# 1. Enable Dataform & BigQuery APIs
echo "📌 Enabling Dataform & BigQuery Storage APIs..."
gcloud services enable dataform.googleapis.com bigquerystorage.googleapis.com --project="${PROJECT_ID}"

# 2. Create Isolated Backfill Sandbox Dataset in BigQuery
echo "📌 Creating BigQuery Sandbox Dataset '${SANDBOX_DATASET}'..."
bq mk -d --location=US "${PROJECT_ID}:${SANDBOX_DATASET}" || true

echo "✅ [Demo 3] Backfill Sandbox Dataset Prepared!"
