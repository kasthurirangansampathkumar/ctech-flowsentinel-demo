#!/usr/bin/env bash
# ==============================================================================
# Demo 2: Setup Code Repair Service & GitHub Webhooks
# Prepared by: CTech Data Engineers
# ==============================================================================
set -eo pipefail

export PATH="/Users/kasthurirangansampathkumar/y/google-cloud-sdk/bin:$PATH"

PROJECT_ID="${GCP_PROJECT_ID:-ctech-flowsentinel-ai}"
SECRET_NAME="github-pat-token"

echo "🚀 [Demo 2] Provisioning Code Repair Agent Dependencies for ${PROJECT_ID}..."

# 1. Enable Cloud Run & Secret Manager APIs
echo "📌 Enabling Cloud Run & Secret Manager APIs..."
gcloud services enable run.googleapis.com secretmanager.googleapis.com --project="${PROJECT_ID}"

# 2. Check or Create GitHub PAT Secret
if ! gcloud secrets describe "${SECRET_NAME}" --project="${PROJECT_ID}" &>/dev/null; then
  echo "📌 Creating Secret Manager entry '${SECRET_NAME}'..."
  gcloud secrets create "${SECRET_NAME}" --replication-policy="automatic" --project="${PROJECT_ID}" || true
  echo "⚠️ Remember to add token version: echo -n 'YOUR_PAT' | gcloud secrets versions add ${SECRET_NAME} --data-file=-"
fi

echo "✅ [Demo 2] Service Dependencies Configured!"
