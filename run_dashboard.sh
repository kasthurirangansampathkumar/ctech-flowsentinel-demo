#!/usr/bin/env bash
# ==============================================================================
# FlowSentinel AI - Ops Dashboard Launcher
# ==============================================================================
set -eo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${BASE_DIR}/dashboard"

if [ ! -d ".venv" ]; then
  echo "📌 Creating Python virtual environment..."
  python3 -m venv .venv
  ./.venv/bin/pip install --quiet -r backend/requirements.txt certifi
fi

# Homebrew Python doesn't ship a CA bundle -- point it at certifi's to fix
# SSL cert verification for GitHub API calls.
export SSL_CERT_FILE="$(./.venv/bin/python3 -c 'import certifi; print(certifi.where())')"
export REQUESTS_CA_BUNDLE="${SSL_CERT_FILE}"

export GCP_PROJECT_ID="${GCP_PROJECT_ID:-ctech-flowsentinel-ai}"
export GITHUB_OWNER="${GITHUB_OWNER:-LatentView-Analytics-Ltd}"
export GITHUB_REPO="${GITHUB_REPO:-ctech-flowsentinel-demo}"
export GCS_LANDING_BUCKET="${GCS_LANDING_BUCKET:-${GCP_PROJECT_ID}-landing-zone}"

# Gates Production Mode's mutating actions (approve, start-fix, start-analysis,
# run-backfill, workflows). Demo Mode never checks this. Override by exporting
# DASHBOARD_ADMIN_KEY before running this script.
export DASHBOARD_ADMIN_KEY="${DASHBOARD_ADMIN_KEY:-test123}"

echo "🚀 Starting FlowSentinel AI Ops Dashboard on http://localhost:3000"
cd backend && ../.venv/bin/uvicorn main:app --host 0.0.0.0 --port 3000
