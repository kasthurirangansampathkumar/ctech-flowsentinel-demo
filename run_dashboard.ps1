# ==============================================================================
# SentinelView AI - Ops Dashboard Launcher (Windows PowerShell)
# Mirrors run_dashboard.sh -- same env vars, same behavior.
# ==============================================================================
$ErrorActionPreference = "Stop"
$BaseDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Join-Path $BaseDir "dashboard")

if (-not (Test-Path ".venv")) {
    Write-Host "Creating Python virtual environment..."
    python -m venv .venv
    & ".venv\Scripts\pip.exe" install --quiet -r backend\requirements.txt certifi truststore
}

# On a machine behind a corporate proxy/TLS inspection, Python's own CA bundle
# can reject valid certs even though the browser trusts them. certifi is the
# baseline fix; ai_agents.py/main.py also use `truststore` (installed above)
# to fall back to the OS trust store when certifi alone isn't enough.
$env:SSL_CERT_FILE = & ".venv\Scripts\python.exe" -c "import certifi; print(certifi.where())"
$env:REQUESTS_CA_BUNDLE = $env:SSL_CERT_FILE

if (-not $env:GCP_PROJECT_ID)      { $env:GCP_PROJECT_ID = "ctech-flowsentinel-ai" }
if (-not $env:GITHUB_OWNER)        { $env:GITHUB_OWNER = "LatentView-Analytics-Ltd" }
if (-not $env:GITHUB_REPO)         { $env:GITHUB_REPO = "ctech-flowsentinel-demo" }
if (-not $env:GCS_LANDING_BUCKET)  { $env:GCS_LANDING_BUCKET = "$($env:GCP_PROJECT_ID)-landing-zone" }

# Gates Production Mode's mutating actions (approve, start-fix, start-analysis,
# run-backfill, workflows). Demo Mode never checks this. Override by setting
# $env:DASHBOARD_ADMIN_KEY before running this script.
if (-not $env:DASHBOARD_ADMIN_KEY) { $env:DASHBOARD_ADMIN_KEY = "test123" }

Write-Host "Starting SentinelView AI Ops Dashboard on http://localhost:3000"
Set-Location backend
& "..\.venv\Scripts\uvicorn.exe" main:app --host 0.0.0.0 --port 3000
