#!/usr/bin/env bash
# ==============================================================================
# Demo 1: Execution Procedure Launcher
# Prepared by: CTech Data Engineers
# ==============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "========================================================================"
echo "🎬 FlowSentinel AI - Demo 1: Autonomous Incident Triage & RCA"
echo "========================================================================"

echo ""
echo "Step 1: Provisioning GCP Infrastructure & Pub/Sub Logging Sink..."
bash "${SCRIPT_DIR}/setup_services.sh" || echo "⚠️ GCP setup notice (dry-run mode active)"

echo ""
echo "Step 2: Seeding Initial Valid Data..."
python3 "${SCRIPT_DIR}/seed_data.py"

echo ""
echo "Step 3: Injecting Schema Drift Anomaly Failure..."
python3 "${SCRIPT_DIR}/inject_failure.py"

echo ""
echo "Step 4: Executing Gemini 2.0 Autonomous Triage Agent & Creating Ticket..."
python3 "${SCRIPT_DIR}/triage_agent.py"

echo ""
echo "========================================================================"
echo "✅ Demo 1 Execution Complete! Review generated RCA ticket above."
echo "========================================================================"
