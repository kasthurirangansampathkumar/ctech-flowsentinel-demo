#!/usr/bin/env bash
# ==============================================================================
# Demo 4: Execution Procedure Launcher
# Prepared by: CTech Data Engineers
# ==============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "========================================================================"
echo "🎬 FlowSentinel AI - Demo 4: Upstream Source Feed Sentinel"
echo "========================================================================"

echo ""
echo "Step 1: Provisioning Source Feed Sentinel Infrastructure..."
bash "${SCRIPT_DIR}/setup_services.sh" || echo "⚠️ Setup notice (dry-run mode)"

echo ""
echo "Step 2: Checking Feed Arrival SLA & Triggering SLA Breach Guardrail..."
python3 "${SCRIPT_DIR}/feed_sentinel.py"

echo ""
echo "Step 3: Simulating Vendor File Landing & Pipeline Auto-Resumption..."
python3 "${SCRIPT_DIR}/resume_pipeline.py"

echo ""
echo "========================================================================"
echo "✅ Demo 4 Execution Complete! Guardrails protected data freshness."
echo "========================================================================"
