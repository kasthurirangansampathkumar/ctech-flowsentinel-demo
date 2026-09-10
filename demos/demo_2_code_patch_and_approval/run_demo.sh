#!/usr/bin/env bash
# ==============================================================================
# Demo 2: Execution Procedure Launcher
# Prepared by: CTech Data Engineers
# ==============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "========================================================================"
echo "🎬 FlowSentinel AI - Demo 2: Autonomous Code Patch & 1-Click Approval"
echo "========================================================================"

echo ""
echo "Step 1: Provisioning Code Repair Agent Dependencies..."
bash "${SCRIPT_DIR}/setup_services.sh" || echo "⚠️ Setup notice (dry-run mode)"

echo ""
echo "Step 2: Executing Gemini 2.0 Code Repair Agent & Generating PR..."
python3 "${SCRIPT_DIR}/code_repair_agent.py"

echo ""
echo "Step 3: Simulating 1-Click Human Approval from Cloud Run Dashboard..."
python3 "${SCRIPT_DIR}/approve_pr_trigger.py"

echo ""
echo "========================================================================"
echo "✅ Demo 2 Execution Complete! PR merged and pipeline redeployed."
echo "========================================================================"
