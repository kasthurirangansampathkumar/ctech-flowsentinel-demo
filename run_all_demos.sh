#!/usr/bin/env bash
# ==============================================================================
# FlowSentinel AI - Master Demo Suite Launcher
# Prepared by: CTech Data Engineers
# ==============================================================================
set -e

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "========================================================================"
echo "🚀 FlowSentinel AI: Master Real-Time Demo Launcher Suite"
echo "Prepared by: CTech Data Engineers"
echo "========================================================================"
echo ""
echo "Select a Demo to run:"
echo "  [1] Demo 1: Autonomous Incident Triage & RCA (Gemini 2.0 + Pub/Sub + GitHub)"
echo "  [2] Demo 2: Autonomous Code Patch & 1-Click Approval (Gemini 2.0 + PR + Dashboard)"
echo "  [3] Demo 3: Code-Change Driven Precision Backfill (Blast-Radius + Quota Runner)"
echo "  [4] Demo 4: Upstream Source Feed Sentinel (SLA Monitor + Guardrails)"
echo "  [A] Run All 4 Demos Sequentially (Full End-to-End Suite)"
echo "========================================================================"

SELECTION="${1:-A}"

run_demo_1() {
  echo ""
  echo ">>> Launching Demo 1: Autonomous Triage & RCA <<<"
  bash "${BASE_DIR}/demos/demo_1_autonomous_triage/run_demo.sh"
}

run_demo_2() {
  echo ""
  echo ">>> Launching Demo 2: Code Patch & 1-Click Approval <<<"
  bash "${BASE_DIR}/demos/demo_2_code_patch_and_approval/run_demo.sh"
}

run_demo_3() {
  echo ""
  echo ">>> Launching Demo 3: Precision Code-Change Backfill <<<"
  bash "${BASE_DIR}/demos/demo_3_blast_radius_backfill/run_demo.sh"
}

run_demo_4() {
  echo ""
  echo ">>> Launching Demo 4: Source Feed Sentinel <<<"
  bash "${BASE_DIR}/demos/demo_4_source_feed_sentinel/run_demo.sh"
}

case "${SELECTION}" in
  1) run_demo_1 ;;
  2) run_demo_2 ;;
  3) run_demo_3 ;;
  4) run_demo_4 ;;
  A|a|all)
    run_demo_1
    run_demo_2
    run_demo_3
    run_demo_4
    ;;
  *)
    echo "Unknown option: ${SELECTION}. Running full suite..."
    run_demo_1
    run_demo_2
    run_demo_3
    run_demo_4
    ;;
esac

echo ""
echo "========================================================================"
echo "🎉 FlowSentinel AI Real-Time Demo Suite Completed Successfully!"
echo "========================================================================"
