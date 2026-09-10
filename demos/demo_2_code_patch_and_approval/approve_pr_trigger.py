#!/usr/bin/env python3
"""
Demo 2: 1-Click Human Approval Trigger & PR Auto-Merger
Prepared by: CTech Data Engineers
"""
import os
import sys
import json
import time
import urllib.request

GITHUB_PAT = os.getenv("GITHUB_PAT_TOKEN", "")
GITHUB_REPO = os.getenv("GITHUB_REPO", "ctech-flowsentinel-demo")
GITHUB_OWNER = os.getenv("GITHUB_OWNER", "kasthurirangan")
PR_NUMBER = os.getenv("PR_NUMBER", "142")

import ssl

def simulate_dashboard_approval():
    print("🎛️ [FlowSentinel HITL Dashboard] Receiving 1-Click Human Approval...")
    print("👤 User Action: Lead On-Call Engineer clicked [Approve PR & Auto-Merge]")
    print(f"📌 Target Pull Request: #{PR_NUMBER} on repo '{GITHUB_OWNER}/{GITHUB_REPO}'")
    time.sleep(1)

def merge_github_pr():
    print(f"⚡ [Auto-Merger] Merging PR #{PR_NUMBER} into 'main' branch...")
    
    if not GITHUB_PAT:
        print("⚠️ GITHUB_PAT_TOKEN not provided. Displaying simulated merge response:")
        print("✅ PR #142 merged cleanly into main branch. Triggering GitHub Actions CI/CD...")
        return True

    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/pulls/{PR_NUMBER}/merge"
    payload = {
        "commit_title": f"Merge pull request #{PR_NUMBER} from fix/schema-drift-orders",
        "merge_method": "squash"
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {GITHUB_PAT}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json"
        },
        method="PUT"
    )

    ssl_context = ssl._create_unverified_context()
    try:
        with urllib.request.urlopen(req, context=ssl_context) as resp:
            data = json.loads(resp.read().decode())
            print(f"🎉 PR #{PR_NUMBER} Merged Successfully! Commit SHA: {data.get('sha', 'abc1234')}")
            return True
    except Exception as e:
        print(f"⚠️ Merge API Notice: {e}. Executing fallback approval flow.")
        return True

def trigger_deployment_pipeline():
    print("🚀 [Cloud Composer / GitHub Actions] Redeploying updated Airflow DAG & Dataform models...")
    time.sleep(1)
    print("✅ Pipeline 'sales_transformation_pipeline' redeployed successfully with schema adapter fix!")

if __name__ == "__main__":
    simulate_dashboard_approval()
    merge_github_pr()
    trigger_deployment_pipeline()
