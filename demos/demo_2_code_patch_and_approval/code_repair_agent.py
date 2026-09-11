#!/usr/bin/env python3
"""
Demo 2: Code Repair Agent & GitHub Pull Request Generator
Prepared by: CTech Data Engineers
"""
import os
import sys
import json
import urllib.request

GITHUB_PAT = os.getenv("GITHUB_PAT_TOKEN", "")
GITHUB_REPO = os.getenv("GITHUB_REPO", "ctech-flowsentinel-demo")
GITHUB_OWNER = os.getenv("GITHUB_OWNER", "LatentView-Analytics-Ltd")

ORIGINAL_BROKEN_SQL = """-- Staging orders transformation model
SELECT
  order_id,
  cust_id,
  CAST(order_date AS DATE) AS order_date,
  CAST(amount_usd AS FLOAT64) AS amount_usd,
  status
FROM `ctech-flowsentinel-demo-dev.raw_staging.orders`
"""

FIXED_SQL_PATCH = """-- Staging orders transformation model (Auto-patched by Gemini 2.0)
SELECT
  order_id,
  COALESCE(
    SAFE_CAST(cust_id AS STRING),
    SAFE_CAST(customer_identifier_v2 AS STRING)
  ) AS cust_id,
  CAST(order_date AS DATE) AS order_date,
  SAFE_CAST(REGEXP_REPLACE(CAST(amount_usd AS STRING), r'\\$', '') AS FLOAT64) AS amount_usd,
  status
FROM `ctech-flowsentinel-demo-dev.raw_staging.orders`
"""

import ssl
import certifi

def generate_code_patch():
    print("🤖 [Code Repair Agent] Generating SQL schema adapter patch using Gemini 2.0...")
    print("------------------------------------------------------------------------")
    print("📝 Original Broken SQL:\n", ORIGINAL_BROKEN_SQL)
    print("------------------------------------------------------------------------")
    print("✨ AI-Generated Fixed Patch:\n", FIXED_SQL_PATCH)
    print("------------------------------------------------------------------------")
    return FIXED_SQL_PATCH

def create_github_pr(fixed_sql):
    print("🐙 [GitHub PR Generator] Creating branch 'fix/schema-drift-orders' and Pull Request...")
    
    pr_body = f"""## 🛠️ Autonomous Code Fix & Schema Adapter
**Framework:** FlowSentinel AI | **Agent:** Code Repair Agent  
**Resolves Issue:** #42  

### 📝 Summary of Changes
1. **Schema Adapter Added**: Uses `COALESCE(cust_id, customer_identifier_v2)` to support legacy and updated vendor column headers gracefully.
2. **Regex Type Sanitization**: Strips currency string symbols (`$`) from `amount_usd` before casting to `FLOAT64`.

```sql
{fixed_sql}
```

### 🎛️ Human-in-the-Loop Action Required
Click **`[Approve & Auto-Deploy]`** on the FlowSentinel Ops Dashboard to merge this PR and trigger historical backfills.
"""

    if not GITHUB_PAT:
        print("⚠️ GITHUB_PAT_TOKEN not set. Displaying simulated PR response:")
        print(f"🎉 Simulated PR #142 Created: https://github.com/{GITHUB_OWNER}/{GITHUB_REPO}/pull/142")
        return "https://github.com/LatentView-Analytics-Ltd/ctech-flowsentinel-demo/pull/142 (Simulated)"

    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/pulls"
    payload = {
        "title": "🛠️ [Auto-Fix] Add Schema Drift Adapter & Currency Regex Sanitizer for Orders",
        "head": "fix/schema-drift-orders",
        "base": "main",
        "body": pr_body
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {GITHUB_PAT}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json"
        },
        method="POST"
    )

    ssl_context = ssl.create_default_context(cafile=certifi.where())
    try:
        with urllib.request.urlopen(req, context=ssl_context) as resp:
            data = json.loads(resp.read().decode())
            pr_url = data.get("html_url", "")
            print(f"🎉 Successfully created Pull Request: {pr_url}")
            return pr_url
    except Exception as e:
        print(f"❌ Failed to post PR: {e}")
        return "https://github.com/LatentView-Analytics-Ltd/ctech-flowsentinel-demo/pull/142 (Fallback)"

def run_repair_agent():
    print("🚀 [Demo 2] Starting Code Repair Agent...")
    patch = generate_code_patch()
    pr_url = create_github_pr(patch)
    print(f"✅ Code Repair Complete! PR awaiting 1-click human approval: {pr_url}")

if __name__ == "__main__":
    run_repair_agent()
