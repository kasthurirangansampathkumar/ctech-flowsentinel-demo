#!/usr/bin/env python3
"""
FlowSentinel AI - Ops Dashboard Backend (FastAPI)
Human-in-the-Loop control plane: read-only status views + the 1-click
approval / trigger actions that are the only human touchpoints in the
framework. Everything else (detection, RCA, patching, ticketing) is
performed by the agent scripts under /demos.

Secrets (GitHub PAT) are fetched server-side from GCP Secret Manager using
the caller's Application Default Credentials -- never hardcoded, never
logged, never sent to the frontend.
"""
import json
import os
import subprocess
import sys
from functools import lru_cache
from pathlib import Path

import certifi
import truststore
truststore.inject_into_ssl()  # verify TLS against the OS trust store (macOS Keychain),
                               # not just certifi's bundle -- needed behind local/corporate TLS inspection
import requests
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import summary_data
import ticket_engine

BASE_DIR = Path(__file__).resolve().parents[2]
DEMOS_DIR = BASE_DIR / "demos"
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "ctech-flowsentinel-ai")
GITHUB_OWNER = os.getenv("GITHUB_OWNER", "LatentView-Analytics-Ltd")
GITHUB_REPO = os.getenv("GITHUB_REPO", "ctech-flowsentinel-demo")
REGION = os.getenv("GCP_REGION", "us-central1")

# Gates the *mutating* endpoints only (approve-pr, run-backfill, workflows/run).
# Read-only status views stay open with no key -- that's the whole point of a
# public demo dashboard. Leave ADMIN_KEY unset to disable the gate (e.g. local dev).
ADMIN_KEY = os.getenv("DASHBOARD_ADMIN_KEY", "")

app = FastAPI(title="FlowSentinel AI Ops Dashboard")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


def is_live_mode(x_demo_mode: str = Header(default="visual")) -> bool:
    """Demo tab toggle: 'live' = real GCP/GitHub calls, anything else (default)
    = Visual Demo Only -- no external calls, no cost, safe to click freely."""
    return x_demo_mode == "live"


def require_admin_if_live(x_admin_key: str = Header(default=""), live: bool = Depends(is_live_mode)):
    """The admin-key gate only matters in Live GCP mode, where an action can
    actually merge a PR, spend BigQuery cost, or touch a real GitHub repo.
    Visual Demo mode never does any of that, so it's never gated."""
    if live and ADMIN_KEY and x_admin_key != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="Missing or invalid admin key")


@lru_cache(maxsize=1)
def get_github_token() -> str:
    """Fetch the GitHub PAT from GCP Secret Manager. Cached in-process only."""
    try:
        from google.cloud import secretmanager

        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{PROJECT_ID}/secrets/github-pat-token/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("utf-8").strip()
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] Could not load GitHub token from Secret Manager: {exc}")
        return os.getenv("GITHUB_PAT_TOKEN", "")


def github_headers():
    token = get_github_token()
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


@app.get("/api/health")
def health():
    return {"status": "ok", "project": PROJECT_ID, "repo": f"{GITHUB_OWNER}/{GITHUB_REPO}"}


@app.get("/api/pipelines")
def pipelines():
    """Real Cloud Composer environment status for this project.

    Uses the Composer REST API directly (via ADC) so this works unmodified on
    Cloud Run, where there's no `gcloud` CLI in the container -- only falls
    back to shelling out to `gcloud` for local dev boxes that have it but
    haven't run `gcloud auth application-default login` yet.
    """
    envs = []
    try:
        from googleapiclient.discovery import build

        service = build("composer", "v1", cache_discovery=False)
        parent = f"projects/{PROJECT_ID}/locations/{REGION}"
        envs = service.projects().locations().environments().list(parent=parent).execute().get("environments", [])
    except Exception as api_exc:  # noqa: BLE001
        try:
            result = subprocess.run(
                ["gcloud", "composer", "environments", "list",
                 f"--project={PROJECT_ID}", f"--locations={REGION}", "--format=json"],
                capture_output=True, text=True, timeout=30,
            )
            envs = json.loads(result.stdout) if result.stdout else []
        except Exception as cli_exc:  # noqa: BLE001
            return {"error": f"{api_exc} / {cli_exc}", "environments": []}

    out = []
    for env in envs:
        name = env.get("name", "").split("/")[-1]
        state = env.get("state", "UNKNOWN")
        out.append({
            "name": name,
            "state": state,
            "status_icon": "🟢" if state == "RUNNING" else "🔴",
        })
    return {"environments": out, "dag_watch": "sales_transformation_pipeline"}


@app.get("/api/incidents")
def incidents():
    """Live GitHub Issues labeled 'incident' -- the auto-created triage tickets."""
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/issues"
    try:
        resp = requests.get(url, headers=github_headers(), params={"state": "all", "labels": "incident"}, timeout=10, verify=certifi.where())
        resp.raise_for_status()
        issues = resp.json()
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc), "issues": []}

    return {"issues": [
        {
            "number": i["number"],
            "title": i["title"],
            "state": i["state"],
            "labels": [l["name"] for l in i.get("labels", [])],
            "url": i["html_url"],
        } for i in issues
    ]}


@app.get("/api/pull-requests")
def pull_requests():
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/pulls"
    try:
        resp = requests.get(url, headers=github_headers(), params={"state": "open"}, timeout=10, verify=certifi.where())
        resp.raise_for_status()
        prs = resp.json()
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc), "pull_requests": []}

    return {"pull_requests": [
        {"number": p["number"], "title": p["title"], "url": p["html_url"], "branch": p["head"]["ref"]}
        for p in prs
    ]}


@app.post("/api/approve-pr/{pr_number}", dependencies=[Depends(require_admin_if_live)])
def approve_pr(pr_number: int, live: bool = Depends(is_live_mode)):
    """The one human-in-the-loop action: 1-click merge + redeploy."""
    if not live:
        return {"merged": True, "pr": pr_number, "sha": "visual-demo-mode", "mode": "visual"}

    token = get_github_token()
    if not token:
        raise HTTPException(status_code=412, detail="GitHub token not available in Secret Manager")

    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/pulls/{pr_number}/merge"
    resp = requests.put(url, headers=github_headers(), json={"merge_method": "squash"}, timeout=15, verify=certifi.where())
    if resp.status_code >= 300:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    return {"merged": True, "pr": pr_number, "sha": resp.json().get("sha")}


# Every vendor feed the framework watches -- each with its own SLA deadline.
# Demo 4 only exercises vendor_crm, but a real on-call setup watches all inbound feeds.
WATCHED_FEEDS = [
    {"key": "vendor_ecom", "path": "vendor_ecom/orders_valid.csv", "sla_deadline": "06:00 UTC",
     "description": "Hourly e-commerce order feed"},
    {"key": "vendor_crm", "path": "vendor_crm/customer_profiles_2026-09-08.json", "sla_deadline": "08:00 UTC",
     "description": "Daily CRM customer profile feed"},
]


@app.get("/api/feed-status")
def feed_status():
    """Real GCS presence check for every watched vendor feed (not just one)."""
    bucket_name = os.getenv("GCS_LANDING_BUCKET", f"{PROJECT_ID}-landing-zone")
    try:
        from google.cloud import storage

        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(bucket_name)
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc), "feeds": []}

    feeds = []
    for feed in WATCHED_FEEDS:
        try:
            exists = bucket.blob(feed["path"]).exists()
        except Exception as exc:  # noqa: BLE001
            feeds.append({**feed, "file_present": None, "error": str(exc)})
            continue
        feeds.append({
            **feed,
            "file_present": exists,
            "sla_breached": not exists,
            "dag_state": "RUNNING" if exists else "PAUSED",
        })
    return {"bucket": bucket_name, "feeds": feeds}


# Backward-compatible single-feed alias used by earlier dashboard builds.
@app.get("/api/feed-sla")
def feed_sla():
    status = feed_status()
    crm = next((f for f in status.get("feeds", []) if f["key"] == "vendor_crm"), None)
    if not crm:
        return status
    return {
        "expected_file": crm["path"],
        "sla_deadline": crm["sla_deadline"],
        "file_present": crm["file_present"],
        "sla_breached": crm.get("sla_breached"),
        "dag_state": crm.get("dag_state"),
    }


# The four agent workflows -- everything the framework automates end to end.
# Human involvement stops at approve-pr / run-backfill above; these are the
# detection/generation steps an on-call engineer would otherwise do by hand.
WORKFLOWS = {
    "triage": {
        "label": "Run Autonomous Triage (Demo 1)",
        "description": "Gemini parses the failure log and files a GitHub incident issue.",
        "script": DEMOS_DIR / "demo_1_autonomous_triage" / "triage_agent.py",
    },
    "inject-failure": {
        "label": "Inject Schema Drift (Demo 1 setup)",
        "description": "Simulates a vendor schema drift to trigger the pipeline failure.",
        "script": DEMOS_DIR / "demo_1_autonomous_triage" / "inject_failure.py",
    },
    "code-repair": {
        "label": "Generate Code Patch (Demo 2)",
        "description": "Gemini drafts the SQL fix and opens a PR for human approval.",
        "script": DEMOS_DIR / "demo_2_code_patch_and_approval" / "code_repair_agent.py",
    },
    "feed-check": {
        "label": "Check Feed SLA (Demo 4)",
        "description": "Checks vendor feed arrival and auto-pauses the DAG on breach.",
        "script": DEMOS_DIR / "demo_4_source_feed_sentinel" / "feed_sentinel.py",
    },
    "feed-resume": {
        "label": "Simulate Feed Landing & Resume (Demo 4)",
        "description": "Simulates the delayed file landing and auto-resumes the DAG.",
        "script": DEMOS_DIR / "demo_4_source_feed_sentinel" / "resume_pipeline.py",
    },
}


@app.get("/api/workflows")
def list_workflows():
    return {"workflows": [{"key": k, "label": v["label"], "description": v["description"]} for k, v in WORKFLOWS.items()]}


@app.post("/api/workflows/{key}/run", dependencies=[Depends(require_admin_if_live)])
def run_workflow(key: str, live: bool = Depends(is_live_mode)):
    workflow = WORKFLOWS.get(key)
    if not workflow:
        raise HTTPException(status_code=404, detail=f"Unknown workflow '{key}'")

    log = ticket_engine.run_agent_script(workflow["script"], live=live)
    return {"ok": True, "log": log[-4000:], "errors": "", "mode": "live" if live else "visual"}


# ---------------------------------------------------------------------------
# Demo Console: ticket creation, the segmenting agent, and the two agent
# behaviors (autonomous fix / AI-assisted recommendation + approval).
# ---------------------------------------------------------------------------

def _mirror_to_github(ticket: "ticket_engine.Ticket", live: bool = True):
    """Best-effort: file a real GitHub Issue for the ticket. Skipped entirely
    in Visual Demo mode, and never blocks the demo if the token isn't
    available either way -- the in-memory ticket board is the source of
    truth regardless."""
    if not live:
        return
    token = get_github_token()
    if not token:
        return
    labels = ["incident", ticket.automation_level or "unclassified"]
    body = f"{ticket.description}\n\n**Inputs:** `{ticket.inputs}`\n\n" + "\n".join(f"- {line}" for line in ticket.log)
    try:
        resp = requests.post(
            f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/issues",
            headers=github_headers(), timeout=10, verify=certifi.where(),
            json={"title": ticket.title, "body": body, "labels": labels},
        )
        if resp.status_code < 300:
            ticket.github_issue_url = resp.json().get("html_url")
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] Could not mirror ticket to GitHub Issues: {exc}")


class NewTicketRequest(BaseModel):
    category: str = "auto"
    pipeline: str = ""
    table: str = ""
    message: str = ""


@app.get("/api/failure-catalog")
def failure_catalog():
    return {"failures": [
        {
            "key": f.key, "label": f.label, "category": f.category,
            "automation_level": f.automation_level, "placeholder": f.placeholder,
        } for f in ticket_engine.FAILURE_CATALOG
    ]}


@app.get("/api/tickets")
def list_tickets():
    return {"tickets": [t.to_dict() for t in ticket_engine.STORE.all()]}


@app.post("/api/tickets/sync-github")
def sync_tickets_from_github(live: bool = Depends(is_live_mode)):
    """Production Mode's ticket source: pull open incident issues straight from
    the GitHub Projects board instead of manual injection. A no-op in Demo
    Mode, since Demo Mode never touches real GitHub."""
    if not live:
        return {"error": "GitHub sync only runs in Production Mode.", "synced": []}

    token = get_github_token()
    if not token:
        raise HTTPException(status_code=412, detail="GitHub token not available in Secret Manager")

    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/issues"
    resp = requests.get(url, headers=github_headers(), params={"state": "open", "labels": "incident"},
                        timeout=10, verify=certifi.where())
    if resp.status_code >= 300:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    known = ticket_engine.STORE.known_github_issue_numbers()
    synced = []
    for issue in resp.json():
        if issue["number"] in known:
            continue
        ticket = ticket_engine.STORE.create(
            title=issue["title"],
            description=issue.get("body") or "(no description provided in the GitHub issue)",
            inputs={"pipeline": "", "table": "", "message": ""},
            source="github",
            github_issue_url=issue["html_url"],
            github_issue_number=issue["number"],
        )
        synced.append(ticket.to_dict())
    return {"synced": synced, "total_open_issues": len(resp.json())}


@app.post("/api/tickets")
def create_ticket(req: NewTicketRequest, live: bool = Depends(is_live_mode)):
    """Demo tab entry point: injects a failure and files it as a raw, unclassified
    ticket -- the Issue Segment Agent sorts it into a queue as a separate step
    on the Ticket Board, and starting the fix is a separate step after that."""
    if req.category != "auto" and req.category in ticket_engine.CATALOG_BY_KEY:
        failure = ticket_engine.CATALOG_BY_KEY[req.category]
        description = req.message or failure.placeholder
        title = f"[{failure.label}] {req.pipeline or 'pipeline'} incident"
    else:
        description = req.message or "Unclassified failure reported by on-call."
        title = f"[On-Call] {req.pipeline or 'pipeline'} incident"

    ticket = ticket_engine.STORE.create(
        title=title, description=description,
        inputs={"pipeline": req.pipeline, "table": req.table, "message": req.message},
    )
    _mirror_to_github(ticket, live=live)
    return ticket.to_dict()


def _get_ticket_or_404(ticket_id: str):
    ticket = ticket_engine.STORE.get(ticket_id)
    if not ticket:
        raise HTTPException(status_code=404, detail="Unknown ticket")
    return ticket


@app.post("/api/tickets/{ticket_id}/classify")
def classify_ticket(ticket_id: str, live: bool = Depends(is_live_mode)):
    """The Issue Segment Agent, run against one ticket."""
    ticket = _get_ticket_or_404(ticket_id)
    ticket_engine.classify_ticket(ticket, live=live)
    return ticket.to_dict()


@app.post("/api/tickets/classify-all")
def classify_all_tickets(live: bool = Depends(is_live_mode)):
    """Issue Segment Agent, run against every unclassified ticket at once --
    the board's main 'Run Issue Segment Agent' button."""
    classified = []
    for ticket in ticket_engine.STORE.all():
        if ticket.status == ticket_engine.NEW:
            ticket_engine.classify_ticket(ticket, live=live)
            classified.append(ticket.id)
    return {"classified": classified}


@app.post("/api/tickets/{ticket_id}/start-fix", dependencies=[Depends(require_admin_if_live)])
def start_fix(ticket_id: str, live: bool = Depends(is_live_mode)):
    ticket = _get_ticket_or_404(ticket_id)
    ticket_engine.start_autonomous_fix(ticket, live=live)
    return ticket.to_dict()


@app.post("/api/tickets/{ticket_id}/start-analysis", dependencies=[Depends(require_admin_if_live)])
def start_analysis(ticket_id: str, live: bool = Depends(is_live_mode)):
    ticket = _get_ticket_or_404(ticket_id)
    ticket_engine.start_assisted_analysis(ticket, live=live)
    return ticket.to_dict()


@app.post("/api/tickets/{ticket_id}/approve", dependencies=[Depends(require_admin_if_live)])
def approve_ticket(ticket_id: str, live: bool = Depends(is_live_mode)):
    ticket = _get_ticket_or_404(ticket_id)
    ticket_engine.approve_ticket(ticket, live=live)
    return ticket.to_dict()


@app.post("/api/demo/reset")
def reset_demo():
    ticket_engine.STORE.reset()
    return {"ok": True}


# A curated spread across automation levels -- 3 autonomous, 2 assisted, 1
# advisory -- so one click gives the Ticket Board something worth walking
# through instead of a single ticket.
DEMO_SEED_SET = [
    "schema_drift", "delayed_missing_data", "resource_exhaustion",
    "orchestration_stall", "connection_pool", "credentials_iam",
]


@app.post("/api/demo/seed")
def seed_demo(live: bool = Depends(is_live_mode)):
    """The 'Begin Demo' button: injects a realistic set of failures at once,
    all left unclassified -- the Ticket Board's Issue Segment Agent and the
    per-ticket fix/analysis buttons are the next steps."""
    created = []
    for key in DEMO_SEED_SET:
        failure = ticket_engine.CATALOG_BY_KEY[key]
        pipeline = summary_data.PIPELINES[hash(key) % len(summary_data.PIPELINES)]
        ticket = ticket_engine.STORE.create(
            title=f"[{failure.label}] {pipeline} incident",
            description=failure.placeholder,
            inputs={"pipeline": pipeline, "table": "", "message": ""},
        )
        _mirror_to_github(ticket, live=live)
        created.append(ticket.to_dict())
    return {"created": created}


@app.get("/api/summary/pipelines")
def summary_pipelines():
    return summary_data.get_pipeline_runs()


@app.get("/api/summary/tables")
def summary_tables():
    return summary_data.get_table_freshness()


@app.get("/api/agents")
def list_agents():
    """Documents every real AI agent in the framework -- purpose, inputs,
    grounding data, and what it decides. Same data the slide deck is built
    from, so this endpoint and the deck can never drift from each other."""
    import ai_agents

    return {"agents": [
        {
            "key": a.key, "name": a.name, "stage": a.stage, "purpose": a.purpose,
            "inputs": a.inputs, "grounding_data": a.grounding_data,
            "decision_task": a.decision_task, "output": a.output,
            "fallback": a.fallback, "model": a.model,
        } for a in ai_agents.AGENT_SPECS
    ]}


app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
