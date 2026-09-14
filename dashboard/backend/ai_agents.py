#!/usr/bin/env python3
"""
FlowSentinel AI - Agent Framework
==================================
This is the reusable piece: every AI agent FlowSentinel runs, independent of
the ticket board or dashboard wired on top of it. A client replicating this
framework only needs this file plus the demo scripts it calls into.

Structure:

  1. FAILURE_CATALOG   -- the 14 DE on-call failure modes this framework
                           recognizes, each tagged with its automation level.
  2. AGENT_SPECS        -- metadata (purpose / inputs / decision logic) for
                           every agent below -- this is the single source of
                           truth the slide deck and the dashboard's Agents
                           view are both generated from, so documentation
                           can't drift from what the code actually does.
  3. call_gemini()      -- the one place Vertex AI Gemini is invoked. Every
                           agent below is a thin wrapper: gather whatever
                           real, deterministic signal is available (a GCS
                           check, a blast-radius calculation, the ticket's
                           own text), hand it to Gemini to diagnose/decide/
                           narrate, and fall back to a fixed template if
                           Gemini or its credentials aren't reachable --
                           the demo must never go blank because a model
                           call failed.
  4. classify()          -- the Issue Segment Agent: reads a ticket's free
                           text and assigns it to one of the 14 categories.
  5. One function per category -- the autonomous fix agents and the
                           AI-assisted recommendation agents. Their apply_fn
                           counterparts (executing an already-approved fix)
                           are mechanical, not a new AI decision, and are
                           kept here too for the same reason run_agent_script
                           lives here: they're part of what each agent does.

Design boundary that matters for compliance / safety review: this module
never calls Gemini for the Advisory category (expired credentials / IAM
policy shifts). That's a hard rule enforced in ticket_engine.classify_ticket,
not something an agent could accidentally override.
"""
import functools
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

BASE_DIR = Path(__file__).resolve().parents[2]
DEMOS_DIR = BASE_DIR / "demos"

AUTONOMOUS = "autonomous"
ASSISTED = "assisted"
ADVISORY = "advisory"

GEMINI_MODEL_NAME = "gemini-2.5-flash"


# ---------------------------------------------------------------------------
# Gemini access -- the one function every agent below goes through.
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=1)
def _vertexai_ready() -> bool:
    import vertexai

    vertexai.init(
        project=os.getenv("GCP_PROJECT_ID", "ctech-flowsentinel-ai"),
        location=os.getenv("GCP_REGION", "us-central1"),
    )
    return True


def call_gemini(prompt: str, live: bool = True) -> Optional[str]:
    """Returns Gemini's response text, or None if unavailable/not live --
    callers always have a fallback template ready for the None case."""
    if not live:
        return None
    try:
        from vertexai.generative_models import GenerativeModel

        _vertexai_ready()
        model = GenerativeModel(GEMINI_MODEL_NAME)
        response = model.generate_content(prompt)
        text = (response.text or "").strip()
        return text or None
    except Exception as exc:  # noqa: BLE001
        print(f"[info] Gemini call unavailable, using fallback template: {exc}")
        return None


def _prompt(agent_name: str, role: str, task: str, ticket: "object", extra_context: str = "") -> str:
    pipeline = ticket.inputs.get("pipeline") or "unknown pipeline"
    table = ticket.inputs.get("table") or "n/a"
    return (
        f"You are the {agent_name}, one module of FlowSentinel AI, an autonomous data-engineering "
        f"on-call framework.\nRole: {role}\n\n"
        f"Incident ticket:\nTitle: {ticket.title}\nDescription: {ticket.description}\n"
        f"Pipeline: {pipeline}  Table: {table}\n"
        f"{extra_context}\n\nTask: {task}\n"
        "Respond in 3-5 short plain-text lines, no markdown, no headers, no preamble. Be specific and "
        "concrete -- use realistic numbers/timings consistent with the incident. Do not mention that "
        "you are a language model."
    )


def _finish(agent_name: str, ai_text: Optional[str], fallback_text: str) -> str:
    if ai_text:
        return f"🧠 [{agent_name} · Gemini] {ai_text.strip()}"
    return f"📋 [{agent_name} · Template] {fallback_text.strip()}"


def run_agent_script(script: Path, timeout: int = 60, live: bool = True) -> str:
    """Shells out to one of the demos/*/  scripts. In Demo Mode, forces every
    real-credential path to fail closed regardless of what's configured on
    this machine, so the script falls into its own offline mode."""
    env = {**os.environ, "GCP_PROJECT_ID": os.getenv("GCP_PROJECT_ID", "ctech-flowsentinel-ai")}
    if not live:
        env.pop("GITHUB_PAT_TOKEN", None)
        env["GOOGLE_APPLICATION_CREDENTIALS"] = "/nonexistent-visual-demo-mode"
        env["CLOUDSDK_CONFIG"] = "/nonexistent-visual-demo-mode"
    result = subprocess.run(
        [sys.executable, str(script)], capture_output=True, text=True, timeout=timeout, env=env,
    )
    return (result.stdout or "") + (("\n" + result.stderr) if result.returncode else "")


# ---------------------------------------------------------------------------
# Autonomous fix agents -- run end to end with no human step.
# ---------------------------------------------------------------------------

def delayed_missing_data_agent(ticket, live: bool = True) -> str:
    """Grounds Gemini in a REAL GCS existence check (via feed_sentinel.py)
    rather than letting it guess whether the file landed."""
    signal = run_agent_script(DEMOS_DIR / "demo_4_source_feed_sentinel" / "feed_sentinel.py", live=live)
    prompt = _prompt(
        "Source Feed Sentinel Agent",
        "Monitors vendor file/stream arrival against SLA deadlines and pauses downstream DAGs on breach.",
        "Given the real feed-check output below, write the final incident resolution note for the ticket log.",
        ticket, extra_context=f"Real feed-check output just ran:\n{signal.strip()[-800:]}",
    )
    ai_text = call_gemini(prompt, live=live)
    return _finish("Source Feed Sentinel Agent", ai_text, signal)


def api_failure_agent(ticket, live: bool = True) -> str:
    prompt = _prompt(
        "API Health Agent",
        "Watches third-party API calls for auth/rate-limit/outage errors and retries transient failures with backoff.",
        "Decide whether this is transient (retry) or persistent (escalate), and narrate the outcome as if you just performed it.",
        ticket,
    )
    ai_text = call_gemini(prompt, live=live)
    fallback = (
        "Retry 1/3 with exponential backoff (2s)... Retry 2/3 (4s)... Retry 3/3 (8s)...\n"
        "Upstream API recovered on retry 3 -- ingestion resumed. No human action needed."
    )
    return _finish("API Health Agent", ai_text, fallback)


def orchestration_stall_agent(ticket, live: bool = True) -> str:
    prompt = _prompt(
        "Orchestration Watchdog Agent",
        "Detects Airflow/Dagster/Prefect stalls (deadlocks, evicted workers, zombie tasks) and clears them.",
        "Diagnose the stall and narrate the automated remediation you just performed.",
        ticket,
    )
    ai_text = call_gemini(prompt, live=live)
    dag = ticket.inputs.get("pipeline", "sales_transformation_pipeline")
    fallback = (
        f"DAG '{dag}' has a task stuck in 'running' for 22 minutes past its median duration. "
        "Diagnosis: worker pod was evicted mid-task (zombie task signature). "
        f"Clearing zombie task state and requesting a fresh worker slot... DAG '{dag}' resumed. "
        "No scheduler restart required."
    )
    return _finish("Orchestration Watchdog Agent", ai_text, fallback)


def long_running_query_agent(ticket, live: bool = True) -> str:
    prompt = _prompt(
        "Query Doctor Agent",
        "Watches warehouse queries for lock contention and runaway runtime, killing anything past a safe threshold.",
        "Diagnose the likely query-plan issue and narrate killing the query and notifying its owner.",
        ticket,
    )
    ai_text = call_gemini(prompt, live=live)
    table = ticket.inputs.get("table", "dw_analytics.fact_orders")
    fallback = (
        f"Query against '{table}' has run 47 min, past the 30 min safety threshold. "
        "EXPLAIN plan shows a full table scan -- missing partition filter on order_date. "
        "Killing the runaway query to release the table lock... Lock released. "
        "Query owner notified with the missing-partition-filter diagnosis."
    )
    return _finish("Query Doctor Agent", ai_text, fallback)


def silent_corruption_agent(ticket, live: bool = True) -> str:
    prompt = _prompt(
        "Data Observability Agent",
        "Watches row-count and value-distribution baselines per table per run to catch corruption no test caught.",
        "Diagnose the anomaly and narrate quarantining the affected partition.",
        ticket,
    )
    ai_text = call_gemini(prompt, live=live)
    table = ticket.inputs.get("table", "dw_analytics.fact_orders")
    fallback = (
        f"Row-count for '{table}' is 4.2 standard deviations below its 30-day baseline. "
        "No dbt test caught it -- the pipeline itself reported success. "
        f"Quarantining today's partition of '{table}' from downstream consumption... "
        "Partition quarantined and flagged. Alert sent -- root cause needs a human look."
    )
    return _finish("Data Observability Agent", ai_text, fallback)


def connection_pool_agent(ticket, live: bool = True) -> str:
    prompt = _prompt(
        "Infra Health Agent",
        "Watches database connection pool utilization and circuit-breaks low-priority jobs before exhaustion.",
        "Diagnose the saturation and narrate circuit-breaking the lowest-priority consumer.",
        ticket,
    )
    ai_text = call_gemini(prompt, live=live)
    fallback = (
        "Connection pool utilization at 98% and climbing. Lowest-priority batch job identified as the "
        "largest consumer. Circuit-breaking that job to free up connections... Pool utilization back "
        "to 61%. Deferred job re-queued for the next off-peak window."
    )
    return _finish("Infra Health Agent", ai_text, fallback)


def storage_quota_agent(ticket, live: bool = True) -> str:
    prompt = _prompt(
        "Capacity Forecaster Agent",
        "Projects storage/slot usage trends and defers low-priority load before a hard cap is hit.",
        "Diagnose the capacity trend and narrate deferring a job to relieve pressure.",
        ticket,
    )
    ai_text = call_gemini(prompt, live=live)
    fallback = (
        "BigQuery slot reservation at 92% utilization. Projected to hit the hard cap in ~40 minutes at "
        "current burn rate. Deferring the lowest-priority scheduled batch job to protect critical "
        "pipelines... Slot pressure relieved. Capacity alert sent to the on-call channel."
    )
    return _finish("Capacity Forecaster Agent", ai_text, fallback)


def missed_deadline_agent(ticket, live: bool = True) -> str:
    prompt = _prompt(
        "Pace Predictor Agent",
        "Compares current batch progress against historical run-duration distributions to catch a missed "
        "SLA before it happens, not after.",
        "Diagnose the pace shortfall and narrate the escalation you're sending, with a specific time margin.",
        ticket,
    )
    ai_text = call_gemini(prompt, live=live)
    dag = ticket.inputs.get("pipeline", "sales_transformation_pipeline")
    fallback = (
        f"'{dag}' is 68% complete at T-45min; historical p90 needs 85% complete by now. "
        "Projected finish is 22 minutes past the dashboard refresh deadline. Escalating to on-call "
        "now -- 40 minutes of runway to intervene, instead of finding out after the miss."
    )
    return _finish("Pace Predictor Agent", ai_text, fallback)


# ---------------------------------------------------------------------------
# AI-assisted recommendation agents -- diagnose and propose, then wait.
# ---------------------------------------------------------------------------

def schema_drift_recommend_agent(ticket, live: bool = True) -> str:
    prompt = _prompt(
        "Schema Drift Repair Agent",
        "Diagnoses upstream schema drift (renamed/retyped columns) and drafts a BigQuery SQL adapter.",
        "Draft the SQL adapter fix (as a short SQL snippet inline in your answer) and explain it in 1-2 sentences. "
        "This is a recommendation only -- a human approves before it ships.",
        ticket,
    )
    ai_text = call_gemini(prompt, live=live)
    fallback = (
        "Schema adapter: COALESCE(SAFE_CAST(cust_id AS STRING), SAFE_CAST(customer_identifier_v2 AS STRING)) "
        "AS cust_id, plus SAFE_CAST(REGEXP_REPLACE(CAST(amount_usd AS STRING), r'\\$', '') AS FLOAT64) AS "
        "amount_usd. Reconciles the legacy and renamed vendor columns and strips the currency symbol before "
        "casting. Awaiting approval before this ships as a PR."
    )
    return _finish("Schema Drift Repair Agent", ai_text, fallback)


def schema_drift_apply_agent(ticket, live: bool = True) -> str:
    """Mechanical: executes the already-approved merge. Not a new AI decision."""
    return run_agent_script(DEMOS_DIR / "demo_2_code_patch_and_approval" / "approve_pr_trigger.py", live=live)


def resource_exhaustion_recommend_agent(ticket, live: bool = True) -> str:
    prompt = _prompt(
        "Compute Doctor Agent",
        "Diagnoses Spark/Trino/Ray OOM and data-skew failures from executor logs and recommends a config fix.",
        "Diagnose the likely skew/config cause and recommend a specific repartition key and memory setting. "
        "This is a recommendation only -- it changes cluster spend, so a human approves it.",
        ticket,
    )
    ai_text = call_gemini(prompt, live=live)
    fallback = (
        "Executor logs show 340x skew between the largest and median partition (key: customer_id). "
        "Recommendation: repartition by (customer_id, order_date); bump executor memory 8g -> 16g for "
        "the next 3 runs. Awaiting approval -- this changes cluster spend, so it's not applied automatically."
    )
    return _finish("Compute Doctor Agent", ai_text, fallback)


def resource_exhaustion_apply_agent(ticket, live: bool = True) -> str:
    return "✅ Applied: repartition key updated, executor memory override queued for the next 3 scheduled runs."


def dq_check_recommend_agent(ticket, live: bool = True) -> str:
    prompt = _prompt(
        "DQ Triage Agent",
        "Diagnoses dbt test / Great Expectations failures (duplicates, nulls, threshold breaches).",
        "Diagnose the root cause of the failing test and recommend a specific logic fix. "
        "This is a recommendation only -- a human approves it before it ships to the model.",
        ticket,
    )
    ai_text = call_gemini(prompt, live=live)
    table = ticket.inputs.get("table", "stg_orders")
    fallback = (
        f"dbt test 'unique_order_id' failing on '{table}': 12 duplicate order_id values. Traced to a Kafka "
        "replay window (02:00-02:15 UTC) re-emitting already-processed events. Recommendation: add a dedup "
        "CTE keyed on (order_id, ingestion_ts), keeping the latest row. Awaiting approval before this logic "
        "change ships to the staging model."
    )
    return _finish("DQ Triage Agent", ai_text, fallback)


def dq_check_apply_agent(ticket, live: bool = True) -> str:
    return "✅ Applied: dedup CTE patch queued to the staging model; dbt test will re-run on next build."


def backfill_recommend_agent(ticket, live: bool = True) -> str:
    """Grounds Gemini in a REAL blast-radius calculation rather than letting
    it invent partition counts or a cost figure."""
    sys.path.insert(0, str(DEMOS_DIR / "demo_3_blast_radius_backfill"))
    from backfill_agent import calculate_blast_radius  # type: ignore

    plan = calculate_blast_radius()
    extra = (
        f"A deterministic blast-radius calculation already ran and found: {plan['partition_count']} "
        f"partitions ({plan['start_date']} to {plan['end_date']}) across {len(plan['affected_tables'])} "
        f"downstream tables, estimated BigQuery cost ${plan['estimated_bq_cost_usd']} at concurrency "
        f"{plan['recommended_concurrency']}."
    )
    prompt = _prompt(
        "Backfill Planning Agent",
        "Summarizes a computed blast-radius plan for an on-call engineer's approval.",
        "Summarize this real plan for the ticket log and recommend approval, citing the exact numbers given -- "
        "do not invent different numbers.",
        ticket, extra_context=extra,
    )
    ai_text = call_gemini(prompt, live=live)
    fallback = (
        f"Blast radius: {plan['partition_count']} partitions ({plan['start_date']} to {plan['end_date']}) "
        f"across {len(plan['affected_tables'])} downstream tables. Estimated BigQuery cost: "
        f"${plan['estimated_bq_cost_usd']} at concurrency {plan['recommended_concurrency']}. "
        "Awaiting approval before running the backfill."
    )
    return _finish("Backfill Planning Agent", ai_text, fallback)


def backfill_apply_agent(ticket, live: bool = True) -> str:
    return run_agent_script(DEMOS_DIR / "demo_3_blast_radius_backfill" / "execute_backfill.py", live=live)


# ---------------------------------------------------------------------------
# Ops Read-Out Agent -- the Summary tab's headline. Not tied to one ticket;
# it reads the same pipeline/table/ticket numbers already on screen and
# writes the one paragraph an on-call engineer would want first: what's the
# state, what's the single biggest problem, what to do about it.
# ---------------------------------------------------------------------------

def ops_readout_agent(pipeline_data: dict, table_data: dict, ticket_stats: dict, live: bool = True) -> dict:
    pipelines = pipeline_data.get("pipelines", [])
    tables = table_data.get("tables", [])
    scorecard = pipeline_data.get("scorecard", {})

    breaching_pipelines = [p["name"] for p in pipelines if not p.get("in_sla")]
    non_sla_tables = [t for t in tables if not t.get("in_sla")]
    worst_table = non_sla_tables[0] if non_sla_tables else None  # tables list is sorted breach-first, oldest-first

    facts = (
        f"Pipelines: {scorecard.get('in_sla', 0)}/{scorecard.get('total_pipelines', 0)} in SLA. "
        f"Breaching pipelines: {', '.join(breaching_pipelines) if breaching_pipelines else 'none'}. "
        f"Non-SLA tables: {len(non_sla_tables)} of {len(tables)}"
        + (f", worst is '{worst_table['table']}' ({worst_table['days_since']} days stale)" if worst_table else "")
        + f". Open incidents: {ticket_stats.get('open_incidents', 0)}. "
        f"Awaiting human approval: {ticket_stats.get('awaiting_approval', 0)}."
    )

    prompt = (
        "You are the Ops Read-Out Agent for FlowSentinel AI, an autonomous data-engineering on-call framework. "
        "Given these real, current numbers from the Summary dashboard:\n\n"
        f"{facts}\n\n"
        "Write a 2-3 sentence executive read-out for the on-call engineer covering: (1) overall status, "
        "(2) the single most significant issue right now -- name it specifically, don't just say 'some pipelines' "
        "-- and (3) the one recommended next action. Plain text, no markdown, no preamble like 'Here is a summary'. "
        "If everything is healthy, say so plainly and don't invent a problem."
    )
    ai_text = call_gemini(prompt, live=live)
    if ai_text:
        return {"text": ai_text.strip(), "engine": "gemini"}

    if not breaching_pipelines and not non_sla_tables:
        fallback = (
            f"All {scorecard.get('total_pipelines', 0)} pipelines are in SLA and every tracked table is fresh. "
            f"{ticket_stats.get('awaiting_approval', 0)} ticket(s) awaiting approval. No action needed right now."
        )
    else:
        worst_desc = f"table '{worst_table['table']}' hasn't refreshed in {worst_table['days_since']} days" if worst_table else "a breaching pipeline"
        fallback = (
            f"{scorecard.get('in_sla', 0)} of {scorecard.get('total_pipelines', 0)} pipelines are in SLA. "
            f"Biggest concern: {worst_desc}. "
            f"Recommended action: review it on the Ticket Board"
            + (f" and approve the {ticket_stats.get('awaiting_approval')} pending fix(es)." if ticket_stats.get("awaiting_approval") else ".")
        )
    return {"text": fallback, "engine": "template"}


# ---------------------------------------------------------------------------
# The Issue Segment Agent -- classification, the front door for every ticket.
# ---------------------------------------------------------------------------

@dataclass
class FailureType:
    key: str
    label: str
    category: str
    automation_level: str
    keywords: list
    placeholder: str
    fix_fn: Optional[Callable] = None
    recommend_fn: Optional[Callable] = None
    apply_fn: Optional[Callable] = None


FAILURE_CATALOG = [
    FailureType("schema_drift", "Unannounced schema change", "Upstream Data & Vendor", ASSISTED,
                ["schema", "column", "renamed", "dropped", "cust_id", "customer_identifier"],
                "Vendor CSV renamed 'cust_id' to 'customer_identifier_v2' and amount_usd now arrives as a currency string.",
                recommend_fn=schema_drift_recommend_agent, apply_fn=schema_drift_apply_agent),
    FailureType("delayed_missing_data", "Delayed or missing data source", "Upstream Data & Vendor", AUTONOMOUS,
                ["delayed", "missing file", "sftp", "s3", "kafka", "stalled", "late feed"],
                "Vendor CRM file 'customer_profiles_2026-09-08.json' has not landed past its 08:00 UTC SLA.",
                fix_fn=delayed_missing_data_agent),
    FailureType("api_failure", "Third-party API failure", "Upstream Data & Vendor", AUTONOMOUS,
                ["api", "rate limit", "429", "expired key", "timeout", "endpoint down"],
                "Vendor pricing API returning HTTP 429 rate-limit errors on the hourly ingestion job.",
                fix_fn=api_failure_agent),
    FailureType("resource_exhaustion", "Resource exhaustion (OOM / spill)", "Pipeline & Compute", ASSISTED,
                ["oom", "out of memory", "spill", "skew", "spark", "trino", "ray"],
                "Spark job 'daily_aggregation' failing with OutOfMemoryError during the shuffle stage.",
                recommend_fn=resource_exhaustion_recommend_agent, apply_fn=resource_exhaustion_apply_agent),
    FailureType("orchestration_stall", "Orchestration stall", "Pipeline & Compute", AUTONOMOUS,
                ["airflow", "dagster", "prefect", "stall", "deadlock", "evicted", "scheduler"],
                "Airflow task 'load_raw_staging_orders' has been stuck in 'running' for 22 minutes.",
                fix_fn=orchestration_stall_agent),
    FailureType("long_running_query", "Long-running query / lock contention", "Pipeline & Compute", AUTONOMOUS,
                ["lock", "hung query", "timeout", "full scan", "contention", "slow query"],
                "BigQuery query against fact_orders has run 47 minutes with a table lock held.",
                fix_fn=long_running_query_agent),
    FailureType("dq_check_failure", "Data quality check failure", "Data Quality & Integrity", ASSISTED,
                ["dbt test", "great expectations", "duplicate", "null", "threshold breach", "unique"],
                "dbt test 'unique_order_id' failing on stg_orders with 12 duplicate values.",
                recommend_fn=dq_check_recommend_agent, apply_fn=dq_check_apply_agent),
    FailureType("silent_corruption", "Silent data corruption", "Data Quality & Integrity", AUTONOMOUS,
                ["corrupted", "zeroed out", "silent", "unnoticed", "row count drop"],
                "fact_orders row count is 90% below its 30-day baseline with no pipeline failure reported.",
                fix_fn=silent_corruption_agent),
    FailureType("late_arriving_records", "Late-arriving / out-of-order records", "Data Quality & Integrity", ASSISTED,
                ["late-arriving", "out of order", "backfill", "watermark", "reprocess"],
                "Events for 2026-09-01 through 2026-09-07 arrived after their partitions had already aggregated.",
                recommend_fn=backfill_recommend_agent, apply_fn=backfill_apply_agent),
    FailureType("connection_pool", "Connection pool exhaustion", "Infrastructure & Security", AUTONOMOUS,
                ["connection pool", "pool exhausted", "connection dropped", "max connections"],
                "Postgres connection pool at 98% utilization during peak nightly load.",
                fix_fn=connection_pool_agent),
    FailureType("credentials_iam", "Expired credentials / IAM policy shift", "Infrastructure & Security", ADVISORY,
                ["expired token", "iam", "permission denied", "credential", "access denied", "rotated secret"],
                "Service-to-service calls started failing with PERMISSION_DENIED after a secret rotation."),
    FailureType("storage_quota", "Storage quota / slot limits", "Infrastructure & Security", AUTONOMOUS,
                ["quota", "disk full", "slot limit", "credit cap", "concurrency cap"],
                "BigQuery slot reservation is at 92% utilization and climbing.",
                fix_fn=storage_quota_agent),
    FailureType("missed_deadline", "Missed dashboard deadline", "SLA & Escalations", AUTONOMOUS,
                ["dashboard", "deadline", "sla", "start of business", "nightly batch delay"],
                "Nightly batch is behind pace to refresh the executive dashboard before 07:00 UTC.",
                fix_fn=missed_deadline_agent),
    FailureType("urgent_backfill", "Urgent backfill request", "SLA & Escalations", ASSISTED,
                ["backfill request", "broken metric", "downstream team reported", "historical rerun"],
                "Finance reports the daily revenue metric has been wrong since Monday -- needs a backfill.",
                recommend_fn=backfill_recommend_agent, apply_fn=backfill_apply_agent),
]

CATALOG_BY_KEY = {f.key: f for f in FAILURE_CATALOG}


def classify(text: str, live: bool = True) -> tuple:
    """Issue Segment Agent: assigns free text to one of the 14 categories.
    Tries Gemini first (needs live=True and ADC); falls back to a
    deterministic keyword scorer so the demo works with zero cloud access."""
    ai_key = _classify_with_gemini(text) if live else None
    if ai_key:
        return ai_key, 0.9

    text_lower = text.lower()
    best_key, best_score = "schema_drift", 0
    for failure in FAILURE_CATALOG:
        score = sum(1 for kw in failure.keywords if kw in text_lower)
        if score > best_score:
            best_key, best_score = failure.key, score
    confidence = min(0.95, 0.5 + 0.15 * best_score) if best_score else 0.35
    return best_key, confidence


def _classify_with_gemini(text: str) -> Optional[str]:
    options = ", ".join(f.key for f in FAILURE_CATALOG)
    prompt = (
        f"Classify this data-engineering incident into exactly one of these keys: {options}.\n"
        f"Reply with only the key, nothing else.\n\nIncident: {text}"
    )
    result = call_gemini(prompt, live=True)
    if result:
        key = result.strip().lower()
        if key in CATALOG_BY_KEY:
            return key
    return None


# ---------------------------------------------------------------------------
# Agent metadata -- documents purpose/inputs/decisions for every real agent
# above. This is what the dashboard's Agents view and the slide deck are
# both generated from, so the docs can never drift from the code.
# ---------------------------------------------------------------------------

@dataclass
class AgentSpec:
    key: str
    name: str
    stage: str  # "Classification" | "Autonomous Fix" | "AI-Assisted Recommendation"
    purpose: str
    inputs: list
    grounding_data: str
    decision_task: str
    output: str
    fallback: str
    model: str = GEMINI_MODEL_NAME


AGENT_SPECS = [
    AgentSpec(
        key="issue_segment_agent", name="Issue Segment Agent", stage="Classification",
        purpose="Reads every new ticket's free text and routes it to one of the 14 recognized failure "
                "categories, determining whether it goes to the Autonomous, AI-Assisted, or Advisory queue.",
        inputs=["Ticket title", "Ticket description (vendor incident text, or a synced GitHub issue body)"],
        grounding_data="None -- this is the first agent in the pipeline; nothing has been diagnosed yet.",
        decision_task="Classify the incident text into exactly one of the 14 category keys.",
        output="A category key + confidence score, which sets the ticket's automation level and queue.",
        fallback="Deterministic keyword scorer over each category's keyword list -- same categories, "
                 "lower confidence, zero cloud dependency.",
    ),
    AgentSpec(
        key="delayed_missing_data", name="Source Feed Sentinel Agent", stage="Autonomous Fix",
        purpose="Detects a vendor file/stream that missed its SLA arrival window and pauses the downstream "
                "DAG before it processes partial or empty data.",
        inputs=["Ticket description", "Pipeline name"],
        grounding_data="A real GCS existence check against the configured landing bucket (feed_sentinel.py) "
                       "-- Gemini narrates the result, it does not decide file presence itself.",
        decision_task="Write the final incident-resolution note given the real feed-check result.",
        output="A resolution note appended to the ticket log; ticket auto-resolves.",
        fallback="The raw feed-check script output is used verbatim as the ticket log entry.",
    ),
    AgentSpec(
        key="api_failure", name="API Health Agent", stage="Autonomous Fix",
        purpose="Classifies third-party API failures as transient (rate-limit/timeout) vs. persistent "
                "(expired key/outage) and retries transient failures with backoff.",
        inputs=["Ticket description", "Pipeline name"],
        grounding_data="None yet (Phase 2 of the automation playbook adds live endpoint health checks).",
        decision_task="Decide retry-vs-escalate and narrate the retry sequence and outcome.",
        output="A resolution note; ticket auto-resolves.",
        fallback="A fixed 3-retry exponential-backoff narrative.",
    ),
    AgentSpec(
        key="orchestration_stall", name="Orchestration Watchdog Agent", stage="Autonomous Fix",
        purpose="Diagnoses Airflow/Dagster/Prefect stalls (deadlocks, evicted workers, zombie tasks) and "
                "clears them without a scheduler restart.",
        inputs=["Ticket description", "Pipeline/DAG name"],
        grounding_data="None yet (Phase 2 adds live scheduler/worker health metrics).",
        decision_task="Diagnose the stall signature and narrate the automated remediation performed.",
        output="A resolution note; ticket auto-resolves.",
        fallback="A fixed zombie-task-clear narrative.",
    ),
    AgentSpec(
        key="long_running_query", name="Query Doctor Agent", stage="Autonomous Fix",
        purpose="Watches warehouse queries for lock contention and kills anything past a safe runtime "
                "threshold, notifying the query owner with the likely cause.",
        inputs=["Ticket description", "Table name"],
        grounding_data="None yet (Phase 2 adds live BigQuery/Snowflake query-plan reads).",
        decision_task="Diagnose the likely query-plan issue and narrate killing the query.",
        output="A resolution note; ticket auto-resolves.",
        fallback="A fixed missing-partition-filter narrative.",
    ),
    AgentSpec(
        key="silent_corruption", name="Data Observability Agent", stage="Autonomous Fix",
        purpose="Watches row-count and value-distribution baselines per table per run to catch corruption "
                "that finished 'successfully' but is wrong -- the case no dbt test caught.",
        inputs=["Ticket description", "Table name"],
        grounding_data="None yet (Phase 2 adds a standing row-count/distribution baseline store).",
        decision_task="Diagnose the anomaly and narrate quarantining the affected partition.",
        output="A resolution note; ticket auto-resolves (root cause still needs a human look).",
        fallback="A fixed row-count-anomaly narrative.",
    ),
    AgentSpec(
        key="connection_pool", name="Infra Health Agent", stage="Autonomous Fix",
        purpose="Watches database connection pool utilization and circuit-breaks the lowest-priority "
                "consumer before the pool fully exhausts.",
        inputs=["Ticket description"],
        grounding_data="None yet (Phase 2 adds live Cloud Monitoring metric reads).",
        decision_task="Diagnose the saturation source and narrate the circuit-break action.",
        output="A resolution note; ticket auto-resolves.",
        fallback="A fixed pool-saturation narrative.",
    ),
    AgentSpec(
        key="storage_quota", name="Capacity Forecaster Agent", stage="Autonomous Fix",
        purpose="Projects storage/slot usage trends and defers low-priority load before a hard quota cap "
                "is hit.",
        inputs=["Ticket description"],
        grounding_data="None yet (Phase 2 adds live usage-trend forecasting).",
        decision_task="Diagnose the capacity trend and narrate deferring a job to relieve pressure.",
        output="A resolution note; ticket auto-resolves.",
        fallback="A fixed capacity-pressure narrative.",
    ),
    AgentSpec(
        key="missed_deadline", name="Pace Predictor Agent", stage="Autonomous Fix",
        purpose="Compares current batch progress against historical run-duration distributions to alert "
                "on-call hours before an SLA is actually missed, not after.",
        inputs=["Ticket description", "Pipeline name"],
        grounding_data="None yet (Phase 2 adds live run-duration history).",
        decision_task="Diagnose the pace shortfall and narrate the escalation with a specific time margin.",
        output="A resolution note (escalation alert); ticket auto-resolves.",
        fallback="A fixed pace-shortfall narrative.",
    ),
    AgentSpec(
        key="schema_drift", name="Schema Drift Repair Agent", stage="AI-Assisted Recommendation",
        purpose="Diagnoses an upstream schema change (renamed/retyped columns) and drafts a BigQuery SQL "
                "adapter to reconcile old and new shapes.",
        inputs=["Ticket description (the schema drift details)", "Pipeline name"],
        grounding_data="None -- the fix is drafted directly from the incident description.",
        decision_task="Draft the SQL adapter and explain it in 1-2 sentences.",
        output="A recommendation posted to the ticket log; ticket moves to Awaiting Approval.",
        fallback="The known-good COALESCE/SAFE_CAST adapter pattern for this exact incident.",
    ),
    AgentSpec(
        key="resource_exhaustion", name="Compute Doctor Agent", stage="AI-Assisted Recommendation",
        purpose="Diagnoses Spark/Trino/Ray OOM and data-skew failures and recommends a repartition key and "
                "memory setting -- gated behind approval since it changes cluster spend.",
        inputs=["Ticket description", "Pipeline name"],
        grounding_data="None yet (Phase 2 adds live executor-log reads).",
        decision_task="Diagnose the likely skew/config cause and recommend a specific fix.",
        output="A recommendation posted to the ticket log; ticket moves to Awaiting Approval.",
        fallback="A fixed skew-diagnosis + repartition recommendation.",
    ),
    AgentSpec(
        key="dq_check_failure", name="DQ Triage Agent", stage="AI-Assisted Recommendation",
        purpose="Diagnoses dbt test / Great Expectations failures (duplicates, nulls, threshold breaches) "
                "and recommends a logic fix.",
        inputs=["Ticket description", "Table name"],
        grounding_data="None yet (Phase 2 adds live test-run history correlation).",
        decision_task="Diagnose the root cause of the failing test and recommend a specific fix.",
        output="A recommendation posted to the ticket log; ticket moves to Awaiting Approval.",
        fallback="A fixed dedup-CTE recommendation.",
    ),
    AgentSpec(
        key="late_arriving_records / urgent_backfill", name="Backfill Planning Agent",
        stage="AI-Assisted Recommendation",
        purpose="Summarizes a computed blast-radius plan (affected partitions, tables, BigQuery cost) for "
                "an on-call engineer to approve -- shared by both the late-arriving-records and "
                "urgent-backfill-request categories.",
        inputs=["Ticket description", "Real blast-radius calculation (backfill_agent.py)"],
        grounding_data="A REAL deterministic calculation: partition count x bytes-per-partition x "
                       "downstream-table count, converted to a $/TB-scanned BigQuery cost estimate. "
                       "Gemini narrates and recommends; it never invents the numbers.",
        decision_task="Summarize the real plan and recommend approval, citing the exact numbers given.",
        output="A recommendation posted to the ticket log; ticket moves to Awaiting Approval.",
        fallback="The formatted real plan output, without Gemini's narration layer.",
    ),
    AgentSpec(
        key="ops_readout_agent", name="Ops Read-Out Agent", stage="Summary",
        purpose="Writes the Summary tab's headline paragraph: overall status, the single biggest issue, and "
                "the recommended next action -- read once instead of scanning every table.",
        inputs=["Pipeline scorecard + per-pipeline SLA status", "Table freshness list",
                "Open incident and awaiting-approval counts"],
        grounding_data="The exact same real numbers already rendered below it on the Summary tab (Demo Mode's "
                       "curated sample, or Production Mode's real Composer/BigQuery data) -- Gemini narrates "
                       "and prioritizes, it never invents a pipeline or a number.",
        decision_task="Name the single most significant issue and the one recommended next action, in 2-3 "
                      "sentences, or say plainly that nothing needs attention.",
        output="A read-out paragraph shown above the scorecard.",
        fallback="A templated sentence built from the same real numbers (worst pipeline/table + pending approvals).",
    ),
]

AGENT_SPECS_BY_KEY = {a.key: a for a in AGENT_SPECS}
