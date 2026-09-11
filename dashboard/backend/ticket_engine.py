#!/usr/bin/env python3
r"""
FlowSentinel AI - Ticket Engine
================================
The ticket board's state machine, built on top of the AI agents defined in
ai_agents.py. This file knows nothing about Gemini or any specific failure
category's remediation logic -- it only knows the lifecycle every ticket
moves through and which agent hook to call at each stage:

  new -----classify----> queued_autonomous ----start_fix----> resolved
                      \-> queued_assisted  ----start_analysis-> awaiting_approval --approve--> resolved
                      \-> advisory (terminal, no action -- by design)

TicketStore is in-memory for the demo. Each ticket also gets best-effort
mirrored to a real GitHub Issue when a token is available (main.py's job),
so "ticket in a ticketing tool" is literally true whenever access allows it.

Advisory-only tickets (credential/IAM changes) are classified but never
acted on automatically -- that boundary is enforced here, not left to the
caller to remember.
"""
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

from ai_agents import (  # noqa: F401 -- re-exported for callers that import them from here
    ADVISORY,
    ASSISTED,
    AUTONOMOUS,
    AGENT_SPECS,
    AGENT_SPECS_BY_KEY,
    CATALOG_BY_KEY,
    FAILURE_CATALOG,
    classify,
    run_agent_script,
)


@dataclass
class Ticket:
    id: str
    title: str
    description: str
    inputs: dict
    category: Optional[str] = None
    automation_level: Optional[str] = None
    confidence: Optional[float] = None
    status: str = "new"
    log: list = field(default_factory=list)
    github_issue_url: Optional[str] = None
    github_issue_number: Optional[int] = None
    source: str = "manual"  # "manual" (Demo Mode injection) or "github" (Production Mode sync)
    created_at: float = field(default_factory=time.time)

    def to_dict(self):
        failure = CATALOG_BY_KEY.get(self.category)
        return {
            "id": self.id, "title": self.title, "description": self.description,
            "inputs": self.inputs, "category": self.category,
            "category_label": failure.label if failure else None,
            "category_group": failure.category if failure else None,
            "automation_level": self.automation_level, "confidence": self.confidence,
            "status": self.status, "log": self.log, "source": self.source,
            "github_issue_url": self.github_issue_url, "created_at": self.created_at,
        }


class TicketStore:
    def __init__(self):
        self._tickets: dict[str, Ticket] = {}

    def create(self, title: str, description: str, inputs: dict, source: str = "manual",
               github_issue_url: Optional[str] = None, github_issue_number: Optional[int] = None) -> Ticket:
        ticket = Ticket(
            id=uuid.uuid4().hex[:8], title=title, description=description, inputs=inputs,
            source=source, github_issue_url=github_issue_url, github_issue_number=github_issue_number,
        )
        self._tickets[ticket.id] = ticket
        return ticket

    def get(self, ticket_id: str) -> Optional[Ticket]:
        return self._tickets.get(ticket_id)

    def known_github_issue_numbers(self) -> set:
        return {t.github_issue_number for t in self._tickets.values() if t.github_issue_number is not None}

    def all(self) -> list:
        return sorted(self._tickets.values(), key=lambda t: t.created_at, reverse=True)

    def reset(self):
        self._tickets.clear()


STORE = TicketStore()

NEW = "new"
QUEUED_AUTONOMOUS = "queued_autonomous"
QUEUED_ASSISTED = "queued_assisted"
AWAITING_APPROVAL = "awaiting_approval"
RESOLVED = "resolved"
ADVISORY_STATUS = "advisory"
NEEDS_ATTENTION = "needs_attention"


def classify_ticket(ticket: Ticket, live: bool = True) -> Ticket:
    """The Issue Segment Agent: reads one ticket and sorts it into its queue.
    Does not do any remediation work itself -- that's a separate click."""
    if ticket.status != NEW:
        return ticket

    key, confidence = classify(f"{ticket.title}. {ticket.description}", live=live)
    failure = CATALOG_BY_KEY[key]
    ticket.category = key
    ticket.automation_level = failure.automation_level
    ticket.confidence = round(confidence, 2)
    ticket.log.append(f"🧭 [Issue Segment Agent] Classified as '{failure.label}' ({failure.category}) "
                       f"-- confidence {ticket.confidence:.0%}. Routed to the "
                       f"{'Autonomous' if failure.automation_level == AUTONOMOUS else 'AI-Assisted' if failure.automation_level == ASSISTED else 'Advisory'} queue.")

    if failure.automation_level == AUTONOMOUS:
        ticket.status = QUEUED_AUTONOMOUS
    elif failure.automation_level == ASSISTED:
        ticket.status = QUEUED_ASSISTED
    else:
        ticket.status = ADVISORY_STATUS
        ticket.log.append("🔒 Advisory only -- this category is never auto-remediated. A human needs to investigate "
                           "and apply any credential/IAM change directly.")
    return ticket


def start_autonomous_fix(ticket: Ticket, live: bool = True) -> Ticket:
    """Click on an Autonomous-queue ticket: the fix agent reads it and works
    it end to end, no further human step."""
    failure = CATALOG_BY_KEY.get(ticket.category)
    if not failure or ticket.status != QUEUED_AUTONOMOUS:
        return ticket
    mode_tag = "" if live else " (Demo Mode -- no real GCP/GitHub/Gemini calls made)"
    try:
        output = failure.fix_fn(ticket, live) if failure.fix_fn else "✅ Resolved (no fix agent wired yet)."
        ticket.log.append(output + mode_tag)
        ticket.status = RESOLVED
    except Exception as exc:  # noqa: BLE001
        ticket.log.append(f"⚠️ Fix agent hit an error, needs a human look: {exc}")
        ticket.status = NEEDS_ATTENTION
    return ticket


def start_assisted_analysis(ticket: Ticket, live: bool = True) -> Ticket:
    """Click on an AI-Assisted-queue ticket: the AI drafts its recommendation
    and then waits -- it never applies the fix itself."""
    failure = CATALOG_BY_KEY.get(ticket.category)
    if not failure or ticket.status != QUEUED_ASSISTED:
        return ticket
    mode_tag = "" if live else " (Demo Mode -- no real GCP/GitHub/Gemini calls made)"
    try:
        output = failure.recommend_fn(ticket, live) if failure.recommend_fn else "No recommendation agent wired yet."
        ticket.log.append(output + mode_tag)
        ticket.status = AWAITING_APPROVAL
    except Exception as exc:  # noqa: BLE001
        ticket.log.append(f"⚠️ Recommendation agent hit an error: {exc}")
        ticket.status = NEEDS_ATTENTION
    return ticket


def approve_ticket(ticket: Ticket, live: bool = True) -> Ticket:
    """The one human click in the AI-Assisted flow: apply the recommendation."""
    failure = CATALOG_BY_KEY.get(ticket.category)
    if not failure or ticket.status != AWAITING_APPROVAL:
        return ticket
    mode_tag = "" if live else " (Demo Mode -- no real GCP/GitHub/Gemini calls made)"
    try:
        output = failure.apply_fn(ticket, live) if failure.apply_fn else "✅ Approved and applied."
        ticket.log.append("👤 [Human Approval] Approved by on-call engineer.")
        ticket.log.append(output + mode_tag)
        ticket.status = RESOLVED
    except Exception as exc:  # noqa: BLE001
        ticket.log.append(f"⚠️ Apply step hit an error: {exc}")
        ticket.status = NEEDS_ATTENTION
    return ticket
