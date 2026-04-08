"""Workflow-state helpers for correction review rows."""
from __future__ import annotations

from typing import Dict, Iterable, List, Mapping, MutableMapping

ISSUE_STATUS_PENDING_REVIEW = "pending_review"
ISSUE_STATUS_APPROVED = "approved"
ISSUE_STATUS_REJECTED = "rejected"
ISSUE_STATUS_APPLIED = "applied"
ISSUE_STATUS_REVALIDATED_RESOLVED = "revalidated_resolved"
ISSUE_STATUS_REVALIDATED_REMAINING = "revalidated_remaining"

ISSUE_STATUS_VALUES = {
    ISSUE_STATUS_PENDING_REVIEW,
    ISSUE_STATUS_APPROVED,
    ISSUE_STATUS_REJECTED,
    ISSUE_STATUS_APPLIED,
    ISSUE_STATUS_REVALIDATED_RESOLVED,
    ISSUE_STATUS_REVALIDATED_REMAINING,
}

FINAL_ISSUE_STATUSES = {
    ISSUE_STATUS_APPLIED,
    ISSUE_STATUS_REVALIDATED_RESOLVED,
    ISSUE_STATUS_REVALIDATED_REMAINING,
}


def normalize_issue_status(value: object) -> str:
    text = "" if value is None else str(value).strip().lower()
    if text in {"", "nan", "none", "null"}:
        return ISSUE_STATUS_PENDING_REVIEW
    aliases = {
        "pending": ISSUE_STATUS_PENDING_REVIEW,
        "pending_review": ISSUE_STATUS_PENDING_REVIEW,
        "approve": ISSUE_STATUS_APPROVED,
        "approved": ISSUE_STATUS_APPROVED,
        "reject": ISSUE_STATUS_REJECTED,
        "rejected": ISSUE_STATUS_REJECTED,
        "apply": ISSUE_STATUS_APPLIED,
        "applied": ISSUE_STATUS_APPLIED,
        "resolved": ISSUE_STATUS_REVALIDATED_RESOLVED,
        "revalidated_resolved": ISSUE_STATUS_REVALIDATED_RESOLVED,
        "remaining": ISSUE_STATUS_REVALIDATED_REMAINING,
        "revalidated_remaining": ISSUE_STATUS_REVALIDATED_REMAINING,
    }
    return aliases.get(text, text)


def normalize_user_action(value: object) -> str:
    text = "" if value is None else str(value).strip().lower()
    aliases = {
        "approved": "approve",
        "apply": "approve",
        "yes": "approve",
        "y": "approve",
        "rejected": "reject",
        "skip": "reject",
        "no": "reject",
        "n": "reject",
    }
    return aliases.get(text, text)


def derive_issue_status(stored_status: object, user_action: object) -> str:
    normalized_status = normalize_issue_status(stored_status)
    normalized_action = normalize_user_action(user_action)
    if normalized_status in FINAL_ISSUE_STATUSES:
        return normalized_status
    if normalized_action == "approve":
        return ISSUE_STATUS_APPROVED
    if normalized_action == "reject":
        return ISSUE_STATUS_REJECTED
    if normalized_status in ISSUE_STATUS_VALUES:
        return normalized_status
    return ISSUE_STATUS_PENDING_REVIEW


def is_valid_issue_status(value: object) -> bool:
    return normalize_issue_status(value) in ISSUE_STATUS_VALUES


def apply_status_updates(rows: Iterable[MutableMapping[str, object]], status_by_issue_id: Mapping[str, str]) -> List[MutableMapping[str, object]]:
    updated_rows: List[MutableMapping[str, object]] = []
    for row in rows:
        issue_id = str(row.get("issue_id", ""))
        if issue_id in status_by_issue_id:
            row["issue_status"] = status_by_issue_id[issue_id]
        updated_rows.append(row)
    return updated_rows


def status_counts(rows: Iterable[Mapping[str, object]]) -> Dict[str, int]:
    counts = {status: 0 for status in ISSUE_STATUS_VALUES}
    for row in rows:
        status = derive_issue_status(row.get("issue_status", ""), row.get("user_action", ""))
        counts[status] = counts.get(status, 0) + 1
    return counts
