"""Shared data structures for the correction workflow."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Dict, List, Mapping


@dataclass
class FixCandidate:
    """A candidate correction proposed for a single issue row."""

    fix_id: str
    suggested_change: str
    target_field: str
    new_value: object
    confidence: float
    explanation: str
    expected_outcome: str
    fix_strategy: str
    rank: int = 0
    ranking_reason: str = ""

    def to_dict(self) -> Dict[str, object]:
        payload = asdict(self)
        payload["confidence"] = round(float(self.confidence), 4)
        return payload


@dataclass
class IssueRecord:
    """Canonical issue row emitted by the correction subsystem."""

    issue_id: str
    product_id: str
    rule_name: str
    tag: str
    severity: str
    jurisdiction: str
    condition: str
    condition_type: str
    issue_description: str
    rule_explanation: str
    current_values: Dict[str, object]
    current_values_display: str
    confidence: float
    migration_state: str
    migration_state_reason: str
    best_engine: str
    rule_ir_hash: str
    fix_candidates: List[FixCandidate] = field(default_factory=list)

    def with_fixes(self, fix_candidates: List[FixCandidate]) -> "IssueRecord":
        self.fix_candidates = list(fix_candidates)
        return self

    def to_dict(self) -> Dict[str, object]:
        payload = asdict(self)
        payload["confidence"] = round(float(self.confidence), 4)
        payload["fix_candidates"] = [candidate.to_dict() for candidate in self.fix_candidates]
        return payload

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> "IssueRecord":
        fix_candidates = [
            FixCandidate(**candidate)
            for candidate in payload.get("fix_candidates", [])
            if isinstance(candidate, Mapping)
        ]
        record = cls(
            issue_id=str(payload.get("issue_id", "")),
            product_id=str(payload.get("product_id", "")),
            rule_name=str(payload.get("rule_name", "")),
            tag=str(payload.get("tag", "")),
            severity=str(payload.get("severity", "")),
            jurisdiction=str(payload.get("jurisdiction", "")),
            condition=str(payload.get("condition", "")),
            condition_type=str(payload.get("condition_type", "")),
            issue_description=str(payload.get("issue_description", "")),
            rule_explanation=str(payload.get("rule_explanation", "")),
            current_values=dict(payload.get("current_values", {})),
            current_values_display=str(payload.get("current_values_display", "")),
            confidence=float(payload.get("confidence", 0.0)),
            migration_state=str(payload.get("migration_state", "review")),
            migration_state_reason=str(payload.get("migration_state_reason", "")),
            best_engine=str(payload.get("best_engine", "python")),
            rule_ir_hash=str(payload.get("rule_ir_hash", "")),
            fix_candidates=[],
        )
        record.fix_candidates = fix_candidates
        return record


@dataclass
class ReviewDecision:
    """Normalized user decision parsed from the reviewed spreadsheet."""

    issue_id: str
    product_id: str
    rule_name: str
    user_action: str
    issue_status: str
    selected_fix_id: str
    manual_changes: Dict[str, object]
    review_notes: str
    applied_changes: Dict[str, object]
    source_fix_ids: List[str]
    reviewer_name: str = ""
    reviewed_at_utc: str = ""

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


@dataclass
class PatchRecord:
    """A single field-level change applied to the corrected dataset."""

    patch_id: str
    issue_id: str
    product_id: str
    rule_name: str
    field: str
    old_value: object
    new_value: object
    source_fix_id: str
    source_kind: str
    review_notes: str
    reviewer_name: str = ""
    reviewed_at_utc: str = ""

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


@dataclass
class AuditEntry:
    """Audit entry emitted for every applied patch."""

    product_id: str
    field: str
    old_value: object
    new_value: object
    rule_name: str
    fix_id: str
    timestamp: str
    review_action: str
    issue_id: str
    review_notes: str
    reviewer_name: str = ""
    reviewed_at_utc: str = ""

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)
