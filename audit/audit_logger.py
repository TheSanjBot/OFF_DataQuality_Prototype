"""Write audit entries for applied corrections."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

from correction.schemas import AuditEntry, PatchRecord


def build_audit_entries(patches: Iterable[PatchRecord]) -> List[AuditEntry]:
    timestamp = datetime.now(timezone.utc).isoformat()
    entries: List[AuditEntry] = []
    for patch in patches:
        entries.append(
            AuditEntry(
                product_id=patch.product_id,
                field=patch.field,
                old_value=patch.old_value,
                new_value=patch.new_value,
                rule_name=patch.rule_name,
                fix_id=patch.source_fix_id,
                timestamp=timestamp,
                review_action="approve",
                issue_id=patch.issue_id,
                review_notes=patch.review_notes,
                reviewer_name=patch.reviewer_name,
                reviewed_at_utc=patch.reviewed_at_utc,
            )
        )
    return entries


def write_audit_log(entries: Iterable[AuditEntry], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for entry in entries:
            handle.write(json.dumps(entry.to_dict(), ensure_ascii=True) + "\n")
