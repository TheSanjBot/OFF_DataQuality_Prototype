"""Apply approved correction decisions as a safe patch overlay."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Dict, List, Tuple

from correction.review_status import ISSUE_STATUS_APPROVED, ISSUE_STATUS_REJECTED
from correction.schemas import PatchRecord, ReviewDecision


def _load_jsonl_records(path: Path) -> Tuple[List[Dict[str, object]], Dict[str, Dict[str, object]]]:
    records: List[Dict[str, object]] = []
    by_product_id: Dict[str, Dict[str, object]] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            records.append(row)
            by_product_id[str(row.get("product_id", ""))] = row
    return records, by_product_id


def _patch_id(issue_id: str, product_id: str, field: str, new_value: object) -> str:
    payload = f"{issue_id}:{product_id}:{field}:{json.dumps(new_value, sort_keys=True, default=str)}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:14]


def apply_review_decisions(
    source_jsonl_path: Path,
    decisions: List[ReviewDecision],
    corrected_dataset_path: Path,
    patches_path: Path,
) -> Dict[str, object]:
    records, records_by_id = _load_jsonl_records(source_jsonl_path)
    patches: List[PatchRecord] = []
    approved_rows = 0
    rejected_rows = 0
    skipped_rows = 0

    for decision in decisions:
        if decision.issue_status == ISSUE_STATUS_REJECTED:
            rejected_rows += 1
            continue
        if decision.issue_status != ISSUE_STATUS_APPROVED:
            skipped_rows += 1
            continue
        approved_rows += 1

        product = records_by_id.get(decision.product_id)
        if product is None:
            skipped_rows += 1
            continue
        if not decision.applied_changes:
            skipped_rows += 1
            continue

        for field, new_value in decision.applied_changes.items():
            old_value = product.get(field)
            if old_value == new_value:
                continue
            product[field] = new_value
            source_fix_id = decision.selected_fix_id if field not in decision.manual_changes else "manual_edit"
            source_kind = "manual_edit" if field in decision.manual_changes else "suggested_fix"
            patches.append(
                PatchRecord(
                    patch_id=_patch_id(decision.issue_id, decision.product_id, field, new_value),
                    issue_id=decision.issue_id,
                    product_id=decision.product_id,
                    rule_name=decision.rule_name,
                    field=field,
                    old_value=old_value,
                    new_value=new_value,
                    source_fix_id=source_fix_id,
                    source_kind=source_kind,
                    review_notes=decision.review_notes,
                    reviewer_name=decision.reviewer_name,
                    reviewed_at_utc=decision.reviewed_at_utc,
                )
            )

    corrected_dataset_path.parent.mkdir(parents=True, exist_ok=True)
    with corrected_dataset_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=True) + "\n")

    patches_path.parent.mkdir(parents=True, exist_ok=True)
    with patches_path.open("w", encoding="utf-8") as handle:
        for patch in patches:
            handle.write(json.dumps(patch.to_dict(), ensure_ascii=True) + "\n")

    return {
        "source_jsonl_path": str(source_jsonl_path),
        "corrected_dataset_path": str(corrected_dataset_path),
        "patches_path": str(patches_path),
        "review_rows": len(decisions),
        "approved_rows": approved_rows,
        "rejected_rows": rejected_rows,
        "skipped_rows": skipped_rows,
        "applied_patch_count": len(patches),
        "changed_products": len({patch.product_id for patch in patches}),
        "patches": [patch.to_dict() for patch in patches],
    }
