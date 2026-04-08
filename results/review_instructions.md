# Correction Review Instructions

Generated at: 2026-04-06T10:04:05.342982+00:00
Review file: `review_sheet.csv`

## Purpose
Review flagged data-quality issues in a spreadsheet-friendly format, choose a suggested fix or enter a manual edit, and send the reviewed sheet back for safe batch application.

## Recommended workflow
1. Open the CSV in Google Sheets or Excel.
2. Do not delete or rename columns.
3. For each row, either:
   - choose a `selected_fix_id` from one of the suggested fix columns, or
   - enter a `manual_edit` using `field=value` pairs.
4. Set `user_action` to `approve` or `reject`.
5. Leave `issue_status` alone if possible; the system updates it as the workflow progresses.
6. Optionally add `review_notes`, `reviewer_name`, and `reviewed_at_utc`.
7. Export the reviewed file back to CSV before importing it into the correction system.

## Column guide
- `selected_fix_id`: copy just the fix id, for example `abc123_limit`
- `manual_edit`: examples: `carbohydrates=105` or `fat=10;saturated_fat=8`
- `user_action`: `approve` or `reject`
- `issue_status`: system-managed workflow state such as `pending_review`, `approved`, `applied`
- `reviewer_name`: optional reviewer name or initials
- `reviewed_at_utc`: optional ISO timestamp such as `2026-04-03T10:30:00Z`

## Validation rules
- `issue_id` must stay unchanged
- `selected_fix_id` must match the row's suggested fixes
- `manual_edit` must use valid `field=value` pairs if provided
- approved rows with no selected fix and no manual edit will be skipped
