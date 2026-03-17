"""Rule-pack registry for profile-aware migration runs.

Profiles:
- global: OFF-derived generic checks.
- canada: Canada-focused checks with official-source traceability metadata.
- hybrid: union of global + canada (default for backward compatibility).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Sequence

DEFAULT_PROFILE = "hybrid"
SUPPORTED_PROFILES = ("global", "canada", "hybrid")

CANADA_RULES = (
    "main_language_code_missing",
    "main_language_missing",
    "ca_allergen_evidence_missing_ingredients_text",
    "ca_contains_statement_without_allergen_evidence",
    "ca_fop_required_but_symbol_missing",
    "ca_fop_symbol_present_but_not_required",
    "ca_fop_symbol_present_on_exempt_product",
)


@dataclass(frozen=True)
class RuleProfileMetadata:
    jurisdiction: str
    profile_tags: List[str]
    regulatory_type: str
    legal_citation: str
    source_url: str
    effective_date: str
    review_status: str
    reviewer: str
    required_fields: List[str]
    exemption_logic: str
    notes: str


def _default_metadata(rule_name: str) -> RuleProfileMetadata:
    return RuleProfileMetadata(
        jurisdiction="global",
        profile_tags=["global", "hybrid"],
        regulatory_type="off_internal",
        legal_citation="",
        source_url="",
        effective_date="",
        review_status="reviewed",
        reviewer="prototype",
        required_fields=[],
        exemption_logic="none",
        notes=f"OFF-derived global rule: {rule_name}",
    )


RULE_PROFILE_METADATA: Dict[str, RuleProfileMetadata] = {
    "main_language_code_missing": RuleProfileMetadata(
        jurisdiction="ca",
        profile_tags=["canada", "hybrid"],
        regulatory_type="statutory",
        legal_citation="SFCR 206(1); FDR B.01.012(2)",
        source_url="https://laws-lois.justice.gc.ca/eng/regulations/SOR-2018-108/section-206.html",
        effective_date="2019-01-15",
        review_status="draft",
        reviewer="pending-mentor-review",
        required_fields=["lc", "lang", "language_code"],
        exemption_logic="Not all products require bilingual labels; this prototype uses a conservative language-presence proxy.",
        notes="Canada pack: proxy check for missing primary language code in label metadata.",
    ),
    "main_language_missing": RuleProfileMetadata(
        jurisdiction="ca",
        profile_tags=["canada", "hybrid"],
        regulatory_type="statutory",
        legal_citation="SFCR 206(1); FDR B.01.012(2)",
        source_url="https://laws-lois.justice.gc.ca/eng/regulations/C.R.C.,_c._870/section-B.01.012.html",
        effective_date="2019-01-15",
        review_status="draft",
        reviewer="pending-mentor-review",
        required_fields=["lang", "language_code", "lc"],
        exemption_logic="Prototype proxy only; legal exemptions by product class must be modeled before strict enforcement.",
        notes="Canada pack: proxy check for missing primary language value.",
    ),
    "ca_allergen_evidence_missing_ingredients_text": RuleProfileMetadata(
        jurisdiction="ca",
        profile_tags=["canada", "hybrid"],
        regulatory_type="statutory_proxy",
        legal_citation="FDR B.01.010.1(2); FDR B.01.010.3",
        source_url="https://laws-lois.justice.gc.ca/eng/regulations/C.R.C.,_c._870/section-B.01.010.1.html",
        effective_date="2012-08-04",
        review_status="draft",
        reviewer="pending-mentor-review",
        required_fields=["allergen_evidence_present", "ingredients_text_present", "ingredients_text"],
        exemption_logic="Proxy check: flags records with allergen evidence but no ingredient text present.",
        notes="Phase-1 Canada allergen rule using OFF-available proxy fields.",
    ),
    "ca_contains_statement_without_allergen_evidence": RuleProfileMetadata(
        jurisdiction="ca",
        profile_tags=["canada", "hybrid"],
        regulatory_type="statutory_proxy",
        legal_citation="FDR B.01.010.3(1)(b), (2)",
        source_url="https://laws-lois.justice.gc.ca/eng/regulations/C.R.C.,_c._870/section-B.01.010.3.html",
        effective_date="2012-08-04",
        review_status="draft",
        reviewer="pending-mentor-review",
        required_fields=["contains_statement_present", "allergen_evidence_present"],
        exemption_logic="Proxy check: 'contains' proxy without allergen evidence proxy.",
        notes="Phase-1 Canada allergen consistency rule.",
    ),
    "ca_fop_required_but_symbol_missing": RuleProfileMetadata(
        jurisdiction="ca",
        profile_tags=["canada", "hybrid"],
        regulatory_type="statutory_proxy",
        legal_citation="FDR B.01.350(1)",
        source_url="https://laws-lois.justice.gc.ca/eng/regulations/C.R.C.,_c._870/section-B.01.350.html",
        effective_date="2026-01-01",
        review_status="draft",
        reviewer="pending-mentor-review",
        required_fields=[
            "fop_threshold_exceeded",
            "fop_symbol_present",
            "fop_exempt_proxy",
            "product_is_prepackaged_proxy",
        ],
        exemption_logic="Applies only when proxy not exempt and prepackaged proxy is true.",
        notes="Phase-1 Canada FOP threshold-vs-symbol proxy.",
    ),
    "ca_fop_symbol_present_but_not_required": RuleProfileMetadata(
        jurisdiction="ca",
        profile_tags=["canada", "hybrid"],
        regulatory_type="guidance_proxy",
        legal_citation="FDR B.01.350; CFIA FOP guidance",
        source_url="https://inspection.canada.ca/en/food-labels/labelling/industry/nutrition-labelling/fop-nutrition-symbol",
        effective_date="2026-01-01",
        review_status="draft",
        reviewer="pending-mentor-review",
        required_fields=[
            "fop_threshold_exceeded",
            "fop_symbol_present",
            "fop_exempt_proxy",
            "product_is_prepackaged_proxy",
        ],
        exemption_logic="Proxy warning for symbol present when threshold proxy not exceeded and not exempt.",
        notes="Phase-1 Canada FOP over-labelling consistency rule.",
    ),
    "ca_fop_symbol_present_on_exempt_product": RuleProfileMetadata(
        jurisdiction="ca",
        profile_tags=["canada", "hybrid"],
        regulatory_type="guidance_proxy",
        legal_citation="FDR B.01.350(5)-(15)",
        source_url="https://laws-lois.justice.gc.ca/eng/regulations/C.R.C.,_c._870/section-B.01.350.html",
        effective_date="2026-01-01",
        review_status="draft",
        reviewer="pending-mentor-review",
        required_fields=["fop_symbol_present", "fop_exempt_proxy", "product_is_prepackaged_proxy"],
        exemption_logic="Proxy warning on symbol presence for exempt categories.",
        notes="Phase-1 Canada FOP exemption consistency rule.",
    ),
}


def _build_profile_rule_names(all_rule_names: Iterable[str]) -> Dict[str, List[str]]:
    names = list(all_rule_names)
    canada_set = set(CANADA_RULES)
    global_rules = [name for name in names if name not in canada_set]
    canada_rules = [name for name in names if name in canada_set]
    hybrid_rules = names
    return {
        "global": global_rules,
        "canada": canada_rules,
        "hybrid": hybrid_rules,
    }


def validate_profile(profile: str) -> str:
    normalized = profile.strip().lower()
    if normalized not in SUPPORTED_PROFILES:
        raise ValueError(f"Unsupported profile `{profile}`. Supported: {', '.join(SUPPORTED_PROFILES)}")
    return normalized


def get_profile_rule_names(profile: str, all_rule_names: Sequence[str]) -> List[str]:
    normalized = validate_profile(profile)
    profile_map = _build_profile_rule_names(all_rule_names)
    return list(profile_map[normalized])


def attach_profile_metadata(rules: Sequence[Mapping[str, object]], profile: str) -> List[Dict[str, object]]:
    validate_profile(profile)
    out: List[Dict[str, object]] = []
    for rule in rules:
        rule_name = str(rule.get("rule_name", ""))
        meta = RULE_PROFILE_METADATA.get(rule_name, _default_metadata(rule_name))
        row = dict(rule)
        row["jurisdiction"] = meta.jurisdiction
        row["profile_tags"] = list(meta.profile_tags)
        row["regulatory_type"] = meta.regulatory_type
        row["legal_citation"] = meta.legal_citation
        row["source_url"] = meta.source_url
        row["effective_date"] = meta.effective_date
        row["review_status"] = meta.review_status
        row["reviewer"] = meta.reviewer
        row["required_fields"] = list(meta.required_fields)
        row["exemption_logic"] = meta.exemption_logic
        row["rule_notes"] = meta.notes
        out.append(row)
    return out
