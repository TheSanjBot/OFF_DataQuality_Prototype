# RULE_NAME: ca_allergen_evidence_missing_ingredients_text
# SEVERITY: warning
# COMPLEXITY: medium
# DECLARATIVE_FRIENDLY: yes
if (($allergen_evidence_present > 0) && ($ingredients_text_present == 0)) {
    push @{$product_ref->{$data_quality_tags}}, "ca-allergen-evidence-but-missing-ingredients-text";
}
