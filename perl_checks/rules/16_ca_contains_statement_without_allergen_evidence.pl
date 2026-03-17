# RULE_NAME: ca_contains_statement_without_allergen_evidence
# SEVERITY: warning
# COMPLEXITY: medium
# DECLARATIVE_FRIENDLY: yes
if (($contains_statement_present > 0) && ($allergen_evidence_present == 0)) {
    push @{$product_ref->{$data_quality_tags}}, "ca-contains-statement-without-allergen-evidence";
}
