# RULE_NAME: sugars_over_105g
# SEVERITY: warning
# COMPLEXITY: simple
# DECLARATIVE_FRIENDLY: yes
if ($sugars > 105) {
    push @{$product_ref->{$data_quality_tags}}, "sugars-value-over-105g";
}
