# RULE_NAME: sugars_over_105g
# SEVERITY: warning
if ($sugars > 105) {
    push @{$product_ref->{$data_quality_tags}}, "sugars-value-over-105g";
}
