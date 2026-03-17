# RULE_NAME: carbohydrates_over_105g
# SEVERITY: warning
# COMPLEXITY: simple
# DECLARATIVE_FRIENDLY: yes
if ($carbohydrates > 105) {
    push @{$product_ref->{$data_quality_tags}}, "carbohydrates-value-over-105g";
}
