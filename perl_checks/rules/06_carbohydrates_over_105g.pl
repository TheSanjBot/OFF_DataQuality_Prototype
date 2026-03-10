# RULE_NAME: carbohydrates_over_105g
# SEVERITY: warning
if ($carbohydrates > 105) {
    push @{$product_ref->{$data_quality_tags}}, "carbohydrates-value-over-105g";
}
