# RULE_NAME: saturated_fat_over_105g
# SEVERITY: warning
if ($saturated_fat > 105) {
    push @{$product_ref->{$data_quality_tags}}, "saturated-fat-value-over-105g";
}
