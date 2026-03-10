# RULE_NAME: saturated_fat_vs_fat
# SEVERITY: error
if ($saturated_fat > $fat) {
    push @{$product_ref->{$data_quality_tags}}, "saturated-fat-greater-than-fat";
}
