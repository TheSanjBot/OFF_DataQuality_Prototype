# RULE_NAME: saturated_fat_vs_fat
# SEVERITY: error
# COMPLEXITY: simple
# DECLARATIVE_FRIENDLY: yes
if ($saturated_fat > (1 * $fat + 0.001)) {
    push @{$product_ref->{$data_quality_tags}}, "saturated-fat-greater-than-fat";
}
