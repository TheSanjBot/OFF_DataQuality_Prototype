# RULE_NAME: sugars_plus_starch_vs_carbohydrates
# SEVERITY: error
# COMPLEXITY: medium
# DECLARATIVE_FRIENDLY: yes
if (($sugars + $starch) > ($carbohydrates + 0.001)) {
    push @{$product_ref->{$data_quality_tags}}, "sugars-plus-starch-greater-than-carbohydrates";
}
