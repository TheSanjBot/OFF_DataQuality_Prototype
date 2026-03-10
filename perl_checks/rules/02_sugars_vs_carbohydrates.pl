# RULE_NAME: sugars_vs_carbohydrates
# SEVERITY: error
if ($sugars > $carbohydrates) {
    push @{$product_ref->{$data_quality_tags}}, "sugars-greater-than-carbohydrates";
}
