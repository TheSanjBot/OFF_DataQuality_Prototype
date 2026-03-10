# RULE_NAME: energy_kcal_vs_kj
# SEVERITY: error
if ($energy_kcal > $energy_kj) {
    push @{$product_ref->{$data_quality_tags}}, "energy-value-in-kcal-greater-than-in-kj";
}
