# RULE_NAME: energy_kcal_vs_kj
# SEVERITY: error
# COMPLEXITY: simple
# DECLARATIVE_FRIENDLY: yes
if ($energy_kcal > $energy_kj) {
    push @{$product_ref->{$data_quality_tags}}, "energy-value-in-kcal-greater-than-in-kj";
}
