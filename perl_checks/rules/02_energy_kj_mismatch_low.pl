# RULE_NAME: energy_kj_mismatch_low
# SEVERITY: error
# COMPLEXITY: intricate
# DECLARATIVE_FRIENDLY: no
if ($energy_kj < (3.7 * $energy_kcal - 2)) {
    push @{$product_ref->{$data_quality_tags}}, "energy-value-in-kcal-does-not-match-value-in-kj-low";
}
