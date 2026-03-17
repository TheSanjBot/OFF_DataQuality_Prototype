# RULE_NAME: energy_kj_computed_mismatch_high
# SEVERITY: error
# COMPLEXITY: intricate
# DECLARATIVE_FRIENDLY: no
if ($energy_kj_computed > (1.3 * $energy_kj + 5)) {
    push @{$product_ref->{$data_quality_tags}}, "energy-value-in-kj-does-not-match-value-computed-from-other-nutrients-high";
}
