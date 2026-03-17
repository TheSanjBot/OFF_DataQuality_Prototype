# RULE_NAME: energy_kj_over_3911
# SEVERITY: error
# COMPLEXITY: simple
# DECLARATIVE_FRIENDLY: yes
if ($energy_kj > 3911) {
    push @{$product_ref->{$data_quality_tags}}, "value-over-3911-energy";
}
