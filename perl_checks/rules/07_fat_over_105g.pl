# RULE_NAME: fat_over_105g
# SEVERITY: warning
# COMPLEXITY: simple
# DECLARATIVE_FRIENDLY: yes
if ($fat > 105) {
    push @{$product_ref->{$data_quality_tags}}, "fat-value-over-105g";
}
