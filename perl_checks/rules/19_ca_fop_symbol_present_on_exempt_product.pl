# RULE_NAME: ca_fop_symbol_present_on_exempt_product
# SEVERITY: warning
# COMPLEXITY: medium
# DECLARATIVE_FRIENDLY: yes
if (($fop_symbol_present > 0) && ($fop_exempt_proxy > 0) && ($product_is_prepackaged_proxy > 0)) {
    push @{$product_ref->{$data_quality_tags}}, "ca-fop-symbol-present-on-exempt-product";
}
