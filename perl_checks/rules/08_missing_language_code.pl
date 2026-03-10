# RULE_NAME: missing_language_code
# SEVERITY: warning
if (!defined $language_code || $language_code eq "") {
    push @{$product_ref->{$data_quality_tags}}, "missing-language-code";
}
