# RULE_NAME: main_language_code_missing
# SEVERITY: bug
# COMPLEXITY: simple
# DECLARATIVE_FRIENDLY: yes
if (!defined $lc || $lc eq "") {
    push @{$product_ref->{$data_quality_tags}}, "main-language-code-missing";
}
