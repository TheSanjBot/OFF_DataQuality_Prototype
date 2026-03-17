# RULE_NAME: main_language_missing
# SEVERITY: bug
# COMPLEXITY: simple
# DECLARATIVE_FRIENDLY: yes
if (!defined $lang || $lang eq "") {
    push @{$product_ref->{$data_quality_tags}}, "main-language-missing";
}
