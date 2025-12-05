/*
error:unbound_var_in_negative_fact(pipeline_id:, rule_id:, fact_id:, var_name:) <-
    var_bound_in_fact(pipeline_id:, rule_id:, fact_id:, var_name:, negated: true)
    not var_bound_in_fact(pipeline_id:, rule_id:, var_name:, negated: false)
*/
CREATE MATERIALIZED VIEW "error:unbound_var_in_negative_fact" AS
    SELECT DISTINCT
        var_bound_in_fact.pipeline_id,
        var_bound_in_fact.rule_id,
        var_bound_in_fact.fact_id,
        var_bound_in_fact.var_name
    FROM var_bound_in_fact
    WHERE var_bound_in_fact.negated
    AND NOT EXISTS (
        SELECT 1
        FROM var_bound_in_fact
        WHERE var_bound_in_fact.pipeline_id = var_bound_in_fact.pipeline_id
        AND var_bound_in_fact.rule_id = var_bound_in_fact.rule_id
        AND var_bound_in_fact.var_name = var_bound_in_fact.var_name
        AND NOT var_bound_in_fact.negated
    );

/*
error:neg_fact_sql_unresolved(pipeline_id:, rule_id:, fact_id:) <-
    body_fact(pipeline_id:, rule_id:, fact_id:, negated: true)
    fact_arg(pipeline_id:, rule_id:, fact_id:, expr_id:, expr_type:)
    not substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type:)
*/
CREATE MATERIALIZED VIEW "error:neg_fact_sql_unresolved" AS
    SELECT DISTINCT
        body_fact.pipeline_id,
        body_fact.rule_id,
        body_fact.fact_id
    FROM body_fact
    JOIN fact_arg
        ON body_fact.pipeline_id = fact_arg.pipeline_id
        AND body_fact.rule_id = fact_arg.rule_id
        AND body_fact.fact_id = fact_arg.fact_id
    WHERE body_fact.negated
    AND NOT EXISTS (
        SELECT 1
        FROM substituted_expr
        WHERE pipeline_id = fact_arg.pipeline_id
        AND rule_id = fact_arg.rule_id
        AND expr_id = fact_arg.expr_id
        AND expr_type = fact_arg.expr_type);

/*
error:match_right_expr_unresolved(pipeline_id:, rule_id:, match_id:) <-
    body_match(pipeline_id:, rule_id:, left_expr_id:, left_expr_type:)
    not substituted_expr(pipeline_id:, expr_id: left_expr_id, expr_type: left_expr_type)
*/
CREATE MATERIALIZED VIEW "error:match_right_expr_unresolved" AS
    SELECT DISTINCT
        body_match.pipeline_id,
        body_match.rule_id,
        body_match.match_id
    FROM body_match
    WHERE NOT EXISTS (
        SELECT 1
        FROM substituted_expr
        WHERE pipeline_id = body_match.pipeline_id
        AND rule_id = body_match.rule_id
        AND expr_id = body_match.left_expr_id
        AND expr_type = body_match.left_expr_type
    );

/*
error:invalid_aggr_expr(pipeline_id:, rule_id:, expr_id:) <-
    fncall_expr(pipeline_id:, rule_id:, expr_id:, aggregated: true)
    not aggr_expr_matching_signature(pipeline_id:, rule_id:, expr_id:)
*/
CREATE MATERIALIZED VIEW "error:invalid_aggr_expr" AS
    SELECT DISTINCT
        fncall_expr.pipeline_id,
        fncall_expr.rule_id,
        fncall_expr.expr_id
    FROM fncall_expr
    WHERE fncall_expr.aggregated
    AND NOT EXISTS (
        SELECT 1
        FROM aggr_expr_matching_signature
        WHERE fncall_expr.pipeline_id = aggr_expr_matching_signature.pipeline_id
        AND fncall_expr.rule_id = aggr_expr_matching_signature.rule_id
        AND fncall_expr.expr_id = aggr_expr_matching_signature.expr_id
    );

/*
error:expr_that_is_neither_pattern_nor_value(pipeline_id:, rule_id:, expr_id:, expr_type:) <-
    expr(pipeline_id:, rule_id:, expr_id:, expr_type:)
    not pattern_expr(pipeline_id:, rule_id:, expr_id:, expr_type:)
    not value_expr(pipeline_id:, rule_id:, expr_id:, expr_type:)
*/
CREATE MATERIALIZED VIEW "error:expr_that_is_neither_pattern_nor_value" AS
    SELECT DISTINCT
        expr.pipeline_id,
        expr.rule_id,
        expr.expr_id,
        expr.expr_type
    FROM expr
    WHERE NOT EXISTS (
        SELECT 1
        FROM pattern_expr
        WHERE expr.pipeline_id = pattern_expr.pipeline_id
        AND expr.rule_id = pattern_expr.rule_id
        AND expr.expr_id = pattern_expr.expr_id
        AND expr.expr_type = pattern_expr.expr_type
    )
    AND NOT EXISTS (
        SELECT 1
        FROM value_expr
        WHERE expr.pipeline_id = value_expr.pipeline_id
        AND expr.rule_id = value_expr.rule_id
        AND expr.expr_id = value_expr.expr_id
        AND expr.expr_type = value_expr.expr_type
    );

/*
error:two_asterisk_vars_in_array_pattern(
    pipeline_id:, rule_id:, first_element_expr_id:, second_element_expr_id:
) <-
    pattern_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "array_expr")
    array_expr(pipeline_id:, rule_id:, expr_id:)
    first_element_expr_id != second_element_expr_id

    array_entry(
        pipeline_id:, rule_id:,
        expr_id: first_element_expr_id, expr_type: "var_expr")
    var_expr(pipeline_id:, rule_id:, special_prefix: "*", expr_id: first_element_expr_id)

    array_entry(
        pipeline_id:, rule_id:,
        expr_id: second_element_expr_id, expr_type: "var_expr")
    var_expr(pipeline_id:, rule_id:, special_prefix: "*", expr_id: second_element_expr_id)
*/
CREATE MATERIALIZED VIEW "error:two_asterisk_vars_in_array_pattern" AS
    SELECT DISTINCT
        pattern_expr.pipeline_id,
        pattern_expr.rule_id,
        first_array_entry.expr_id AS first_element_expr_id,
        second_array_entry.expr_id AS second_element_expr_id
    FROM pattern_expr
    JOIN array_expr
        ON pattern_expr.pipeline_id = array_expr.pipeline_id
        AND pattern_expr.rule_id = array_expr.rule_id
        AND pattern_expr.expr_id = array_expr.expr_id

    JOIN array_entry AS first_array_entry
        ON array_expr.pipeline_id = first_array_entry.pipeline_id
        AND array_expr.rule_id = first_array_entry.rule_id
        AND array_expr.array_id = first_array_entry.array_id
    JOIN var_expr AS first_element_expr
        ON first_array_entry.pipeline_id = first_element_expr.pipeline_id
        AND first_array_entry.rule_id = first_element_expr.rule_id
        AND first_array_entry.expr_id = first_element_expr.expr_id

    JOIN array_entry AS second_array_entry
        ON array_expr.pipeline_id = second_array_entry.pipeline_id
        AND array_expr.rule_id = second_array_entry.rule_id
        AND array_expr.array_id = second_array_entry.array_id
    JOIN var_expr AS second_element_expr
        ON second_array_entry.pipeline_id = second_element_expr.pipeline_id
        AND second_array_entry.rule_id = second_element_expr.rule_id
        AND second_array_entry.expr_id = second_element_expr.expr_id

    WHERE first_array_entry.expr_id != second_array_entry.expr_id
        AND first_element_expr.special_prefix = '*'
        AND second_element_expr.special_prefix = '*'
        AND pattern_expr.expr_type = 'array_expr';

/*
error:asterisk_used_outside_of_array(pipeline_id:, rule_id:, expr_id:) <-
    var_expr(pipeline_id:, rule_id:, expr_id:, special_prefix: "*")
    not array_entry(pipeline_id:, rule_id:, expr_id:, expr_type: "var_expr")
*/
CREATE MATERIALIZED VIEW "error:asterisk_used_outside_of_array" AS
    SELECT DISTINCT
        var_expr.pipeline_id,
        var_expr.rule_id,
        var_expr.expr_id
    FROM var_expr
    WHERE var_expr.special_prefix = '*'
    AND NOT EXISTS (
        SELECT 1
        FROM array_entry
        WHERE var_expr.pipeline_id = array_entry.pipeline_id
        AND var_expr.rule_id = array_entry.rule_id
        AND var_expr.expr_id = array_entry.expr_id
    );

/*
error:asterisk_used_outside_of_pattern_expr(pipeline_id:, rule_id:, expr_id:) <-
    var_expr(pipeline_id:, rule_id:, expr_id:, special_prefix: "*")
    not pattern_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "var_expr")
*/
CREATE MATERIALIZED VIEW "error:asterisk_used_outside_of_pattern_expr" AS
    SELECT DISTINCT
        var_expr.pipeline_id,
        var_expr.rule_id,
        var_expr.expr_id
    FROM var_expr
    WHERE var_expr.special_prefix = '*'
    AND NOT EXISTS (
        SELECT 1
        FROM pattern_expr
        WHERE var_expr.pipeline_id = pattern_expr.pipeline_id
        AND var_expr.rule_id = pattern_expr.rule_id
        AND var_expr.expr_id = pattern_expr.expr_id
    );

/*
# Actually, I think having variable bound twice shouldn't be an error.
# It should mean that values in both places should be equal.
# However, it would require some additional work on dependency analysis,
# so I'll leave it out for now.
error:var_bound_twice_via_match(
    pipeline_id:, rule_id:, first_match_id:, second_match_id: expr_id:
) <-
    # TODO
*/

/*
error(pipeline_id:, error_type: "unbound_var_in_negative_fact") <-
    error:unbound_var_in_negative_fact(pipeline_id:)
error(pipeline_id:, error_type: "neg_fact_sql_unresolved") <-
    error:neg_fact_sql_unresolved(pipeline_id:)
error(pipeline_id:, error_type: "match_right_expr_unresolved") <-
    error:match_right_expr_unresolved(pipeline_id:)
error(pipeline_id:, error_type: "invalid_aggr_expr") <-
    error:invalid_aggr_expr(pipeline_id:)
error(pipeline_id:, error_type: "two_asterisk_vars_in_array_pattern") <-
    error:two_asterisk_vars_in_array_pattern(pipeline_id:)
error(pipeline_id:, error_type: "expr_that_is_neither_pattern_nor_value") <-
    error:expr_that_is_neither_pattern_nor_value(pipeline_id:)
error(pipeline_id:, error_type: "asterisk_used_outside_of_array") <-
    error:asterisk_used_outside_of_array(pipeline_id:)
error(pipeline_id:, error_type: "asterisk_used_outside_of_pattern_expr") <-
    error:asterisk_used_outside_of_pattern_expr(pipeline_id:)
*/
CREATE MATERIALIZED VIEW "error" AS
    SELECT DISTINCT
        "error:unbound_var_in_negative_fact".pipeline_id AS pipeline_id,
        'unbound_var_in_negative_fact' AS error_type
    FROM "error:unbound_var_in_negative_fact"
    UNION
    SELECT DISTINCT
        "error:neg_fact_sql_unresolved".pipeline_id AS pipeline_id,
        'neg_fact_sql_unresolved' AS error_type
    FROM "error:neg_fact_sql_unresolved"
    UNION
    SELECT DISTINCT
        "error:match_right_expr_unresolved".pipeline_id AS pipeline_id,
        'match_right_expr_unresolved' AS error_type
    FROM "error:match_right_expr_unresolved"
    UNION
    SELECT DISTINCT
        "error:invalid_aggr_expr".pipeline_id AS pipeline_id,
        'invalid_aggr_expr' AS error_type
    FROM "error:invalid_aggr_expr"
    UNION
    SELECT DISTINCT
        "error:two_asterisk_vars_in_array_pattern".pipeline_id AS pipeline_id,
        'two_asterisk_vars_in_array_pattern' AS error_type
    FROM "error:two_asterisk_vars_in_array_pattern"
    UNION
    SELECT DISTINCT
        "error:expr_that_is_neither_pattern_nor_value".pipeline_id AS pipeline_id,
        'expr_that_is_neither_pattern_nor_value' AS error_type
    FROM "error:expr_that_is_neither_pattern_nor_value"
    UNION
    SELECT DISTINCT
        "error:asterisk_used_outside_of_array".pipeline_id AS pipeline_id,
        'asterisk_used_outside_of_array' AS error_type
    FROM "error:asterisk_used_outside_of_array"
    UNION
    SELECT DISTINCT
        "error:asterisk_used_outside_of_pattern_expr".pipeline_id AS pipeline_id,
        'asterisk_used_outside_of_pattern_expr' AS error_type
    FROM "error:asterisk_used_outside_of_pattern_expr";

