
/*
# same variable in the same rule can be bound several times.
# we select one binding as canonical, and use it for grouping and return expressions.
*/

/*
canonical_var_bound_sql(pipeline_id:, rule_id:, var_name:, sql:, aggregated: false) <-
    canonical_fact_var_sql(pipeline_id:, rule_id:, var_name:, sql:)
canonical_var_bound_sql(
    pipeline_id:, rule_id:, var_name:, sql: min<sql>, aggregated: some<aggregated>
) <-
    var_bound_via_match(pipeline_id:, rule_id:, var_name:, sql:, aggregated:)
    not canonical_fact_var_sql(pipeline_id:, rule_id:, var_name:)
*/
DECLARE RECURSIVE VIEW var_bound_via_match (pipeline_id TEXT, rule_id TEXT, match_id TEXT, var_name TEXT, sql TEXT, aggregated BOOLEAN);
DECLARE RECURSIVE VIEW canonical_var_bound_sql (pipeline_id TEXT, rule_id TEXT, var_name TEXT, sql TEXT, aggregated BOOLEAN);
CREATE MATERIALIZED VIEW canonical_var_bound_sql AS
    SELECT DISTINCT
        canonical_fact_var_sql.pipeline_id,
        canonical_fact_var_sql.rule_id,
        canonical_fact_var_sql.var_name,
        canonical_fact_var_sql.sql,
        false AS aggregated
    FROM canonical_fact_var_sql

    UNION

    SELECT DISTINCT
        var_bound_via_match.pipeline_id,
        var_bound_via_match.rule_id,
        var_bound_via_match.var_name,
        MIN(var_bound_via_match.sql) AS sql,
        SOME(var_bound_via_match.aggregated) AS aggregated
    FROM var_bound_via_match
    WHERE NOT EXISTS (
        SELECT 1
        FROM canonical_fact_var_sql
        WHERE canonical_fact_var_sql.pipeline_id = var_bound_via_match.pipeline_id
        AND canonical_fact_var_sql.rule_id = var_bound_via_match.rule_id
        AND canonical_fact_var_sql.var_name = var_bound_via_match.var_name
    )
    GROUP BY var_bound_via_match.pipeline_id, var_bound_via_match.rule_id, var_bound_via_match.var_name
    ;

/*
sql_expr_template_part_with_substitution(pipeline_id:, rule_id:, expr_id:, part:, index:) <-
    sql_expr_template_part(pipeline_id:, rule_id:, expr_id:, part: source_part, index:)
    source_part ~ "{{[a-z_][a-zA-Z0-9_]*}}"
    var_name := part[2:-2]
    canonical_var_bound_sql(pipeline_id:, rule_id:, var_name:, sql: part)
sql_expr_template_part_with_substitution(pipeline_id:, rule_id:, expr_id:, part:, index:) <-
    sql_expr_template_part(pipeline_id:, rule_id:, expr_id:, part:, index:)
    not (part ~ "{{[a-z_][a-zA-Z0-9_]*}}")
*/
-- DECLARE RECURSIVE VIEW sql_expr_template_part_with_substitution (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, part TEXT, "index" INTEGER);
-- CREATE MATERIALIZED VIEW sql_expr_template_part_with_substitution AS
--     SELECT DISTINCT
--         sql_expr_template_part.pipeline_id,
--         sql_expr_template_part.rule_id,
--         sql_expr_template_part.expr_id,
--         canonical_var_bound_sql.sql AS part,
--         sql_expr_template_part."index"
--     FROM sql_expr_template_part
--     JOIN canonical_var_bound_sql
--         ON sql_expr_template_part.pipeline_id = canonical_var_bound_sql.pipeline_id
--         AND sql_expr_template_part.rule_id = canonical_var_bound_sql.rule_id
--         AND SUBSTRING(sql_expr_template_part.part FROM 3 FOR (CHAR_LENGTH(sql_expr_template_part.part)-4)) = canonical_var_bound_sql.var_name
--     WHERE sql_expr_template_part.part RLIKE '^\{\{[a-zA-Z_][A-Za-z0-9_:]*\}\}$'

--     UNION

--     SELECT DISTINCT
--         sql_expr_template_part.pipeline_id,
--         sql_expr_template_part.rule_id,
--         sql_expr_template_part.expr_id,
--         sql_expr_template_part.part,
--         sql_expr_template_part."index"
--     FROM sql_expr_template_part
--     WHERE NOT (sql_expr_template_part.part RLIKE '^\{\{[a-zA-Z_][A-Za-z0-9_:]*\}\}$');

/*
sql_expr_substitution_status(pipeline_id:, rule_id:, expr_id:, count: count<>) <-
    sql_expr_template_part_with_substitution(pipeline_id:, rule_id:, expr_id:, index:)
*/
-- DECLARE RECURSIVE VIEW sql_expr_substitution_status (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, count BIGINT);
-- CREATE MATERIALIZED VIEW sql_expr_substitution_status AS
--     SELECT DISTINCT
--         t.pipeline_id,
--         t.rule_id,
--         t.expr_id,
--         COUNT(*) AS count
--     FROM sql_expr_template_part_with_substitution AS t
--     GROUP BY t.pipeline_id, t.rule_id, t.expr_id;

/*
sql_expr_all_vars_are_bound(pipeline_id:, rule_id:, expr_id:) <-
    sql_expr(pipeline_id:, rule_id:, expr_id:, template:)
    sql_expr_substitution_status(pipeline_id:, rule_id:, expr_id:, count:)
    count = array_length(template)
*/
-- DECLARE RECURSIVE VIEW sql_expr_all_vars_are_bound (pipeline_id TEXT, rule_id TEXT, expr_id TEXT);
-- CREATE MATERIALIZED VIEW sql_expr_all_vars_are_bound AS
--     SELECT DISTINCT
--         sql_expr.pipeline_id,
--         sql_expr.rule_id,
--         sql_expr.expr_id
--     FROM sql_expr
--     JOIN sql_expr_substitution_status
--         ON sql_expr.pipeline_id = sql_expr_substitution_status.pipeline_id
--         AND sql_expr.rule_id = sql_expr_substitution_status.rule_id
--         AND sql_expr.expr_id = sql_expr_substitution_status.expr_id
--     WHERE sql_expr_substitution_status.count = ARRAY_LENGTH(sql_expr.template);

/*
substituted_sql_expr(pipeline_id:, rule_id:, expr_id:, sql:) <-
    sql_expr_all_vars_are_bound(pipeline_id:, rule_id:, expr_id:)
    sql_expr_template_part_with_substitution(pipeline_id:, rule_id:, expr_id:, part:, index:)
    sql := join(array<part, order_by: [index]>, "")
*/
-- DECLARE RECURSIVE VIEW substituted_sql_expr (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, sql TEXT);
-- CREATE MATERIALIZED VIEW substituted_sql_expr AS
--     SELECT DISTINCT
--         a.pipeline_id,
--         a.rule_id,
--         a.expr_id,
--         ARRAY_TO_STRING(ARRAY_AGG(b.part ORDER BY b."index"), '') AS sql
--     FROM sql_expr_all_vars_are_bound AS a
--     JOIN sql_expr_template_part_with_substitution AS b
--         ON a.pipeline_id = b.pipeline_id
--         AND a.rule_id = b.rule_id
--         AND a.expr_id = b.expr_id
--     GROUP BY a.pipeline_id, a.rule_id, a.expr_id;

DECLARE RECURSIVE VIEW substituted_expr (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, expr_type TEXT, sql TEXT, aggregated BOOLEAN);

/*
str_template_part(pipeline_id:, rule_id:, expr_id:, part:, index:) <-
    str_template_expr(pipeline_id:, rule_id:, expr_id:, template:)
    (element: part, index:) <- template
*/
CREATE MATERIALIZED VIEW str_template_part AS
    SELECT DISTINCT
        str_template_expr.pipeline_id,
        str_template_expr.rule_id,
        str_template_expr.expr_id,
        t.part,
        t."index"
    FROM str_template_expr
    CROSS JOIN UNNEST(str_template_expr.template) WITH ORDINALITY AS t (part, "index");

/*
str_template_part_with_substitution(
    pipeline_id:, rule_id:, expr_id:, part:, index:, aggregated:
) <-
    str_template_part(pipeline_id:, rule_id:, expr_id:, part: source_part, index:)
    source_part ~ "{{[a-z_][a-zA-Z0-9_]*}}"
    var_name := part[2:-2]
    canonical_var_bound_sql(pipeline_id:, rule_id:, var_name:, sql: part, aggregated:)
str_template_part_with_substitution(
    pipeline_id:, rule_id:, expr_id:, part:, index:, aggregated: false
) <-
    str_template_part(pipeline_id:, rule_id:, expr_id:, part:, index:)
    not (part ~ "{{[a-z_][a-zA-Z0-9_]*}}")
*/
DECLARE RECURSIVE VIEW str_template_part_with_substitution (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, part TEXT, "index" INTEGER, aggregated BOOLEAN);
CREATE MATERIALIZED VIEW str_template_part_with_substitution AS
    SELECT DISTINCT
        str_template_part.pipeline_id,
        str_template_part.rule_id,
        str_template_part.expr_id,
        canonical_var_bound_sql.sql AS part,
        str_template_part."index",
        canonical_var_bound_sql.aggregated
    FROM str_template_part
    JOIN canonical_var_bound_sql
        ON str_template_part.pipeline_id = canonical_var_bound_sql.pipeline_id
        AND str_template_part.rule_id = canonical_var_bound_sql.rule_id
        AND SUBSTRING(str_template_part.part FROM 3 FOR (CHAR_LENGTH(str_template_part.part)-4)) = canonical_var_bound_sql.var_name
    WHERE str_template_part.part RLIKE '^\{\{[a-zA-Z_][A-Za-z0-9_:]*\}\}$'

    UNION

    SELECT DISTINCT
        str_template_part.pipeline_id,
        str_template_part.rule_id,
        str_template_part.expr_id,
        ('''' || str_template_part.part || '''') AS part,
        str_template_part."index",
        false AS aggregated
    FROM str_template_part
    WHERE NOT (str_template_part.part RLIKE '^\{\{[a-zA-Z_][A-Za-z0-9_:]*\}\}$');

/*
str_template_expr_all_vars_are_bound(pipeline_id:, rule_id:, expr_id:) <-
    str_template_expr(pipeline_id:, rule_id:, expr_id:, template:)
    str_template_part_with_substitution(pipeline_id:, rule_id:, expr_id:)
    count<> = array_length(template)
*/
DECLARE RECURSIVE VIEW str_template_expr_all_vars_are_bound (pipeline_id TEXT, rule_id TEXT, expr_id TEXT);
CREATE MATERIALIZED VIEW str_template_expr_all_vars_are_bound AS
    SELECT DISTINCT
        str_template_expr.pipeline_id,
        str_template_expr.rule_id,
        str_template_expr.expr_id
    FROM str_template_expr
    JOIN str_template_part_with_substitution
        ON str_template_expr.pipeline_id = str_template_part_with_substitution.pipeline_id
        AND str_template_expr.rule_id = str_template_part_with_substitution.rule_id
        AND str_template_expr.expr_id = str_template_part_with_substitution.expr_id
    GROUP BY str_template_expr.pipeline_id, str_template_expr.rule_id, str_template_expr.expr_id, str_template_expr.template
    HAVING COUNT(*) = ARRAY_LENGTH(str_template_expr.template);

/*
sustituted_str_template_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated:) <-
    str_template_expr_all_vars_are_bound(pipeline_id:, rule_id:, expr_id:)
    str_template_part_with_substitution(pipeline_id:, rule_id:, expr_id:, part:, index:)
    sql := "(" ++ join(array<part, order_by: [index]>, " || ") ++ ")"
*/
DECLARE RECURSIVE VIEW substituted_str_template_expr (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, sql TEXT, aggregated BOOLEAN);
CREATE MATERIALIZED VIEW substituted_str_template_expr AS
    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        (
            '(' ||
            ARRAY_TO_STRING(ARRAY_AGG(b.part ORDER BY b."index"), ' || ') ||
            ')') AS sql,
        SOME(b.aggregated) AS aggregated
    FROM str_template_expr_all_vars_are_bound AS a
    JOIN str_template_part_with_substitution AS b
        ON a.pipeline_id = b.pipeline_id
        AND a.rule_id = b.rule_id
        AND a.expr_id = b.expr_id
    GROUP BY a.pipeline_id, a.rule_id, a.expr_id;

/*
substituted_val_arg(pipeline_id:, rule_id:, fncall_id:, arg_index:, sql:) <-
    fn_val_arg(
        pipeline_id:, rule_id:, fncall_id:,
        arg_index:, expr_id:, expr_type:)
    substituted_expr(
        pipeline_id:, rule_id:, expr_id:, expr_type:, sql:)
*/
DECLARE RECURSIVE VIEW substituted_val_arg (pipeline_id TEXT, rule_id TEXT, fncall_id TEXT, arg_index INTEGER, sql TEXT);
CREATE MATERIALIZED VIEW substituted_val_arg AS
    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.fncall_id,
        a.arg_index,
        b.sql
    FROM fn_val_arg AS a
    JOIN substituted_expr AS b
        ON a.pipeline_id = b.pipeline_id
        AND a.rule_id = b.rule_id
        AND a.expr_id = b.expr_id
        AND a.expr_type = b.expr_type;

/*
substituted_kv_arg(pipeline_id:, rule_id:, fncall_id:, key:, sql:) <-
    fn_kv_arg(
        pipeline_id:, rule_id:, fncall_id:,
        key:, expr_id:, expr_type:)
    substituted_expr(
        pipeline_id:, rule_id:, expr_id:, expr_type:, sql:)
*/
DECLARE RECURSIVE VIEW substituted_kv_arg (pipeline_id TEXT, rule_id TEXT, fncall_id TEXT, key TEXT, sql TEXT);
CREATE MATERIALIZED VIEW substituted_kv_arg AS
    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.fncall_id,
        a.key,
        b.sql
    FROM fn_kv_arg AS a
    JOIN substituted_expr AS b
        ON a.pipeline_id = b.pipeline_id
        AND a.rule_id = b.rule_id
        AND a.expr_id = b.expr_id
        AND a.expr_type = b.expr_type;

/*
# all aggregate functions are mapped here
# because arguments that they accept vary a lot
substituted_fncall_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated: true) <-
    fncall_expr_matching_signature(
        pipeline_id:, rule_id:, expr_id:, fn_name: "count",
        val_arg_count: 0, kv_arg_keys: [], aggregated: true)
    sql := "COUNT(*)"
substituted_fncall_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated:) <-
    fncall_expr_matching_signature(
        pipeline_id:, rule_id:, expr_id:, sql_name:, fncall_id:,
        val_arg_count: 1, kv_arg_keys: [], aggregated:)
    substituted_val_arg(
        pipeline_id:, rule_id:, fncall_id:,
        arg_index: 0, sql: arg_sql)
    sql := `{{sql_name}}({{arg_sql}})`
substituted_fncall_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated:) <-
    fncall_expr_matching_signature(
        pipeline_id:, rule_id:, expr_id:, fn_name: "is_NULL", sql_name: NULL, fncall_id:,
        val_arg_count: 1, kv_arg_keys: [], aggregated:)
    substituted_val_arg(
        pipeline_id:, rule_id:, fncall_id:,
        arg_index: 0, sql: arg_sql)
    sql := `({{arg_sql}} IS NULL)`
substituted_fncall_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated: true) <-
    fncall_expr_matching_signature(
        pipeline_id:, rule_id:, expr_id:, fn_name:, fncall_id:, sql_name:,
        val_arg_count: 1, kv_arg_keys: ["by"], aggregated: true)
    fn_name in ["argmin", "argmax"]
    substituted_val_arg(
        pipeline_id:, rule_id:, fncall_id:,
        arg_index: 0, sql: arg_sql)
    substituted_kv_arg(
        pipeline_id:, rule_id:, fncall_id:,
        key: "by", sql: by_sql)
    sql := `{{sql_name}}({{arg_sql}}, {{by_sql}})`
*/
DECLARE RECURSIVE VIEW substituted_fncall_expr (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, sql TEXT, aggregated BOOLEAN);
CREATE MATERIALIZED VIEW substituted_fncall_expr AS
    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        'COUNT(*)' AS sql,
        a.aggregated
    FROM fncall_expr_matching_signature AS a
    WHERE a.fn_name = 'count'
    AND a.val_arg_count = 0
    AND ARRAY_SIZE(a.kv_arg_keys) = 0
    AND a.aggregated
    
    UNION

    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        (a.sql_name || '(' || c.sql || ')') AS sql,
        a.aggregated
    FROM fncall_expr_matching_signature AS a
    JOIN substituted_val_arg AS c
        ON a.pipeline_id = c.pipeline_id
        AND a.rule_id = c.rule_id
        AND a.fncall_id = c.fncall_id
        AND c.arg_index = 0
    WHERE a.val_arg_count = 1
    AND a.sql_name IS NOT NULL
    AND ARRAY_SIZE(a.kv_arg_keys) = 0

    UNION

    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        ('(' || c.sql || ' IS NULL)') AS sql,
        a.aggregated
    FROM fncall_expr_matching_signature AS a
    JOIN substituted_val_arg AS c
        ON a.pipeline_id = c.pipeline_id
        AND a.rule_id = c.rule_id
        AND a.fncall_id = c.fncall_id
        AND c.arg_index = 0
    WHERE a.fn_name = 'is_NULL'
    AND a.sql_name IS NULL
    AND a.val_arg_count = 1
    AND ARRAY_SIZE(a.kv_arg_keys) = 0

    UNION

    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        (a.sql_name || '(' || c.sql || ', ' || d.sql || ')') AS sql,
        a.aggregated
    FROM fncall_expr_matching_signature AS a
    JOIN substituted_val_arg AS c
        ON a.pipeline_id = c.pipeline_id
        AND a.rule_id = c.rule_id
        AND a.fncall_id = c.fncall_id
        AND c.arg_index = 0
    JOIN substituted_kv_arg AS d
        ON a.pipeline_id = d.pipeline_id
        AND a.rule_id = d.rule_id
        AND a.fncall_id = d.fncall_id
        AND d.key = 'by'
    WHERE a.fn_name in ('argmin', 'argmax')
    AND a.aggregated
    AND a.val_arg_count = 1
    AND ARRAY_SIZE(a.kv_arg_keys) = 1;

/*
substituted_array_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated: some<aggregated>) <-
    array_expr(pipeline_id:, rule_id:, expr_id:, array_id:)
    array_entry(
        pipeline_id:, rule_id:, array_id:, index:,
        expr_id: element_expr_id, expr_type: element_expr_type)
    substituted_expr(
        pipeline_id:, rule_id:, expr_id: element_expr_id, expr_type: element_expr_type,
        sql: element_sql, aggregated:)
    sql := "ARRAY[" ++ join(array<element_sql, order_by: [index]>, ", ") ++ "]"
*/
DECLARE RECURSIVE VIEW substituted_array_expr (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, sql TEXT, aggregated BOOLEAN);
CREATE MATERIALIZED VIEW substituted_array_expr AS
    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        ('ARRAY[' || ARRAY_TO_STRING(ARRAY_AGG(c.sql ORDER BY b."index"), ', ') || ']') AS sql,
        SOME(c.aggregated) AS aggregated
    FROM array_expr AS a
    JOIN array_entry AS b
        ON a.pipeline_id = b.pipeline_id
        AND a.rule_id = b.rule_id
        AND a.array_id = b.array_id
    JOIN substituted_expr AS c
        ON b.pipeline_id = c.pipeline_id
        AND b.rule_id = c.rule_id
        AND b.expr_id = c.expr_id
        AND b.expr_type = c.expr_type
    GROUP BY a.pipeline_id, a.rule_id, a.expr_id;

/*
substituted_dict_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated: some<aggregated>) <-
    dict_expr(pipeline_id:, rule_id:, expr_id:, dict_id:)
    dict_entry(pipeline_id:, rule_id:, dict_id:, key:, expr_id: value_expr_id, expr_type: value_expr_type)
    substituted_expr(pipeline_id:, rule_id:, expr_id: value_expr_id, expr_type: value_expr_type, sql: value_sql, aggregated:)
    sql := "MAP[" ++ join(array<`'{{key}}', {{value_sql}}`>, ", ") ++ "]"
*/
DECLARE RECURSIVE VIEW substituted_dict_expr (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, sql TEXT, aggregated BOOLEAN);
CREATE MATERIALIZED VIEW substituted_dict_expr AS
    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        ('MAP[' || ARRAY_TO_STRING(ARRAY_AGG('''' || b.key || '''' || ', ' || c.sql), ', ') || ']') AS sql,
        SOME(c.aggregated) AS aggregated
    FROM dict_expr AS a
    JOIN dict_entry AS b
        ON a.pipeline_id = b.pipeline_id
        AND a.rule_id = b.rule_id
        AND a.dict_id = b.dict_id
    JOIN substituted_expr AS c
        ON b.pipeline_id = c.pipeline_id
        AND b.rule_id = c.rule_id
        AND b.expr_id = c.expr_id
        AND b.expr_type = c.expr_type
    GROUP BY a.pipeline_id, a.rule_id, a.expr_id;

/*
binop_sql(op: "is", sql: "IS")
binop_sql(op: ">=", sql: ">=")
binop_sql(op: ">", sql: ">")
binop_sql(op: "<=", sql: "<=")
binop_sql(op: "<", sql: "<")
binop_sql(op: "=", sql: "=")
binop_sql(op: "!=", sql: "!=")
*/
CREATE MATERIALIZED VIEW binop_sql AS
    SELECT DISTINCT
        b.op,
        b.sql
    FROM (VALUES
        ('is', 'IS'),
        ('>=', '>='),
        ('>', '>'),
        ('<=', '<='),
        ('<', '<'),
        ('=', '='),
        ('!=', '!=')
    ) AS b (op, sql);

/*
substituted_binop_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated:) <-
    binop_expr(
        pipeline_id:, rule_id:, expr_id:, op:,
        left_expr_id: left_expr_id, left_expr_type: left_expr_type,
        right_expr_id: right_expr_id, right_expr_type: right_expr_type)
    substituted_expr(
        pipeline_id:, rule_id:, expr_id: left_expr_id, expr_type: left_expr_type,
        sql: left_sql, aggregated: left_aggregated)
    substituted_expr(
        pipeline_id:, rule_id:, expr_id: right_expr_id, expr_type: right_expr_type,
        sql: right_sql, aggregated: right_aggregated)
    binop_sql(op:, sql: op_sql)
    sql := `({{left_sql}} {{op_sql}} {{right_sql}})`
    aggregated := left_aggregated or right_aggregated
*/
DECLARE RECURSIVE VIEW substituted_binop_expr (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, sql TEXT, aggregated BOOLEAN);
CREATE MATERIALIZED VIEW substituted_binop_expr AS
    SELECT DISTINCT
        b.pipeline_id,
        b.rule_id,
        b.expr_id,
        ('(' || a.sql || ' ' || d.sql || ' ' || c.sql || ')') AS sql,
        (a.aggregated OR c.aggregated) AS aggregated
    FROM binop_expr AS b
    JOIN substituted_expr AS a
        ON b.pipeline_id = a.pipeline_id
        AND b.rule_id = a.rule_id
        AND b.left_expr_id = a.expr_id
        AND b.left_expr_type = a.expr_type
    JOIN substituted_expr AS c
        ON b.pipeline_id = c.pipeline_id
        AND b.rule_id = c.rule_id
        AND b.right_expr_id = c.expr_id
        AND b.right_expr_type = c.expr_type
    JOIN binop_sql AS d
        ON b.op = d.op;

/*
#substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "sql_expr", sql:, aggregated: false) <-
#    substituted_sql_expr(pipeline_id:, rule_id:, expr_id:, sql:)
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "int_expr", sql:, aggregated: false) <-
    int_expr(pipeline_id:, rule_id:, expr_id:, value:)
    sql := string(value)
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "str_expr", sql:, aggregated: false) <-
    str_expr(pipeline_id:, rule_id:, expr_id:, value:)
    sql := `'{{value}}'`
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "str_template_expr", sql:, aggregated:) <-
    sustituted_str_template_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated:)
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "null_expr", sql:, aggregated: false) <-
    null_expr(pipeline_id:, rule_id:, expr_id:)
    sql := 'NULL'
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "bool_expr", sql:, aggregated: false) <-
    bool_expr(pipeline_id:, rule_id:, expr_id:, value:)
    sql := `{{value}}
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "binop_expr", sql:, aggregated:) <-
    substituted_binop_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated:)
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "var_expr", sql:, aggregated:) <-
    var_expr(pipeline_id:, rule_id:, expr_id:, var_name:)
    canonical_var_bound_sql(pipeline_id:, rule_id:, var_name:, sql:, aggregated:)
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "fncall_expr", sql:, aggregated: true) <-
    substituted_fncall_expr(pipeline_id:, rule_id:, expr_id:, sql:)
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "array_expr", sql:, aggregated:) <-
    substituted_array_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated:)
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "dict_expr", sql:, aggregated:) <-
    substituted_dict_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated:)
*/
CREATE MATERIALIZED VIEW substituted_expr AS
    --SELECT a.pipeline_id, a.rule_id, a.expr_id, CAST('sql_expr' AS TEXT) AS expr_type, a.sql, false AS aggregated
    --FROM substituted_sql_expr AS a
    
    --UNION
    
    SELECT b.pipeline_id, b.rule_id, b.expr_id, CAST('int_expr' AS TEXT) AS expr_type, b.value AS sql, false AS aggregated
    FROM int_expr AS b

    UNION

    SELECT c.pipeline_id, c.rule_id, c.expr_id, 'str_expr' AS expr_type, ('''' || c.value || '''') AS sql, false AS aggregated
    FROM str_expr AS c

    UNION

    SELECT d.pipeline_id, d.rule_id, d.expr_id, 'str_template_expr' AS expr_type, d.sql, d.aggregated
    FROM substituted_str_template_expr AS d

    UNION

    SELECT d.pipeline_id, d.rule_id, d.expr_id, 'null_expr' AS expr_type, 'NULL' AS sql, false AS aggregated
    FROM null_expr AS d

    UNION

    SELECT d.pipeline_id, d.rule_id, d.expr_id, 'bool_expr' AS expr_type, CAST(d.value AS TEXT) AS sql, false AS aggregated
    FROM bool_expr AS d

    UNION

    SELECT e.pipeline_id, e.rule_id, e.expr_id, 'binop_expr' AS expr_type, e.sql, e.aggregated
    FROM substituted_binop_expr AS e

    UNION

    SELECT d.pipeline_id, d.rule_id, d.expr_id, 'var_expr' AS expr_type, canonical_var_bound_sql.sql, canonical_var_bound_sql.aggregated
    FROM var_expr AS d
    JOIN canonical_var_bound_sql
        ON d.pipeline_id = canonical_var_bound_sql.pipeline_id
        AND d.rule_id = canonical_var_bound_sql.rule_id
        AND d.var_name = canonical_var_bound_sql.var_name

    UNION

    SELECT e.pipeline_id, e.rule_id, e.expr_id, 'fncall_expr' AS expr_type, e.sql, true AS aggregated
    FROM substituted_fncall_expr AS e

    UNION

    SELECT f.pipeline_id, f.rule_id, f.expr_id, 'array_expr' AS expr_type, f.sql, f.aggregated
    FROM substituted_array_expr AS f

    UNION

    SELECT g.pipeline_id, g.rule_id, g.expr_id, 'dict_expr' AS expr_type, g.sql, g.aggregated
    FROM substituted_dict_expr AS g;






/*
match_right_expr_sql(pipeline_id:, rule_id:, match_id:, sql:, aggregated:) <-
    body_match(pipeline_id:, rule_id:, match_id:, right_expr_id:, right_expr_type:)
    substituted_expr(pipeline_id:, rule_id:, expr_id: right_expr_id, expr_type: right_expr_type, sql:, aggregated:)
*/
DECLARE RECURSIVE VIEW match_right_expr_sql (pipeline_id TEXT, rule_id TEXT, match_id TEXT, sql TEXT, aggregated BOOLEAN);
CREATE MATERIALIZED VIEW match_right_expr_sql AS
    SELECT DISTINCT a.pipeline_id, a.rule_id, a.match_id, b.sql, b.aggregated
    FROM body_match AS a
    JOIN substituted_expr AS b
        ON a.pipeline_id = b.pipeline_id
        AND a.rule_id = b.rule_id
        AND a.right_expr_id = b.expr_id
        AND a.right_expr_type = b.expr_type;


/*
# This looks really complicated, because I try to cover all kinds of pattern matching cases
# and trying to flatten GRASP_VARIANT_ARRAY_DROP_SIDES calls at the same time.
#
# Maybe I should try to generate some intermediary output expressions in a straightforward manner,
# and then simplify them, if necessary. Sounds reasonable.
#
# This way all parts will be simple and easy to follow.
*/

/*
# oexpr is output expr.
#
# The idea, is to construct nested GRASP_VARIANT_ARRAY_DROP_SIDES calls
# And then flatten them afterwards, without creating extra clauses.
*/








/*
# if right side is var not bound with asterisk
# we compute sql as is, no need to do anything special for pattern matching
#
# any-expr() := var
match_oexpr(
    pipeline_id:, rule_id:, pattern_expr_id:, pattern_expr_type:, match_id:, sql:, aggregated:,
) <-
    # left expr is a var that was never bound with asterisk
    body_match(
        pipeline_id:, rule_id:, match_id:,
        left_expr_id: pattern_expr_id, left_expr_type: pattern_expr_type,
        right_expr_id:, right_expr_type: "var_expr")
    var_expr(pipeline_id:, rule_id:, expr_id: right_expr_id, var_name:)
    not asterisk_array_var_offsets(pipeline_id:, rule_id:, var_name:)
    substituted_expr(
        pipeline_id:, rule_id:, sql:, aggregated:,
        expr_id: right_expr_id, expr_type: "var_expr")
*/
DECLARE RECURSIVE VIEW match_oexpr (pipeline_id TEXT, rule_id TEXT, pattern_expr_id TEXT, pattern_expr_type TEXT, match_id TEXT, sql TEXT, aggregated BOOLEAN);
DECLARE RECURSIVE VIEW match_oexpr_array_drop_sides (pipeline_id TEXT, rule_id TEXT, pattern_expr_id TEXT, pattern_expr_type TEXT, match_id TEXT, aggregated BOOLEAN, array_sql TEXT, left_offset INTEGER, right_offset INTEGER);
CREATE MATERIALIZED VIEW match_oexpr AS
    SELECT DISTINCT
        body_match.pipeline_id,
        body_match.rule_id,
        body_match.left_expr_id AS pattern_expr_id,
        body_match.left_expr_type AS pattern_expr_type,
        body_match.match_id,
        substituted_expr.sql,
        substituted_expr.aggregated
    FROM body_match
    JOIN var_expr
        ON body_match.pipeline_id = var_expr.pipeline_id
        AND body_match.rule_id = var_expr.rule_id
        AND body_match.right_expr_id = var_expr.expr_id
    JOIN substituted_expr
        ON body_match.pipeline_id = substituted_expr.pipeline_id
        AND body_match.rule_id = substituted_expr.rule_id
        AND body_match.right_expr_id = substituted_expr.expr_id
    WHERE body_match.right_expr_type = 'var_expr'
    AND substituted_expr.expr_type = 'var_expr'
    AND NOT EXISTS (
        SELECT 1
        FROM asterisk_array_var_offsets
        WHERE body_match.pipeline_id = asterisk_array_var_offsets.pipeline_id
        AND body_match.rule_id = asterisk_array_var_offsets.rule_id
        AND var_expr.var_name = asterisk_array_var_offsets.var_name
    )
    UNION
/*
# any-expr() := not-var()
match_oexpr(
    pipeline_id:, rule_id:, pattern_expr_id:, pattern_expr_type:, match_id:, sql:, aggregated:,
) <-
    # left expr is a var that was never bound with asterisk
    body_match(
        pipeline_id:, rule_id:, match_id:,
        left_expr_id: pattern_expr_id, left_expr_type: pattern_expr_type,
        right_expr_id:, right_expr_type:)
    right_expr_type != "var_expr"
    substituted_expr(
        pipeline_id:, rule_id:, sql:, aggregated:,
        expr_id: right_expr_id, expr_type: right_expr_type)
*/
    SELECT DISTINCT
        body_match.pipeline_id,
        body_match.rule_id,
        body_match.left_expr_id AS pattern_expr_id,
        body_match.left_expr_type AS pattern_expr_type,
        body_match.match_id,
        substituted_expr.sql,
        substituted_expr.aggregated
    FROM body_match
    JOIN substituted_expr
        ON body_match.pipeline_id = substituted_expr.pipeline_id
        AND body_match.rule_id = substituted_expr.rule_id
        AND body_match.right_expr_id = substituted_expr.expr_id
        AND body_match.right_expr_type = substituted_expr.expr_type
    WHERE body_match.right_expr_type != 'var_expr'
    UNION
/*
# matching nested array pattern vars without asterisks in left side
match_oexpr(
    pipeline_id:, rule_id:, pattern_expr_id:, pattern_expr_type:, match_id:, sql:, aggregated:,
) <-
    # left expr is not a var, but not an array as well
    match_oexpr(
        pipeline_id:, rule_id:, pattern_expr_id: array_expr_id, pattern_expr_type: "array_expr",
        match_id:, sql: array_expr_sql, aggregated:)
    array_expr(pipeline_id:, rule_id:, expr_id: array_expr_id, array_id:)
    array_entry(
        pipeline_id:, rule_id:, array_id:, index:,
        expr_id: pattern_expr_id, expr_type: pattern_expr_type)
    not var_expr(pipeline_id:, rule_id:, expr_id: pattern_expr_id, special_prefix: "*")
    sql := `{{array_expr_sql}}[{{index}}]`
*/
    SELECT DISTINCT
        match_oexpr.pipeline_id,
        match_oexpr.rule_id,
        array_entry.expr_id AS pattern_expr_id,
        array_entry.expr_type AS pattern_expr_type,
        match_oexpr.match_id,
        (match_oexpr.sql || '[' || array_entry."index" || ']') AS sql,
        match_oexpr.aggregated
    FROM match_oexpr
    JOIN array_expr
        ON match_oexpr.pipeline_id = array_expr.pipeline_id
        AND match_oexpr.rule_id = array_expr.rule_id
        AND match_oexpr.pattern_expr_id = array_expr.expr_id
    JOIN array_entry
        ON match_oexpr.pipeline_id = array_entry.pipeline_id
        AND match_oexpr.rule_id = array_entry.rule_id
        AND array_expr.array_id = array_entry.array_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM var_expr
        WHERE match_oexpr.pipeline_id = var_expr.pipeline_id
        AND match_oexpr.rule_id = var_expr.rule_id
        AND array_entry.expr_id = var_expr.expr_id
        AND var_expr.special_prefix = '*'
    )
    UNION
/*
# matching nested dict pattern vars without asterisks in left side
match_oexpr(
    pipeline_id:, rule_id:, pattern_expr_id:, pattern_expr_type:, match_id:, sql:, aggregated:,
) <-
    # left expr is not a var, but not a dict as well
    match_oexpr(
        pipeline_id:, rule_id:, pattern_expr_id: dict_expr_id, pattern_expr_type: "dict_expr",
        match_id:, sql: dict_expr_sql, aggregated:)
    dict_expr(pipeline_id:, rule_id:, expr_id: dict_expr_id, dict_id:)
    dict_entry(
        pipeline_id:, rule_id:, dict_id:, key:,
        expr_id: pattern_expr_id, expr_type: pattern_expr_type)
    sql := `{{dict_expr_sql}}['{{key}}']`
*/
    SELECT DISTINCT
        match_oexpr.pipeline_id,
        match_oexpr.rule_id,
        dict_entry.expr_id AS pattern_expr_id,
        dict_entry.expr_type AS pattern_expr_type,
        match_oexpr.match_id,
        (match_oexpr.sql || '[' || '''' || dict_entry.key || '''' || ']') AS sql,
        match_oexpr.aggregated
    FROM match_oexpr
    JOIN dict_expr
        ON match_oexpr.pipeline_id = dict_expr.pipeline_id
        AND match_oexpr.rule_id = dict_expr.rule_id
        AND match_oexpr.pattern_expr_id = dict_expr.expr_id
    JOIN dict_entry
        ON match_oexpr.pipeline_id = dict_entry.pipeline_id
        AND match_oexpr.rule_id = dict_entry.rule_id
        AND dict_expr.dict_id = dict_entry.dict_id
    WHERE match_oexpr.pattern_expr_type = 'dict_expr'
    UNION
/*
# matching nested array pattern associated with asterisk expression,
# shift indexes accordingly
match_oexpr(
    pipeline_id:, rule_id:, pattern_expr_id:, pattern_expr_type:, match_id:, sql:, aggregated:,
) <-
    match_oexpr_array_drop_sides(
        pipeline_id:, rule_id:, pattern_expr_id: array_expr_id, pattern_expr_type: "array_expr",
        match_id:, aggregated:,
        array_sql:, left_offset:)
    array_expr(pipeline_id:, rule_id:, expr_id: array_expr_id, array_id:)
    array_entry(
        pipeline_id:, rule_id:, array_id:, index: inner_index,
        expr_id: pattern_expr_id, expr_type: pattern_expr_type)
    not var_expr(pipeline_id:, rule_id:, expr_id: pattern_expr_id, special_prefix: "*")
    index := inner_index + left_offset
    sql := `{{array_expr_sql}}[{{index}}]`
*/
    SELECT DISTINCT
        match_oexpr_array_drop_sides.pipeline_id,
        match_oexpr_array_drop_sides.rule_id,
        array_entry.expr_id AS pattern_expr_id,
        array_entry.expr_type AS pattern_expr_type,
        match_oexpr_array_drop_sides.match_id,
        (match_oexpr_array_drop_sides.array_sql || '[' || (array_entry."index" + match_oexpr_array_drop_sides.left_offset) || ']') AS sql,
        match_oexpr_array_drop_sides.aggregated
    FROM match_oexpr_array_drop_sides
    JOIN array_expr
        ON match_oexpr_array_drop_sides.pipeline_id = array_expr.pipeline_id
        AND match_oexpr_array_drop_sides.rule_id = array_expr.rule_id
        AND match_oexpr_array_drop_sides.pattern_expr_id = array_expr.expr_id
    JOIN array_entry
        ON match_oexpr_array_drop_sides.pipeline_id = array_entry.pipeline_id
        AND match_oexpr_array_drop_sides.rule_id = array_entry.rule_id
        AND array_expr.array_id = array_entry.array_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM var_expr
        WHERE match_oexpr_array_drop_sides.pipeline_id = var_expr.pipeline_id
        AND match_oexpr_array_drop_sides.rule_id = var_expr.rule_id
        AND array_entry.expr_id = var_expr.expr_id
        AND var_expr.special_prefix = '*'
    )
    AND match_oexpr_array_drop_sides.pattern_expr_type = 'array_expr';




/*
# fact(a: [1, *right, 3])
# left := right
match_oexpr_array_drop_sides(
    pipeline_id:, rule_id:, pattern_expr_id:, pattern_expr_type:,
    match_id:, aggregated: false,
    array_sql:, left_offset:, right_offset:
) <-
    body_match(
        pipeline_id:, rule_id:, match_id:,
        left_expr_id: pattern_expr_id, left_expr_type: pattern_expr_type,
        right_expr_id:, right_expr_type: "var_expr")
    var_expr(pipeline_id:, rule_id:, expr_id: right_expr_id, var_name: right_var_name)
    fact_oexpr_array_drop_sides(
        pipeline_id:, rule_id:, var_name: right_var_name,
        array_sql:, left_offset:, right_offset:)
*/
CREATE MATERIALIZED VIEW match_oexpr_array_drop_sides AS
    SELECT DISTINCT 
        body_match.pipeline_id,
        body_match.rule_id,
        body_match.left_expr_id AS pattern_expr_id,
        body_match.left_expr_type AS pattern_expr_type,
        body_match.match_id,
        FALSE AS aggregated,
        fact_oexpr_array_drop_sides.array_sql,
        fact_oexpr_array_drop_sides.left_offset,
        fact_oexpr_array_drop_sides.right_offset
    FROM body_match
    JOIN var_expr AS right_var_expr
        ON body_match.pipeline_id = right_var_expr.pipeline_id
        AND body_match.rule_id = right_var_expr.rule_id
        AND body_match.right_expr_id = right_var_expr.expr_id
    JOIN fact_oexpr_array_drop_sides
        ON body_match.pipeline_id = fact_oexpr_array_drop_sides.pipeline_id
        AND body_match.rule_id = fact_oexpr_array_drop_sides.rule_id
        AND right_var_expr.var_name = fact_oexpr_array_drop_sides.var_name
    WHERE body_match.right_expr_type = 'var_expr'
    UNION
/*
# [1, *right, 3] := any_expr
# left := right
match_oexpr_array_drop_sides(
    pipeline_id:, rule_id:, pattern_expr_id:, pattern_expr_type:,
    match_id:, aggregated:,
    array_sql:, left_offset:, right_offset:
) <-
    body_match(
        pipeline_id:, rule_id:, match_id:,
        left_expr_id: pattern_expr_id, left_expr_type: pattern_expr_type,
        right_expr_id:, right_expr_type: "var_expr")
    var_expr(pipeline_id:, rule_id:, expr_id: right_expr_type, var_name: right_var_name)
    var_expr(pipeline_id:, rule_id:, expr_id: var_expr_id, var_name: right_var_name)
    match_oexpr_array_drop_sides(
        pipeline_id:, rule_id:, expr_id: var_expr_id, aggregated:,
        array_sql:, left_offset:, right_offset:)
*/
    SELECT DISTINCT
        body_match.pipeline_id,
        body_match.rule_id,
        body_match.left_expr_id AS pattern_expr_id,
        body_match.left_expr_type AS pattern_expr_type,
        match_oexpr_array_drop_sides.match_id,
        match_oexpr_array_drop_sides.aggregated,
        match_oexpr_array_drop_sides.array_sql,
        match_oexpr_array_drop_sides.left_offset,
        match_oexpr_array_drop_sides.right_offset
    FROM body_match
    JOIN var_expr AS right_var_expr
        ON body_match.pipeline_id = right_var_expr.pipeline_id
        AND body_match.rule_id = right_var_expr.rule_id
        AND body_match.right_expr_id = right_var_expr.expr_id
    JOIN var_expr AS prev_bound_var_expr
        ON body_match.pipeline_id = prev_bound_var_expr.pipeline_id
        AND body_match.rule_id = prev_bound_var_expr.rule_id
        AND right_var_expr.var_name = prev_bound_var_expr.var_name
    JOIN match_oexpr_array_drop_sides
        ON body_match.pipeline_id = match_oexpr_array_drop_sides.pipeline_id
        AND body_match.rule_id = match_oexpr_array_drop_sides.rule_id
        AND prev_bound_var_expr.expr_id = match_oexpr_array_drop_sides.pattern_expr_id
    WHERE body_match.right_expr_type = 'var_expr'
    AND match_oexpr_array_drop_sides.pattern_expr_type = 'var_expr'
    UNION
/*
# matching nested asterisk array pattern vars in left side
match_oexpr_array_drop_sides(
    pipeline_id:, rule_id:, pattern_expr_id:, pattern_expr_type: "var_expr",
    match_id:, aggregated:,
    array_sql:, left_offset:, right_offset:
) <-
    match_oexpr_array_drop_sides(
        pipeline_id:, rule_id:, pattern_expr_id: parent_pattern_expr_id, pattern_expr_type: "array_expr",
        match_id:, aggregated:,
        array_sql:, left_offset: parent_left_offset, right_offset: parent_left_offset)
    array_expr(pipeline_id:, rule_id:, expr_id: parent_pattern_expr_id, array_id:)
    asterisk_array_var_offsets(
        pipeline_id:, rule_id:, array_id:, var_name:,
        expr_id: pattern_expr_id, left_offset: innert_left_offset, right_offset: innert_right_offset)
    left_offset := parent_left_offset + innert_left_offset
    right_offset := parent_left_offset + innert_right_offset - 1
*/
    SELECT DISTINCT
        match_oexpr_array_drop_sides.pipeline_id,
        match_oexpr_array_drop_sides.rule_id,
        asterisk_array_var_offsets.expr_id AS pattern_expr_id,
        'var_expr' AS pattern_expr_type,
        match_oexpr_array_drop_sides.match_id,
        match_oexpr_array_drop_sides.aggregated,
        match_oexpr_array_drop_sides.array_sql,
        (match_oexpr_array_drop_sides.left_offset + asterisk_array_var_offsets.left_offset) AS left_offset,
        (match_oexpr_array_drop_sides.left_offset + asterisk_array_var_offsets.right_offset - 1) AS right_offset
    FROM match_oexpr_array_drop_sides
    JOIN array_expr
        ON match_oexpr_array_drop_sides.pipeline_id = array_expr.pipeline_id
        AND match_oexpr_array_drop_sides.rule_id = array_expr.rule_id
        AND match_oexpr_array_drop_sides.pattern_expr_id = array_expr.expr_id
    JOIN asterisk_array_var_offsets
        ON match_oexpr_array_drop_sides.pipeline_id = asterisk_array_var_offsets.pipeline_id
        AND match_oexpr_array_drop_sides.rule_id = asterisk_array_var_offsets.rule_id
        AND array_expr.array_id = asterisk_array_var_offsets.array_id
    WHERE match_oexpr_array_drop_sides.pattern_expr_type = 'array_expr'
    UNION
/*
# matching nested asterisk array pattern vars in left side, with non-asterisk expression
match_oexpr_array_drop_sides(
    pipeline_id:, rule_id:, pattern_expr_id:, pattern_expr_type: "var_expr",
    match_id:, aggregated:,
    array_sql:, left_offset:, right_offset:
) <-
    match_oexpr(
        pipeline_id:, rule_id:,
        pattern_expr_id: parent_pattern_expr_id, pattern_expr_type: "array_expr",
        match_id:, sql: array_sql, aggregated:)
    array_expr(pipeline_id:, rule_id:, expr_id: parent_pattern_expr_id, array_id:)
    asterisk_array_var_offsets(
        pipeline_id:, rule_id:, array_id:, var_name:,
        expr_id: pattern_expr_id, left_offset:, right_offset:)
*/
    SELECT DISTINCT
        match_oexpr.pipeline_id,
        match_oexpr.rule_id,
        asterisk_array_var_offsets.expr_id AS pattern_expr_id,
        'var_expr' AS pattern_expr_type,
        match_oexpr.match_id,
        match_oexpr.aggregated,
        match_oexpr.sql AS array_sql,
        asterisk_array_var_offsets.left_offset,
        asterisk_array_var_offsets.right_offset
    FROM match_oexpr
    JOIN array_expr
        ON match_oexpr.pipeline_id = array_expr.pipeline_id
        AND match_oexpr.rule_id = array_expr.rule_id
        AND match_oexpr.pattern_expr_id = array_expr.expr_id
    JOIN asterisk_array_var_offsets
        ON match_oexpr.pipeline_id = asterisk_array_var_offsets.pipeline_id
        AND match_oexpr.rule_id = asterisk_array_var_offsets.rule_id
        AND array_expr.array_id = asterisk_array_var_offsets.array_id
    WHERE match_oexpr.pattern_expr_type = 'array_expr';



/*
var_bound_via_match(pipeline_id:, rule_id:, match_id:, var_name:, sql:, aggregated:) <-
    match_oexpr(
        pipeline_id:, rule_id:,
        pattern_expr_id:, pattern_expr_type: "var_expr",
        match_id:, sql:, aggregated:)
    var_expr(pipeline_id:, rule_id:, expr_id: pattern_expr_id, var_name:)
var_bound_via_match(pipeline_id:, rule_id:, match_id:, var_name:, sql:, aggregated:) <-
    match_oexpr_array_drop_sides(
        pipeline_id:, rule_id:, pattern_expr_type: "var_expr",
        match_id:, aggregated:,
        array_sql:, left_offset:, right_offset:)
    var_expr(pipeline_id:, rule_id:, expr_id: pattern_expr_id, var_name:)
    sql := `GRASP_VARIANT_ARRAY_DROP_SIDES(CAST({{array_sql}} AS VARIANT ARRAY), CAST({{left_offset}} AS INTEGER UNSIGNED), CAST({{right_offset}} AS INTEGER UNSIGNED))`
*/
CREATE MATERIALIZED VIEW var_bound_via_match AS
    SELECT DISTINCT
        match_oexpr.pipeline_id,
        match_oexpr.rule_id,
        match_oexpr.match_id,
        var_expr.var_name,
        match_oexpr.sql,
        match_oexpr.aggregated
    FROM match_oexpr
    JOIN var_expr
        ON match_oexpr.pipeline_id = var_expr.pipeline_id
        AND match_oexpr.rule_id = var_expr.rule_id
        AND match_oexpr.pattern_expr_id = var_expr.expr_id
    WHERE match_oexpr.pattern_expr_type = 'var_expr'

    UNION

    SELECT DISTINCT
        match_oexpr_array_drop_sides.pipeline_id,
        match_oexpr_array_drop_sides.rule_id,
        match_oexpr_array_drop_sides.match_id,
        var_expr.var_name,
        ('GRASP_VARIANT_ARRAY_DROP_SIDES(CAST(' || match_oexpr_array_drop_sides.array_sql || 'AS VARIANT ARRAY), CAST(' || match_oexpr_array_drop_sides.left_offset || ' AS INTEGER UNSIGNED), CAST(' || match_oexpr_array_drop_sides.right_offset || ' AS INTEGER UNSIGNED))') AS sql,
        match_oexpr_array_drop_sides.aggregated
    FROM match_oexpr_array_drop_sides
    JOIN var_expr
        ON match_oexpr_array_drop_sides.pipeline_id = var_expr.pipeline_id
        AND match_oexpr_array_drop_sides.rule_id = var_expr.rule_id
        AND match_oexpr_array_drop_sides.pattern_expr_id = var_expr.expr_id
    WHERE match_oexpr_array_drop_sides.pattern_expr_type = 'var_expr';



/*
sql_where_cond(pipeline_id:, rule_id:, cond_id:, sql:) <-
    body_sql_cond(pipeline_id:, rule_id:, cond_id:, sql_expr_id: expr_id)
    substituted_sql_expr(pipeline_id:, rule_id:, expr_id:, sql:)
*/
-- CREATE MATERIALIZED VIEW sql_where_cond AS
--     SELECT DISTINCT
--         body_sql_cond.pipeline_id,
--         body_sql_cond.rule_id,
--         body_sql_cond.cond_id,
--         substituted_sql_expr.sql
--     FROM body_sql_cond
--     JOIN substituted_sql_expr
--         ON body_sql_cond.pipeline_id = substituted_sql_expr.pipeline_id
--         AND body_sql_cond.rule_id = substituted_sql_expr.rule_id
--         AND body_sql_cond.sql_expr_id = substituted_sql_expr.expr_id;

/*
output_json_type_cast_to(expr_type: "int_expr", output_type: "DECIMAL")
output_json_type_cast_to(expr_type: "str_expr", output_type: "TEXT")
output_json_type_cast_to(expr_type: "bool_expr", output_type: "BOOLEAN")
*/
CREATE MATERIALIZED VIEW output_json_type_cast_to AS
    SELECT 'int_expr' AS expr_type, 'DECIMAL' AS output_type
    UNION
    SELECT 'str_expr' AS expr_type, 'TEXT' AS output_type
    UNION
    SELECT 'bool_expr' AS expr_type, 'BOOLEAN' AS output_type;

/*
# just variable referenced, make sure it is not null
fact_where_cond(pipeline_id:, rule_id:, fact_id:, sql:) <-
    fact_oexpr(
        pipeline_id:, rule_id:, pattern_expr_type: "var_expr", pattern_expr_id:,
        sql: access_sql, negated: false, fact_id:)
    # we do not add check for null, if there is a prefix that allows it
    not var_expr(pipeline_id:, rule_id:, expr_id: pattern_expr_id, maybe_null_prefix: true)
    sql := `{{access_sql}} IS NOT NULL`
fact_where_cond(pipeline_id:, rule_id:, fact_id:, sql:) <-
    fact_oexpr(
        pipeline_id:, rule_id:, pattern_expr_id:, pattern_expr_type:,
        sql: access_sql, negated: false, fact_id:)
    pattern_expr_type != "var_expr"
    pattern_expr_type != "dict_expr"
    pattern_expr_type != "array_expr"
    substituted_expr(
        pipeline_id:, rule_id:, expr_id: pattern_expr_id, expr_type: pattern_expr_type,
        sql: value_sql)
    output_json_type_cast_to(expr_type: pattern_expr_type, output_type:)
    sql := `CAST({{access_sql}} AS {{output_type}}) = {{value_sql}}`
*/
CREATE MATERIALIZED VIEW fact_where_cond AS
    SELECT DISTINCT
        fact_oexpr.pipeline_id,
        fact_oexpr.rule_id,
        fact_oexpr.fact_id,
        (fact_oexpr.sql || ' IS NOT NULL') AS sql
    FROM fact_oexpr
    WHERE fact_oexpr.pattern_expr_type = 'var_expr'
    AND NOT fact_oexpr.negated
    AND NOT EXISTS (
        SELECT *
        FROM var_expr
        WHERE fact_oexpr.pipeline_id = var_expr.pipeline_id
        AND fact_oexpr.rule_id = var_expr.rule_id
        AND fact_oexpr.pattern_expr_id = var_expr.expr_id
        AND var_expr.maybe_null_prefix
    )

    UNION

    SELECT DISTINCT
        fact_oexpr.pipeline_id,
        fact_oexpr.rule_id,
        fact_oexpr.fact_id,
        ('CAST(' || fact_oexpr.sql || ' AS ' || output_json_type_cast_to.output_type || ') = ' || substituted_expr.sql) AS sql
    FROM fact_oexpr
    JOIN substituted_expr
        ON fact_oexpr.pipeline_id = substituted_expr.pipeline_id
        AND fact_oexpr.rule_id = substituted_expr.rule_id
        AND fact_oexpr.pattern_expr_id = substituted_expr.expr_id
        AND fact_oexpr.pattern_expr_type = substituted_expr.expr_type
    JOIN output_json_type_cast_to
        ON fact_oexpr.pattern_expr_type = output_json_type_cast_to.expr_type
    WHERE fact_oexpr.pattern_expr_type NOT IN ('var_expr', 'dict_expr', 'array_expr')
    AND NOT fact_oexpr.negated;

/*
match_where_cond(pipeline_id:, rule_id:, match_id:, sql:) <-
    match_oexpr(
        pipeline_id:, rule_id:, pattern_expr_id:, pattern_expr_type: "var_expr",
        match_id:, sql: access_sql)
    # all nested var bindings should check for NULL
    not body_match(
        pipeline_id:, rule_id:, match_id:,
        left_expr_id: pattern_expr_id, left_expr_type: "var_expr")
    sql := `{{access_sql}} IS NOT NULL`
match_where_cond(pipeline_id:, rule_id:, match_id:, sql:) <-
    match_oexpr(
        pipeline_id:, rule_id:, pattern_expr_id:, pattern_expr_type:,
        match_id:, sql: access_sql)
    pattern_expr_type != "var_expr"
    pattern_expr_type != "dict_expr"
    pattern_expr_type != "array_expr"
    substituted_expr(
        pipeline_id:, rule_id:, expr_id: pattern_expr_id, expr_type: pattern_expr_type,
        sql: value_sql)
    output_json_type_cast_to(expr_type: pattern_expr_type, output_type:)
    sql := `CAST({{access_sql}} AS {{output_type}}) = {{value_sql}}`
*/
CREATE MATERIALIZED VIEW match_where_cond AS
    SELECT DISTINCT
        match_oexpr.pipeline_id,
        match_oexpr.rule_id,
        match_oexpr.match_id,
        (match_oexpr.sql || ' IS NOT NULL') AS sql
    FROM match_oexpr
    WHERE match_oexpr.pattern_expr_type = 'var_expr'
    AND NOT EXISTS (
        SELECT *
        FROM body_match
        WHERE match_oexpr.pipeline_id = body_match.pipeline_id
        AND match_oexpr.rule_id = body_match.rule_id
        AND match_oexpr.match_id = body_match.match_id
        AND body_match.left_expr_id = match_oexpr.pattern_expr_id
        AND body_match.left_expr_type = 'var_expr'
    )

    UNION

    SELECT DISTINCT
        match_oexpr.pipeline_id,
        match_oexpr.rule_id,
        match_oexpr.match_id,
        ('CAST(' || match_oexpr.sql || ' AS ' || output_json_type_cast_to.output_type || ') = ' || substituted_expr.sql) AS sql
    FROM match_oexpr
    JOIN substituted_expr
        ON match_oexpr.pipeline_id = substituted_expr.pipeline_id
        AND match_oexpr.rule_id = substituted_expr.rule_id
        AND match_oexpr.pattern_expr_id = substituted_expr.expr_id
        AND match_oexpr.pattern_expr_type = substituted_expr.expr_type
    JOIN output_json_type_cast_to
        ON match_oexpr.pattern_expr_type = output_json_type_cast_to.expr_type
    WHERE match_oexpr.pattern_expr_type NOT IN ('var_expr', 'dict_expr', 'array_expr');

/*
#where_cond(pipeline_id:, rule_id:, sql:) <-
#    sql_where_cond(pipeline_id:, rule_id:, sql:)
where_cond(pipeline_id:, rule_id:, sql:) <-
    fact_where_cond(pipeline_id:, rule_id:, sql:)
where_cond(pipeline_id:, rule_id:, sql:) <-
    match_where_cond(pipeline_id:, rule_id:, sql:)
where_cond(pipeline_id:, rule_id:, sql:) <-
    body_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, sql:)
    substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, sql:, aggregated: false)
*/
CREATE MATERIALIZED VIEW where_cond AS
    -- SELECT DISTINCT sql_where_cond.pipeline_id, sql_where_cond.rule_id, sql_where_cond.sql
    -- FROM sql_where_cond
    -- UNION
    SELECT DISTINCT fact_where_cond.pipeline_id, fact_where_cond.rule_id, fact_where_cond.sql
    FROM fact_where_cond
    UNION
    SELECT DISTINCT match_where_cond.pipeline_id, match_where_cond.rule_id, match_where_cond.sql
    FROM match_where_cond
    UNION
    SELECT DISTINCT
        body_expr.pipeline_id,
        body_expr.rule_id,
        substituted_expr.sql
    FROM body_expr
    JOIN substituted_expr
        ON body_expr.pipeline_id = substituted_expr.pipeline_id
        AND body_expr.rule_id = substituted_expr.rule_id
        AND body_expr.expr_id = substituted_expr.expr_id
        AND body_expr.expr_type = substituted_expr.expr_type
    WHERE NOT substituted_expr.aggregated;

/*
neg_fact_where_cond(pipeline_id:, rule_id:, fact_id:, sql_lines:) <-
    fact_alias(pipeline_id:, rule_id:, fact_id:, alias:, table_name:, negated: true)
    output_table_name(pipeline_id:, table_name:, output_table_name:)
    fact_arg(pipeline_id:, rule_id:, fact_id:, key:, expr_id:, expr_type:)
    substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, sql: expr_sql)
    [first_cond, *rest_cond] := array<`"{{alias}}"."{{key}}" = {{expr_sql}}`>
    sql_lines := [
        "NOT EXISTS (SELECT 1",
        `    FROM "{{output_table_name}}" AS "{{alias}}"`,
        `    WHERE {{first_cond}}`,
        *map(rest_cond, x -> `        AND {{x}}`),
        "  )",
    ]
*/
CREATE MATERIALIZED VIEW neg_fact_where_cond AS
    SELECT DISTINCT
        fact_alias.pipeline_id,
        fact_alias.rule_id,
        fact_alias.fact_id,
        ARRAY_CONCAT(
            ARRAY[
                'NOT EXISTS (SELECT 1',
                ('    FROM "' || output_table_name.output_table_name || '" AS "' || fact_alias.alias || '"'),
                ('    WHERE ' || ARRAY_AGG('"' || fact_alias.alias || '"."' || fact_arg.key || '" = ' || substituted_expr.sql)[1])
            ],
            TRANSFORM(
                GRASP_TEXT_ARRAY_DROP_LEFT(
                    ARRAY_AGG('"' || fact_alias.alias || '"."' || fact_arg.key || '" = ' || substituted_expr.sql),
                    CAST(1 AS INTEGER UNSIGNED)),
                x -> '        AND ' || x),
            ARRAY['  )']
        ) AS sql_lines
    FROM fact_alias
    JOIN output_table_name
        ON fact_alias.pipeline_id = output_table_name.pipeline_id
        AND fact_alias.table_name = output_table_name.table_name
    JOIN fact_arg
        ON fact_alias.pipeline_id = fact_arg.pipeline_id
        AND fact_alias.rule_id = fact_arg.rule_id
        AND fact_alias.fact_id = fact_arg.fact_id
    JOIN substituted_expr
        ON fact_arg.pipeline_id = substituted_expr.pipeline_id
        AND fact_arg.rule_id = substituted_expr.rule_id
        AND fact_arg.expr_id = substituted_expr.expr_id
        AND fact_arg.expr_type = substituted_expr.expr_type
    WHERE fact_alias.negated
    GROUP BY fact_alias.pipeline_id, fact_alias.rule_id, fact_alias.fact_id, fact_alias.alias, output_table_name.output_table_name;

/*
neg_fact_where_cond_concatenated(pipeline_id:, rule_id:, fact_id:, sql_lines:) <-
    first_fact_alias(pipeline_id:, rule_id:, fact_id:, negated: true)
    neg_fact_where_cond(pipeline_id:, rule_id:, fact_id:, sql_lines:)
neg_fact_where_cond_concatenated(pipeline_id:, rule_id:, fact_id:, sql_lines:) <-
    neg_fact_where_cond_concatenated(
        pipeline_id:, rule_id:, fact_id: prev_fact_id, sql_lines: prev_sql_lines)
    adjacent_facts(pipeline_id:, rule_id:, prev_fact_id:, next_fact_id: fact_id)
    neg_fact_where_cond(pipeline_id:, rule_id:, fact_id:, sql_lines: next_sql_lines)
    [first_line, *rest_lines] := next_sql_lines
    sql_lines := [*prev_sql_lines, `    AND {{first_line}}`, *rest_lines]
*/
DECLARE RECURSIVE VIEW neg_fact_where_cond_concatenated (pipeline_id TEXT, rule_id TEXT, fact_id TEXT, sql_lines TEXT ARRAY);
CREATE MATERIALIZED VIEW neg_fact_where_cond_concatenated AS
    SELECT DISTINCT
        neg_fact_where_cond.pipeline_id,
        neg_fact_where_cond.rule_id,
        neg_fact_where_cond.fact_id,
        neg_fact_where_cond.sql_lines
    FROM neg_fact_where_cond

    UNION

    SELECT DISTINCT
        neg_fact_where_cond_concatenated.pipeline_id,
        neg_fact_where_cond_concatenated.rule_id,
        neg_fact_where_cond_concatenated.fact_id,
        ARRAY_CONCAT(
            neg_fact_where_cond_concatenated.sql_lines,
            ARRAY['    AND ' || neg_fact_where_cond.sql_lines[1]],
            GRASP_TEXT_ARRAY_DROP_LEFT(neg_fact_where_cond.sql_lines, CAST(1 AS INTEGER UNSIGNED))
        ) AS sql_lines
    FROM neg_fact_where_cond_concatenated
    JOIN adjacent_facts
        ON neg_fact_where_cond_concatenated.pipeline_id = adjacent_facts.pipeline_id
        AND neg_fact_where_cond_concatenated.rule_id = adjacent_facts.rule_id
        AND neg_fact_where_cond_concatenated.fact_id = adjacent_facts.prev_fact_id
    JOIN neg_fact_where_cond
        ON neg_fact_where_cond_concatenated.pipeline_id = neg_fact_where_cond.pipeline_id
        AND neg_fact_where_cond_concatenated.rule_id = neg_fact_where_cond.rule_id
        AND adjacent_facts.next_fact_id = neg_fact_where_cond.fact_id;

/*
neg_facts_where_conds_full(pipeline_id:, rule_id:, sql_lines:) <-
    last_fact_alias(pipeline_id:, rule_id:, fact_id:, negated: true)
    neg_fact_where_cond_concatenated(pipeline_id:, rule_id:, fact_id:, sql_lines:)
*/
CREATE MATERIALIZED VIEW neg_facts_where_conds_full AS
    SELECT DISTINCT
        neg_fact_where_cond_concatenated.pipeline_id,
        neg_fact_where_cond_concatenated.rule_id,
        neg_fact_where_cond_concatenated.sql_lines
    FROM neg_fact_where_cond_concatenated
    JOIN last_fact_alias
        ON neg_fact_where_cond_concatenated.pipeline_id = last_fact_alias.pipeline_id
        AND neg_fact_where_cond_concatenated.rule_id = last_fact_alias.rule_id
        AND neg_fact_where_cond_concatenated.fact_id = last_fact_alias.fact_id;

/*
full_where_cond_sql(pipeline_id:, rule_id:, sql_lines:) <-
    neg_facts_where_conds_full(pipeline_id:, rule_id:, sql_lines: [first_line, *rest_lines])
    not where_cond(pipeline_id:, rule_id:)
    sql_lines := [`  WHERE {{first_line}}`, *rest_lines]
full_where_cond_sql(pipeline_id:, rule_id:, sql_lines:) <-
    where_cond(pipeline_id:, rule_id:, sql:)
    not neg_facts_where_conds_full(pipeline_id:, rule_id:)
    [first_line, *rest_lines] := array<sql>
    sql_lines := [
        `  WHERE {{first_line}}`,
        *map((sql_line) -> `    AND {{sql_line}}`, rest_lines),
    ]
full_where_cond_sql(pipeline_id:, rule_id:, sql_lines:) <-
    neg_facts_where_conds_full(pipeline_id:, rule_id:, sql_lines: [first_line, *rest_lines])
    where_cond(pipeline_id:, rule_id:, sql:)
    cond_lines := array<sql>
    sql_lines := [
        `  WHERE {{first_line}}`,
        *rest_lines,
        *map((sql_line) -> `    AND {{sql_line}}`, cond_lines),
    ]
full_where_cond_sql(pipeline_id:, rule_id:, sql_lines:) <-
    rule(pipeline_id:, rule_id:)
    not neg_facts_where_conds_full(pipeline_id:, rule_id:)
    not where_cond(pipeline_id:, rule_id:)
    sql_lines := []
*/
CREATE MATERIALIZED VIEW full_where_cond_sql AS
    SELECT DISTINCT
        neg_facts_where_conds_full.pipeline_id,
        neg_facts_where_conds_full.rule_id,
        ARRAY_CONCAT(
            ARRAY['  WHERE ' || neg_facts_where_conds_full.sql_lines[1]],
            GRASP_TEXT_ARRAY_DROP_LEFT(neg_facts_where_conds_full.sql_lines, CAST(1 AS INTEGER UNSIGNED))
        ) AS sql_lines
    FROM neg_facts_where_conds_full
    WHERE NOT EXISTS (
        SELECT *
        FROM where_cond
        WHERE neg_facts_where_conds_full.pipeline_id = where_cond.pipeline_id
        AND neg_facts_where_conds_full.rule_id = where_cond.rule_id
    )

    UNION

    SELECT DISTINCT
        where_cond.pipeline_id,
        where_cond.rule_id,
        ARRAY_CONCAT(
            ARRAY['  WHERE ' || ARRAY_AGG(where_cond.sql)[1]],
            TRANSFORM(
                GRASP_TEXT_ARRAY_DROP_LEFT(ARRAY_AGG(where_cond.sql), CAST(1 AS INTEGER UNSIGNED)),
                (sql_line) -> '    AND ' || sql_line)
        ) AS sql_lines
    FROM where_cond
    WHERE NOT EXISTS (
        SELECT *
        FROM neg_facts_where_conds_full
        WHERE where_cond.pipeline_id = neg_facts_where_conds_full.pipeline_id
        AND where_cond.rule_id = neg_facts_where_conds_full.rule_id
    )
    GROUP BY where_cond.pipeline_id, where_cond.rule_id
    
    UNION
    
    SELECT DISTINCT
        neg_facts_where_conds_full.pipeline_id,
        neg_facts_where_conds_full.rule_id,
        ARRAY_CONCAT(
            ARRAY['  WHERE ' || neg_facts_where_conds_full.sql_lines[1]],
            GRASP_TEXT_ARRAY_DROP_LEFT(neg_facts_where_conds_full.sql_lines, CAST(1 AS INTEGER UNSIGNED)),
            TRANSFORM(
                ARRAY_AGG(where_cond.sql),
                (sql_line) -> '    AND ' || sql_line)
        ) AS sql_lines
    FROM neg_facts_where_conds_full
    JOIN where_cond
        ON neg_facts_where_conds_full.pipeline_id = where_cond.pipeline_id
        AND neg_facts_where_conds_full.rule_id = where_cond.rule_id
    GROUP BY neg_facts_where_conds_full.pipeline_id, neg_facts_where_conds_full.rule_id, neg_facts_where_conds_full.sql_lines
    
    UNION
    
    SELECT DISTINCT
        rule.pipeline_id,
        rule.rule_id,
        CAST(ARRAY() AS TEXT ARRAY) AS sql_lines
    FROM rule
    WHERE NOT EXISTS (
        SELECT *
        FROM neg_facts_where_conds_full
        WHERE rule.pipeline_id = neg_facts_where_conds_full.pipeline_id
        AND rule.rule_id = neg_facts_where_conds_full.rule_id
    )
    AND NOT EXISTS (
        SELECT *
        FROM where_cond
        WHERE rule.pipeline_id = where_cond.pipeline_id
        AND rule.rule_id = where_cond.rule_id
    );

/*
having_cond(pipeline_id:, rule_id:, sql:) <-
    body_expr(pipeline_id:, rule_id:, expr_id:, expr_type:)
    substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, sql:, aggregated: true)
*/
CREATE MATERIALIZED VIEW having_cond AS
    SELECT DISTINCT
        body_expr.pipeline_id,
        body_expr.rule_id,
        substituted_expr.sql
    FROM body_expr
    JOIN substituted_expr
        ON body_expr.pipeline_id = substituted_expr.pipeline_id
        AND body_expr.rule_id = substituted_expr.rule_id
        AND body_expr.expr_id = substituted_expr.expr_id
        AND body_expr.expr_type = substituted_expr.expr_type
    WHERE substituted_expr.aggregated;

/*
having_cond_sql(pipeline_id:, rule_id:, sql_lines: []) <-
    rule(pipeline_id:, rule_id:)
    not having_cond(pipeline_id:, rule_id:)
having_cond_sql(pipeline_id:, rule_id:, sql_lines:) <-
    having_cond(pipeline_id:, rule_id:, sql:)
    [first_sql, *rest_sql] := array<sql>
    sql_lines := [
        `  HAVING {{first_sql}}`,
        *map(rest_sql, (sql) -> `    AND {{sql}}`),
    ]
*/
CREATE MATERIALIZED VIEW having_cond_sql AS
    SELECT DISTINCT
        rule.pipeline_id,
        rule.rule_id,
        CAST(ARRAY() AS TEXT ARRAY) AS sql_lines
    FROM rule
    WHERE NOT EXISTS (
        SELECT *
        FROM having_cond
        WHERE rule.pipeline_id = having_cond.pipeline_id
        AND rule.rule_id = having_cond.rule_id
    )

    UNION

    SELECT DISTINCT
        having_cond.pipeline_id,
        having_cond.rule_id,
        ARRAY_CONCAT(
            ARRAY['  HAVING ' || ARRAY_AGG(having_cond.sql)[1]],
            TRANSFORM(
                GRASP_TEXT_ARRAY_DROP_LEFT(ARRAY_AGG(having_cond.sql), CAST(1 AS INTEGER UNSIGNED)),
                (sql_line) -> '    AND ' || sql_line)
        ) AS sql_lines
    FROM having_cond
    GROUP BY having_cond.pipeline_id, having_cond.rule_id;

/*
var_join(pipeline_id:, rule_id:, fact_id:, var_name:, sql:) <-
    canonical_fact_var_sql(pipeline_id:, rule_id:, fact_index: prev_fact_index, var_name:, sql: prev_sql)
    var_bound_in_fact(pipeline_id:, rule_id:, fact_id:, fact_index: next_fact_index, var_name:, sql: next_sql, negated: false)
    prev_fact_index < next_fact_index
    sql := `{{next_sql}} = {{prev_sql}}`
*/
CREATE MATERIALIZED VIEW var_join AS
    SELECT DISTINCT
        var_bound_in_fact.pipeline_id,
        var_bound_in_fact.rule_id,
        var_bound_in_fact.fact_id,
        canonical_fact_var_sql.var_name,
        (var_bound_in_fact.sql || ' = ' || canonical_fact_var_sql.sql) AS sql
    FROM canonical_fact_var_sql
    JOIN var_bound_in_fact
        ON canonical_fact_var_sql.pipeline_id = var_bound_in_fact.pipeline_id
        AND canonical_fact_var_sql.rule_id = var_bound_in_fact.rule_id
        AND canonical_fact_var_sql.var_name = var_bound_in_fact.var_name
        AND NOT var_bound_in_fact.negated
    WHERE canonical_fact_var_sql.fact_index < var_bound_in_fact.fact_index;

/*
rule_join_sql(pipeline_id:, rule_id:, fact_id:, sql_lines:) <-
    first_fact_alias(pipeline_id:, rule_id:, fact_id:, table_name:, alias:, negated: false)
    output_table_name(pipeline_id:, table_name:, output_table_name:)
    sql_lines := [`  FROM "{{output_table_name}}" AS "{{alias}}"`]
rule_join_sql(pipeline_id:, rule_id:, fact_id:, sql_lines:) <-
    rule_join_sql(pipeline_id:, rule_id:, fact_id: prev_fact_id, sql_lines: prev_sql_lines)
    adjacent_facts(pipeline_id:, rule_id:, prev_fact_id:, next_fact_id:)
    fact_alias(pipeline_id:, rule_id:, table_name:, fact_id: next_fact_id, alias: next_alias)
    output_table_name(pipeline_id:, table_name:, output_table_name:)
    not var_join(pipeline_id:, rule_id:, fact_id: next_fact_id)
    sql_lines := [*prev_sql_lines, `  CROSS JOIN "{{output_table_name}}" AS "{{next_alias}}"`]
rule_join_sql(pipeline_id:, rule_id:, fact_id: next_fact_id, sql_lines:) <-
    rule_join_sql(pipeline_id:, rule_id:, fact_id: prev_fact_id, sql_lines: prev_sql_lines)
    adjacent_facts(pipeline_id:, rule_id:, prev_fact_id:, next_fact_id:)
    fact_alias(pipeline_id:, rule_id:, table_name:, fact_id: next_fact_id, alias: next_alias)
    var_join(pipeline_id:, rule_id:, fact_id: next_fact_id, sql:)
    output_table_name(pipeline_id:, table_name:, output_table_name:)
    [first_cond, *rest_conds] := array<sql>
    sql_lines := [
        *prev_sql_lines,
        `  JOIN "{{output_table_name}}" AS "{{next_alias}}"`,
        `    ON {{first_cond}}`,
        *map(rest_conds, x -> `AND {{x}}`),
    ]
*/
DECLARE RECURSIVE VIEW rule_join_sql (pipeline_id TEXT, rule_id TEXT, fact_id TEXT, sql_lines TEXT ARRAY);
CREATE MATERIALIZED VIEW rule_join_sql AS
    SELECT DISTINCT
        first_fact_alias.pipeline_id,
        first_fact_alias.rule_id,
        first_fact_alias.fact_id,
        ARRAY['  FROM "' || output_table_name.output_table_name || '" AS "' || first_fact_alias.alias || '"'] AS sql_lines
    FROM first_fact_alias
    JOIN output_table_name
        ON first_fact_alias.pipeline_id = output_table_name.pipeline_id
        AND first_fact_alias.table_name = output_table_name.table_name
    WHERE NOT first_fact_alias.negated
    
    UNION

    SELECT DISTINCT
        next_fact_alias.pipeline_id,
        next_fact_alias.rule_id,
        next_fact_alias.fact_id,
        ARRAY_CONCAT(
            prev_rule_join_sql.sql_lines,
            ARRAY['  CROSS JOIN "' || output_table_name.output_table_name || '" AS "' || next_fact_alias.alias || '"']) AS sql_lines
    FROM rule_join_sql AS prev_rule_join_sql
    JOIN adjacent_facts
        ON prev_rule_join_sql.pipeline_id = adjacent_facts.pipeline_id
        AND prev_rule_join_sql.rule_id = adjacent_facts.rule_id
        AND prev_rule_join_sql.fact_id = adjacent_facts.prev_fact_id
    JOIN fact_alias AS next_fact_alias
        ON adjacent_facts.pipeline_id = next_fact_alias.pipeline_id
        AND adjacent_facts.rule_id = next_fact_alias.rule_id
        AND adjacent_facts.next_fact_id = next_fact_alias.fact_id
    JOIN output_table_name
        ON next_fact_alias.pipeline_id = output_table_name.pipeline_id
        AND next_fact_alias.table_name = output_table_name.table_name
    WHERE NOT EXISTS (
        SELECT 1
        FROM var_join
        WHERE adjacent_facts.pipeline_id = var_join.pipeline_id
        AND adjacent_facts.rule_id = var_join.rule_id
        AND adjacent_facts.next_fact_id = var_join.fact_id
    )

    UNION

    SELECT DISTINCT
        next_fact_alias.pipeline_id,
        next_fact_alias.rule_id,
        next_fact_alias.fact_id,
        ARRAY_CONCAT(
            prev_rule_join_sql.sql_lines,
            ARRAY['  JOIN "' || output_table_name.output_table_name || '" AS "' || next_fact_alias.alias || '"'],
            ARRAY['    ON ' || ARRAY_AGG(var_join.sql)[1]],
            TRANSFORM(
                GRASP_TEXT_ARRAY_DROP_LEFT(
                    ARRAY_AGG(var_join.sql),
                    CAST(1 AS INTEGER UNSIGNED)),
                x -> '    AND ' || x)
        ) AS sql_lines
    FROM rule_join_sql AS prev_rule_join_sql
    JOIN adjacent_facts
        ON prev_rule_join_sql.pipeline_id = adjacent_facts.pipeline_id
        AND prev_rule_join_sql.rule_id = adjacent_facts.rule_id
        AND prev_rule_join_sql.fact_id = adjacent_facts.prev_fact_id
    JOIN fact_alias AS next_fact_alias
        ON adjacent_facts.pipeline_id = next_fact_alias.pipeline_id
        AND adjacent_facts.rule_id = next_fact_alias.rule_id
        AND adjacent_facts.next_fact_id = next_fact_alias.fact_id
    JOIN var_join
        ON adjacent_facts.pipeline_id = var_join.pipeline_id
        AND adjacent_facts.rule_id = var_join.rule_id
        AND adjacent_facts.next_fact_id = var_join.fact_id
    JOIN output_table_name
        ON next_fact_alias.pipeline_id = output_table_name.pipeline_id
        AND next_fact_alias.table_name = output_table_name.table_name
    GROUP BY next_fact_alias.pipeline_id, next_fact_alias.rule_id, next_fact_alias.fact_id, prev_rule_join_sql.sql_lines, output_table_name.output_table_name, next_fact_alias.alias;

/*
unaggregated_param_expr(pipeline_id:, rule_id:, key:, expr_id:, expr_type:, sql:) <-
    rule_param(pipeline_id:, rule_id:, key:, expr_id:, expr_type:)
    substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, sql:, aggregated: false)
*/
CREATE MATERIALIZED VIEW unaggregated_param_expr AS
    SELECT DISTINCT
        rule_param.pipeline_id,
        rule_param.rule_id,
        rule_param.key,
        rule_param.expr_id,
        rule_param.expr_type,
        substituted_expr.sql
    FROM rule_param
    JOIN substituted_expr
        ON rule_param.pipeline_id = substituted_expr.pipeline_id
        AND rule_param.rule_id = substituted_expr.rule_id
        AND rule_param.expr_id = substituted_expr.expr_id
        AND rule_param.expr_type = substituted_expr.expr_type
    WHERE NOT substituted_expr.aggregated;

/*
has_aggregation(pipeline_id:, rule_id:) <-
    substituted_expr(pipeline_id:, rule_id:, aggregated: true)
*/
CREATE MATERIALIZED VIEW has_aggregation AS
    SELECT DISTINCT
        substituted_expr.pipeline_id,
        substituted_expr.rule_id
    FROM substituted_expr
    WHERE substituted_expr.aggregated;

/*
grouped_by_sql(pipeline_id:, rule_id:, sql_lines:) <-
    unaggregated_param_expr(pipeline_id:, rule_id:, sql: param_sql)
    has_aggregation(pipeline_id:, rule_id:)
    exprs_sql := join(array<param_sql>, ", ")
    sql_lines := [`  GROUP BY {{exprs_sql}}`]
grouped_by_sql(pipeline_id:, rule_id:, sql_lines:) <-
    unaggregated_param_expr(pipeline_id:, rule_id:)
    not has_aggregation(pipeline_id:, rule_id:)
    sql_lines := []
grouped_by_sql(pipeline_id:, rule_id:, sql_lines:) <-
    not unaggregated_param_expr(pipeline_id:, rule_id:)
    has_aggregation(pipeline_id:, rule_id:)
    sql_lines := []
*/
CREATE MATERIALIZED VIEW grouped_by_sql AS
    SELECT DISTINCT
        unaggregated_param_expr.pipeline_id,
        unaggregated_param_expr.rule_id,
        ARRAY[('  GROUP BY ' || ARRAY_TO_STRING(ARRAY_AGG(unaggregated_param_expr.sql), ', '))] AS sql_lines
    FROM unaggregated_param_expr
    JOIN has_aggregation
        ON unaggregated_param_expr.pipeline_id = has_aggregation.pipeline_id
        AND unaggregated_param_expr.rule_id = has_aggregation.rule_id
    GROUP BY unaggregated_param_expr.pipeline_id, unaggregated_param_expr.rule_id

    UNION

    SELECT DISTINCT
        unaggregated_param_expr.pipeline_id,
        unaggregated_param_expr.rule_id,
        CAST(ARRAY() AS TEXT ARRAY) AS sql_lines
    FROM unaggregated_param_expr
    WHERE NOT EXISTS (
        SELECT 1
        FROM has_aggregation
        WHERE unaggregated_param_expr.pipeline_id = has_aggregation.pipeline_id
        AND unaggregated_param_expr.rule_id = has_aggregation.rule_id
    )

    UNION

    SELECT DISTINCT
        has_aggregation.pipeline_id,
        has_aggregation.rule_id,
        CAST(ARRAY() AS TEXT ARRAY) AS sql_lines
    FROM has_aggregation
    WHERE NOT EXISTS (
        SELECT 1
        FROM unaggregated_param_expr
        WHERE has_aggregation.pipeline_id = unaggregated_param_expr.pipeline_id
        AND has_aggregation.rule_id = unaggregated_param_expr.rule_id
    );

/*
join_sql(pipeline_id:, rule_id:, sql_lines:) <-
    # if there is only one fact, then there is no adjacent fact
    rule_join_sql(pipeline_id:, rule_id:, fact_id:, sql_lines:)
    not adjacent_facts(pipeline_id:, rule_id:, next_fact_id: fact_id)
    not adjacent_facts(pipeline_id:, rule_id:, prev_fact_id: fact_id)
join_sql(pipeline_id:, rule_id:, sql_lines:) <-
    # pick the last fact_id, for which there is no next one
    adjacent_facts(pipeline_id:, rule_id:, next_fact_id: last_fact_id)
    not adjacent_facts(pipeline_id:, rule_id:, prev_fact_id: last_fact_id)
    rule_join_sql(pipeline_id:, rule_id:, fact_id: last_fact_id, sql_lines:)
*/
CREATE MATERIALIZED VIEW join_sql AS
    SELECT DISTINCT
        rule_join_sql.pipeline_id,
        rule_join_sql.rule_id,
        rule_join_sql.sql_lines
    FROM rule_join_sql
    WHERE NOT EXISTS (
        SELECT 1
        FROM adjacent_facts
        WHERE rule_join_sql.pipeline_id = adjacent_facts.pipeline_id
        AND rule_join_sql.rule_id = adjacent_facts.rule_id
        AND adjacent_facts.next_fact_id = rule_join_sql.fact_id
    )
    AND NOT EXISTS (
        SELECT 1
        FROM adjacent_facts
        WHERE rule_join_sql.pipeline_id = adjacent_facts.pipeline_id
        AND rule_join_sql.rule_id = adjacent_facts.rule_id
        AND adjacent_facts.prev_fact_id = rule_join_sql.fact_id
    )
 
    UNION

    SELECT DISTINCT
        rule_join_sql.pipeline_id,
        rule_join_sql.rule_id,
        rule_join_sql.sql_lines
    FROM rule_join_sql
    JOIN adjacent_facts AS a
        ON rule_join_sql.pipeline_id = a.pipeline_id
        AND rule_join_sql.rule_id = a.rule_id
        AND rule_join_sql.fact_id = a.next_fact_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM adjacent_facts
        WHERE rule_join_sql.pipeline_id = adjacent_facts.pipeline_id
        AND rule_join_sql.rule_id = adjacent_facts.rule_id
        AND adjacent_facts.prev_fact_id = a.next_fact_id
    );

/*
substituted_param_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, sql:) <-
    rule_param(pipeline_id:, rule_id:, key:, expr_id:, expr_type:)
    substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, sql:)
    expr_type != 'array_expr'
    expr_type != 'dict_expr'
substituted_param_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, sql:) <-
    rule_param(pipeline_id:, rule_id:, key:, expr_id:, expr_type:)
    substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, sql: expr_sql)
    (expr_type = 'array_expr') or (expr_type = 'dict_expr')
    sql := `CAST({{expr_sql}} AS VARIANT)`
*/  
CREATE MATERIALIZED VIEW substituted_param_expr AS
    SELECT DISTINCT
        rule_param.pipeline_id,
        rule_param.rule_id,
        rule_param.expr_id,
        rule_param.expr_type,
        substituted_expr.sql
    FROM rule_param
    JOIN substituted_expr
        ON rule_param.pipeline_id = substituted_expr.pipeline_id
        AND rule_param.rule_id = substituted_expr.rule_id
        AND rule_param.expr_id = substituted_expr.expr_id
        AND rule_param.expr_type = substituted_expr.expr_type
    WHERE rule_param.expr_type != 'array_expr'
    AND rule_param.expr_type != 'dict_expr'

    UNION

    SELECT DISTINCT
        rule_param.pipeline_id,
        rule_param.rule_id,
        rule_param.expr_id,
        rule_param.expr_type,
        ('CAST(' || substituted_expr.sql || ' AS VARIANT)') AS sql
    FROM rule_param
    JOIN substituted_expr
        ON rule_param.pipeline_id = substituted_expr.pipeline_id
        AND rule_param.rule_id = substituted_expr.rule_id
        AND rule_param.expr_id = substituted_expr.expr_id
        AND rule_param.expr_type = substituted_expr.expr_type
    WHERE ((rule_param.expr_type = 'array_expr') OR (rule_param.expr_type = 'dict_expr'));

/*
select_sql(pipeline_id:, rule_id:, sql_lines:) <-
    constant_rule(pipeline_id:, rule_id:)
    rule_param(pipeline_id:, rule_id:, key:, expr_id:, expr_type:)
    substituted_param_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, sql: expr_sql)
    columns_sql := join(array<`{{expr_sql}} AS "{{key}}"`, by: key>, ", ")
    sql_lines := [`  SELECT DISTINCT {{columns_sql}}`]
select_sql(pipeline_id:, rule_id:, sql_lines:) <-
    rule_param(pipeline_id:, rule_id:, key:, expr_id:, expr_type:)
    substituted_param_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, sql: expr_sql)
    full_where_cond_sql(pipeline_id:, rule_id:, sql_lines: where_sql_lines)
    having_cond_sql(pipeline_id:, rule_id:, sql_lines: having_sql_lines)
    grouped_by_sql(pipeline_id:, rule_id:, sql_lines: group_by_sql_lines)
    join_sql(pipeline_id:, rule_id:, sql_lines: join_sql_lines)
    columns_sql := join(array<`{{expr_sql}} AS "{{key}}"`, by: key>, ", ")
    sql_lines := [
        `  SELECT DISTINCT {{columns_sql}}`,
        *join_sql_lines,
        *where_sql_lines,
        *group_by_sql_lines,
        *having_sql_lines,
    ]
*/
CREATE MATERIALIZED VIEW select_sql AS
    SELECT DISTINCT
        rule_param.pipeline_id,
        rule_param.rule_id,
        ARRAY['  SELECT DISTINCT ' || ARRAY_TO_STRING(ARRAY_AGG(substituted_param_expr.sql || ' AS "' || rule_param.key || '"' ORDER BY rule_param.key), ', ')] AS sql_lines
    FROM constant_rule
    JOIN rule_param
        ON constant_rule.pipeline_id = rule_param.pipeline_id
        AND constant_rule.rule_id = rule_param.rule_id
    JOIN substituted_param_expr
        ON rule_param.pipeline_id = substituted_param_expr.pipeline_id
        AND rule_param.rule_id = substituted_param_expr.rule_id
        AND rule_param.expr_id = substituted_param_expr.expr_id
        AND rule_param.expr_type = substituted_param_expr.expr_type
    WHERE NOT EXISTS (
        SELECT 1
        FROM fact_alias
        WHERE rule_param.pipeline_id = fact_alias.pipeline_id
        AND rule_param.rule_id = fact_alias.rule_id
    )
    GROUP BY rule_param.pipeline_id, rule_param.rule_id

    UNION

    SELECT DISTINCT
        rule_param.pipeline_id,
        rule_param.rule_id,
        ARRAY_CONCAT(
            ARRAY['  SELECT DISTINCT ' || ARRAY_TO_STRING(ARRAY_AGG(substituted_param_expr.sql || ' AS "' || rule_param.key || '"' ORDER BY rule_param.key), ', ')],
            join_sql.sql_lines,
            full_where_cond_sql.sql_lines,
            grouped_by_sql.sql_lines,
            having_cond_sql.sql_lines
        ) AS sql_lines
    FROM rule_param
    JOIN substituted_param_expr
        ON rule_param.pipeline_id = substituted_param_expr.pipeline_id
        AND rule_param.rule_id = substituted_param_expr.rule_id
        AND rule_param.expr_id = substituted_param_expr.expr_id
        AND rule_param.expr_type = substituted_param_expr.expr_type
    JOIN full_where_cond_sql
        ON rule_param.pipeline_id = full_where_cond_sql.pipeline_id
        AND rule_param.rule_id = full_where_cond_sql.rule_id
    JOIN having_cond_sql
        ON rule_param.pipeline_id = having_cond_sql.pipeline_id
        AND rule_param.rule_id = having_cond_sql.rule_id
    JOIN grouped_by_sql
        ON rule_param.pipeline_id = grouped_by_sql.pipeline_id
        AND rule_param.rule_id = grouped_by_sql.rule_id
    JOIN join_sql
        ON rule_param.pipeline_id = join_sql.pipeline_id
        AND rule_param.rule_id = join_sql.rule_id
    GROUP BY rule_param.pipeline_id, rule_param.rule_id, join_sql.sql_lines, full_where_cond_sql.sql_lines, grouped_by_sql.sql_lines, having_cond_sql.sql_lines;

/*
table_view_sql(pipeline_id:, table_name:, rule_id:, sql_lines:) <-
    table_first_rule(pipeline_id:, table_name:, rule_id:)
    select_sql(pipeline_id:, rule_id:, sql_lines: rule_sql_lines)
    output_table_name(pipeline_id:, table_name:, output_table_name:)
    sql_lines := [
        `CREATE MATERIALIZED VIEW "{{output_table_name}}" AS`,
        *rule_sql_lines,
    ]
table_view_sql(pipeline_id:, table_name:, rule_id:, sql_lines:) <-
    table_view_sql(pipeline_id:, table_name:, rule_id: prev_rule_id, sql_lines: prev_sql_lines)
    table_next_rule(pipeline_id:, table_name:, prev_rule_id:, next_rule_id:)
    select_sql(pipeline_id:, rule_id: next_rule_id, sql_lines: next_sql_lines)
    # table_next_rule(pipeline_id:, table_name:, prev_rule_id: next_rule_id)
    sql_lines := [*prev_sql_lines, "  UNION", *next_sql_lines]
*/
DECLARE RECURSIVE VIEW table_view_sql (pipeline_id TEXT, table_name TEXT, rule_id TEXT, sql_lines TEXT ARRAY);
CREATE MATERIALIZED VIEW table_view_sql AS
    SELECT DISTINCT
        table_first_rule.pipeline_id,
        table_first_rule.table_name,
        table_first_rule.rule_id,
        ARRAY_CONCAT(
            ARRAY['CREATE MATERIALIZED VIEW "' || output_table_name.output_table_name || '" AS'],
            select_sql.sql_lines
        ) AS sql_lines
    FROM table_first_rule
    JOIN select_sql
        ON table_first_rule.pipeline_id = select_sql.pipeline_id
        AND table_first_rule.rule_id = select_sql.rule_id
    JOIN output_table_name
        ON table_first_rule.pipeline_id = output_table_name.pipeline_id
        AND table_first_rule.table_name = output_table_name.table_name

    UNION

    SELECT DISTINCT
        tn.pipeline_id,
        tn.table_name,
        tn.next_rule_id,
        ARRAY_CONCAT(
            table_view_sql.sql_lines,
            ARRAY['  UNION'],
            select_sql.sql_lines
        ) AS sql_lines
    FROM table_view_sql
    JOIN table_next_rule AS tn
        ON tn.pipeline_id = table_view_sql.pipeline_id
        AND tn.table_name = table_view_sql.table_name
        AND tn.prev_rule_id = table_view_sql.rule_id
    JOIN select_sql
        ON tn.pipeline_id = select_sql.pipeline_id
        AND tn.next_rule_id = select_sql.rule_id
    -- JOIN table_next_rule AS tnn
    --     ON tn.pipeline_id = tnn.pipeline_id
    --     AND tn.table_name = tnn.table_name
    --     AND tn.next_rule_id = tnn.prev_rule_id
    ;

/*
output_column_type(input_type: "JSON", output_type: "VARIANT")
output_column_type(input_type:, output_type:) <-
    schema_table_column(column_type: input_type)
    input_type != "JSON"
    output_type := input_type
output_column_type(input_type:, output_type:) <-
    var_expr(assigned_type: input_type)
    input_type != "JSON"
    output_type := input_type
*/
CREATE MATERIALIZED VIEW output_column_type AS
    SELECT 'JSON' AS input_type, 'VARIANT' AS output_type
    UNION
    SELECT DISTINCT
        schema_table_column.column_type AS input_type,
        schema_table_column.column_type AS output_type
    FROM schema_table_column
    WHERE schema_table_column.column_type != 'JSON'
    UNION
    SELECT DISTINCT
        var_expr.assigned_type AS input_type,
        var_expr.assigned_type AS output_type
    FROM var_expr
    WHERE var_expr.assigned_type != 'JSON';

/*
schema_table_column_sql(pipeline_id:, table_name:, column_name:, sql:) <-
    schema_table_column(
        pipeline_id:, table_name:, column_name:, column_type:, nullable: true)
    output_column_type(input_type: column_type, output_type:)
    sql := `{{column_name}} {{output_type}}`
schema_table_column_sql(pipeline_id:, table_name:, column_name:, sql:) <-
    schema_table_column(
        pipeline_id:, table_name:, column_name:, column_type:, nullable: false)
    output_column_type(input_type: column_type, output_type:)
    sql := `{{column_name}} {{output_type}} NOT NULL`
*/
CREATE MATERIALIZED VIEW schema_table_column_sql AS
    SELECT DISTINCT
        schema_table_column.pipeline_id,
        schema_table_column.table_name,
        schema_table_column.column_name,
        (schema_table_column.column_name || ' ' || output_column_type.output_type) AS sql
    FROM schema_table_column
    JOIN output_column_type
        ON schema_table_column.column_type = output_column_type.input_type
    WHERE schema_table_column.nullable

    UNION

    SELECT DISTINCT
        schema_table_column.pipeline_id,
        schema_table_column.table_name,
        schema_table_column.column_name,
        (schema_table_column.column_name || ' ' || output_column_type.output_type || ' NOT NULL') AS sql
    FROM schema_table_column
    JOIN output_column_type
        ON schema_table_column.column_type = output_column_type.input_type
    WHERE NOT schema_table_column.nullable;

/*
schema_table_with_sql(pipeline_id:, table_name:, with_sql:) <-
    schema_table(pipeline_id:, table_name:, materialized: true)
    with_sql := "WITH ('materialized' = 'true')"
schema_table_with_sql(pipeline_id:, table_name:, with_sql: "") <-
    schema_table(pipeline_id:, table_name:, materialized: false)
*/
CREATE MATERIALIZED VIEW schema_table_with_sql AS
    SELECT DISTINCT
        schema_table.pipeline_id,
        schema_table.table_name,
        'WITH (' || '''' || 'materialized' || '''' || ' = ' || '''' || 'true' || '''' || ')' AS sql
    FROM schema_table
    WHERE schema_table."materialized"
    
    UNION

    SELECT DISTINCT
        schema_table.pipeline_id,
        schema_table.table_name,
        '' AS sql
    FROM schema_table
    WHERE NOT schema_table."materialized";

/*
schema_table_sql(pipeline_id:, table_name:, sql_lines:) <-
    schema_table_with_sql(pipeline_id:, table_name:, sql: with_sql)
    schema_table_column_sql(pipeline_id:, table_name:, column_name:, sql:)
    output_table_name(pipeline_id:, table_name:, output_table_name:)
    [*init_lines, last_line] := array<sql>
    sql_lines := [
        `CREATE TABLE "{{output_table_name}}" (`,
        *map(init_lines, x -> `  {{x}},`),
        `  {{last_line}}`,
        `) {{with_sql}} ;`,
    ]
*/
CREATE MATERIALIZED VIEW schema_table_sql AS
    SELECT DISTINCT
        schema_table_with_sql.pipeline_id,
        schema_table_with_sql.table_name,
        ARRAY_CONCAT(
            ARRAY['CREATE TABLE "' || output_table_name.output_table_name || '" ('],
            TRANSFORM(
                GRASP_TEXT_ARRAY_DROP_RIGHT(
                    ARRAY_AGG(schema_table_column_sql.sql),
                    CAST(1 AS INTEGER UNSIGNED)),
                x -> '  ' || x || ','),
            ARRAY['  ' || ARRAY_AGG(schema_table_column_sql.sql)[ARRAY_LENGTH(ARRAY_AGG(schema_table_column_sql.sql))]],
            ARRAY[') ' || schema_table_with_sql.sql || ' ;']
        ) AS sql_lines
    FROM schema_table_with_sql
    JOIN schema_table_column_sql
        ON schema_table_with_sql.pipeline_id = schema_table_column_sql.pipeline_id
        AND schema_table_with_sql.table_name = schema_table_column_sql.table_name
    JOIN output_table_name
        ON schema_table_with_sql.pipeline_id = output_table_name.pipeline_id
        AND schema_table_with_sql.table_name = output_table_name.table_name
    GROUP BY schema_table_with_sql.pipeline_id, schema_table_with_sql.table_name, schema_table_with_sql.sql, output_table_name.output_table_name;

/*
table_sql(pipeline_id:, table_name:, sql_lines:) <-
    table_last_rule(pipeline_id:, table_name:, rule_id:)
    table_view_sql(pipeline_id:, table_name:, rule_id:, sql_lines: sql_lines0)
    sql_lines := [*sql_lines0, ";", ""]
table_sql(pipeline_id:, table_name:, sql_lines:) <-
    schema_table(pipeline_id:, table_name:)
    schema_table_sql(pipeline_id:, table_name:, sql_lines:)
*/
CREATE MATERIALIZED VIEW table_sql AS
    SELECT DISTINCT
        table_view_sql.pipeline_id,
        table_view_sql.table_name,
        ARRAY_CONCAT(table_view_sql.sql_lines, ARRAY[';', '']) AS sql_lines
    FROM table_last_rule
    JOIN table_view_sql
        ON table_last_rule.pipeline_id = table_view_sql.pipeline_id
        AND table_last_rule.table_name = table_view_sql.table_name
        AND table_last_rule.rule_id = table_view_sql.rule_id
    
    UNION

    SELECT DISTINCT
        schema_table.pipeline_id,
        schema_table.table_name,
        schema_table_sql.sql_lines
    FROM schema_table
    JOIN schema_table_sql
        ON schema_table.pipeline_id = schema_table_sql.pipeline_id
        AND schema_table.table_name = schema_table_sql.table_name
    ;

/*
pipeline_tables_sql(pipeline_id:, table_name:, sql_lines:) <-
    first_table(pipeline_id:, table_name:, order: 0)
    table_sql(pipeline_id:, table_name:, sql_lines:)
pipeline_tables_sql(pipeline_id:, table_name:, sql_lines:) <-
    pipeline_tables_sql(pipeline_id:, table_name: prev_table_name, sql_lines: prev_sql_lines)
    next_table(pipeline_id:, prev_table_name:, next_table_name: table_name)
    table_sql(pipeline_id:, table_name:, sql_lines: next_sql_lines)
    sql_lines := [*prev_sql_lines, *next_sql_lines]
*/
DECLARE RECURSIVE VIEW pipeline_tables_sql (pipeline_id TEXT, table_name TEXT, sql_lines TEXT ARRAY);
CREATE MATERIALIZED VIEW pipeline_tables_sql AS
    SELECT DISTINCT
        first_table.pipeline_id,
        first_table.table_name,
        table_sql.sql_lines
    FROM first_table
    JOIN table_sql
        ON first_table.pipeline_id = table_sql.pipeline_id
        AND first_table.table_name = table_sql.table_name
    WHERE first_table."order" = 0

    UNION

    SELECT DISTINCT
        next_table.pipeline_id,
        next_table.next_table_name,
        ARRAY_CONCAT(
            pipeline_tables_sql.sql_lines,
            table_sql.sql_lines
        ) AS sql_lines
    FROM pipeline_tables_sql
    JOIN next_table
        ON next_table.pipeline_id = pipeline_tables_sql.pipeline_id
        AND next_table.prev_table_name = pipeline_tables_sql.table_name
    JOIN table_sql
        ON next_table.pipeline_id = table_sql.pipeline_id
        AND next_table.next_table_name = table_sql.table_name;

/*
full_pipeline_sql(pipeline_id:, sql_lines:) <-
    pipeline_tables_sql(pipeline_id:, table_name:, sql_lines:)
    not next_table(pipeline_id:, prev_table_name: table_name)
*/
CREATE MATERIALIZED VIEW full_pipeline_sql AS
    SELECT DISTINCT
        pipeline_tables_sql.pipeline_id,
        pipeline_tables_sql.sql_lines
    FROM pipeline_tables_sql
    WHERE NOT EXISTS (
        SELECT 1
        FROM next_table
        WHERE next_table.pipeline_id = pipeline_tables_sql.pipeline_id
        AND next_table.prev_table_name = pipeline_tables_sql.table_name
    );
