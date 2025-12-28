/*
fn_signature(fn_name: "count", sql_name: "COUNT", val_arg_count: 0, kv_arg_keys: [], aggregated: true)
fn_signature(fn_name: "min", sql_name: "MIN", val_arg_count: 1, kv_arg_keys: [], aggregated: true)
fn_signature(fn_name: "max", sql_name: "MAX", val_arg_count: 1, kv_arg_keys: [], aggregated: true)
fn_signature(fn_name: "some", sql_name: "SOME", val_arg_count: 1, kv_arg_keys: [], aggregated: true)
fn_signature(fn_name: "argmin", sql_name: "ARG_MIN", val_arg_count: 1, kv_arg_keys: ["by"], aggregated: true)
fn_signature(fn_name: "argmax", sql_name: "ARG_MAX", val_arg_count: 1, kv_arg_keys: ["by"], aggregated: true)
fn_signature(fn_name: "md5", sql_name: "MD5", val_arg_count: 1, kv_arg_keys: [], aggregated: false)
fn_signature(fn_name: "is_NULL", sql_name: NULL, val_arg_count: 1, kv_arg_keys: [], aggregated: false)
*/
CREATE MATERIALIZED VIEW fn_signature AS
    SELECT 'count' AS fn_name, 'COUNT' AS sql_name, 0 AS val_arg_count, CAST(ARRAY() AS TEXT ARRAY) AS kv_arg_keys, true AS aggregated
    UNION
    SELECT 'min' AS fn_name, 'MIN' AS sql_name, 1 AS val_arg_count, CAST(ARRAY() AS TEXT ARRAY) AS kv_arg_keys, true AS aggregated
    UNION
    SELECT 'max' AS fn_name, 'MAX' AS sql_name, 1 AS val_arg_count, CAST(ARRAY() AS TEXT ARRAY) AS kv_arg_keys, true AS aggregated
    UNION
    SELECT 'some' AS fn_name, 'SOME' AS sql_name, 1 AS val_arg_count, CAST(ARRAY() AS TEXT ARRAY) AS kv_arg_keys, true AS aggregated
    UNION
    SELECT 'argmin' AS fn_name, 'ARG_MIN' AS sql_name, 1 AS val_arg_count, ARRAY['by'] AS kv_arg_keys, true AS aggregated
    UNION
    SELECT 'argmax' AS fn_name, 'ARG_MAX' AS sql_name, 1 AS val_arg_count, ARRAY['by'] AS kv_arg_keys, true AS aggregated
    UNION
    SELECT 'md5' AS fn_name, 'MD5' AS sql_name, 1 AS val_arg_count, CAST(ARRAY() AS TEXT ARRAY) AS kv_arg_keys, false AS aggregated
    UNION
    SELECT 'is_NULL' AS fn_name, NULL AS sql_name, 1 AS val_arg_count, CAST(ARRAY() AS TEXT ARRAY) AS kv_arg_keys, false AS aggregated;

/*
fncall_expr_val_arg_count(pipeline_id:, rule_id:, expr_id:, val_arg_count:, aggregated:) <-
    fncall_expr(pipeline_id:, rule_id:, expr_id:, fncall_id:, aggregated:)
    fn_val_arg(pipeline_id:, rule_id:, fncall_id:)
    val_arg_count := count<>
fncall_expr_val_arg_count(pipeline_id:, rule_id:, expr_id:, val_arg_count:, aggregated:) <-
    fncall_expr(pipeline_id:, rule_id:, expr_id:, fncall_id:, aggregated:)
    not fn_val_arg(pipeline_id:, rule_id:, fncall_id:)
    val_arg_count := 0
*/
CREATE MATERIALIZED VIEW fncall_expr_val_arg_count AS
    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        COUNT(*) AS val_arg_count,
        a.aggregated
    FROM fncall_expr AS a
    JOIN fn_val_arg
        ON a.pipeline_id = fn_val_arg.pipeline_id
        AND a.rule_id = fn_val_arg.rule_id
        AND a.fncall_id = fn_val_arg.fncall_id
    GROUP BY a.pipeline_id, a.rule_id, a.expr_id, a.aggregated
    
    UNION
    
    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        0 AS val_arg_count,
        a.aggregated
    FROM fncall_expr AS a
    WHERE NOT EXISTS (
        SELECT 1
        FROM fn_val_arg
        WHERE a.pipeline_id = fn_val_arg.pipeline_id
        AND a.rule_id = fn_val_arg.rule_id
        AND a.fncall_id = fn_val_arg.fncall_id
    );

/*
fncall_expr_matching_signature(
    pipeline_id:, rule_id:, expr_id:, fncall_id:,
    fn_name:, sql_name:, val_arg_count:, kv_arg_keys: [],
    aggregated:,
) <-
    fncall_expr(pipeline_id:, rule_id:, expr_id:, fncall_id:, aggregated:)
    fn_signature(fn_name:, val_arg_count:, kv_arg_keys: [], sql_name:, aggregated:)
    fncall_expr_val_arg_count(pipeline_id:, rule_id:, expr_id:, val_arg_count:)
    not fn_kv_arg(pipeline_id:, rule_id:, fncall_id:)
fncall_expr_matching_signature(
    pipeline_id:, rule_id:, expr_id:, fncall_id:,
    fn_name:, sql_name:, val_arg_count:, kv_arg_keys:,
    aggregated:,
) <-
    fncall_expr(pipeline_id:, rule_id:, expr_id:, fncall_id:, aggregated:)
    fn_signature(fn_name:, val_arg_count:, kv_arg_keys:, sql_name:, aggregated:)
    fncall_expr_val_arg_count(pipeline_id:, rule_id:, expr_id:, val_arg_count:)
    fn_kv_arg(pipeline_id:, rule_id:, fncall_id:, key:)
    sort(array<key>) = kv_arg_keys
*/
CREATE MATERIALIZED VIEW fncall_expr_matching_signature AS
    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        a.fncall_id,
        b.fn_name,
        b.sql_name,
        b.val_arg_count,
        b.kv_arg_keys,
        b.aggregated
    FROM fncall_expr AS a
    JOIN fn_signature AS b
        ON a.fn_name = b.fn_name
        AND a.aggregated = b.aggregated
    JOIN fncall_expr_val_arg_count AS c
        ON a.pipeline_id = c.pipeline_id
        AND a.rule_id = c.rule_id
        AND a.expr_id = c.expr_id
        AND b.val_arg_count = c.val_arg_count
    WHERE ARRAY_SIZE(b.kv_arg_keys) = 0
    AND NOT EXISTS (
        SELECT 1
        FROM fn_kv_arg AS d
        WHERE a.pipeline_id = d.pipeline_id
        AND a.rule_id = d.rule_id
        AND a.fncall_id = d.fncall_id
    )

    UNION

    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        a.fncall_id,
        b.fn_name,
        b.sql_name,
        b.val_arg_count,
        b.kv_arg_keys,
        b.aggregated
    FROM fncall_expr AS a
    JOIN fn_signature AS b
        ON a.fn_name = b.fn_name
        AND a.aggregated = b.aggregated
    JOIN fncall_expr_val_arg_count AS c
        ON a.pipeline_id = c.pipeline_id
        AND a.rule_id = c.rule_id
        AND a.expr_id = c.expr_id
        AND b.val_arg_count = c.val_arg_count
    JOIN fn_kv_arg AS d
        ON a.pipeline_id = d.pipeline_id
        AND a.rule_id = d.rule_id
        AND a.fncall_id = d.fncall_id
    GROUP BY a.pipeline_id, a.rule_id, a.expr_id, b.kv_arg_keys, b.fn_name, b.val_arg_count, b.sql_name, a.fncall_id, b.aggregated
    HAVING b.kv_arg_keys = SORT_ARRAY(ARRAY_AGG(d.key));
