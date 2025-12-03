/*
aggr_fn_signature(fn_name: "count", sql_name: "COUNT", val_arg_count: 0, kv_arg_keys: [])
aggr_fn_signature(fn_name: "min", sql_name: "MIN", val_arg_count: 1, kv_arg_keys: [])
aggr_fn_signature(fn_name: "max", sql_name: "MAX", val_arg_count: 1, kv_arg_keys: [])
aggr_fn_signature(fn_name: "some", sql_name: "SOME", val_arg_count: 1, kv_arg_keys: [])
aggr_fn_signature(fn_name: "argmin", sql_name: "ARG_MIN", val_arg_count: 1, kv_arg_keys: ["by"])
aggr_fn_signature(fn_name: "argmax", sql_name: "ARG_MAX", val_arg_count: 1, kv_arg_keys: ["by"])
*/
CREATE MATERIALIZED VIEW aggr_fn_signature AS
    SELECT DISTINCT
        t.fn_name,
        t.sql_name,
        t.val_arg_count,
        CAST(t.kv_arg_keys AS TEXT ARRAY) AS kv_arg_keys
    FROM (
        VALUES
            ('count', 'COUNT', 0, ARRAY()),
            ('min', 'MIN', 1, ARRAY()),
            ('max', 'MAX', 1, ARRAY()),
            ('some', 'SOME', 1, ARRAY()),
            ('argmin', 'ARG_MIN', 1, SORT_ARRAY(ARRAY['by'])),
            ('argmax', 'ARG_MAX', 1, SORT_ARRAY(ARRAY['by']))
    ) AS t (fn_name, sql_name, val_arg_count, kv_arg_keys);

/*
aggr_expr_val_arg_count(pipeline_id:, rule_id:, expr_id:, val_arg_count:) <-
    fncall_expr(pipeline_id:, rule_id:, expr_id:, fncall_id:, aggregated: true)
    fn_val_arg(pipeline_id:, rule_id:, fncall_id:)
    val_arg_count := count<>
aggr_expr_val_arg_count(pipeline_id:, rule_id:, expr_id:, val_arg_count:) <-
    fncall_expr(pipeline_id:, rule_id:, expr_id:, fncall_id:, aggregated: true)
    not fn_val_arg(pipeline_id:, rule_id:, fncall_id:)
    val_arg_count := 0
*/
CREATE MATERIALIZED VIEW aggr_expr_val_arg_count AS
    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        COUNT(*) AS val_arg_count
    FROM fncall_expr AS a
    JOIN fn_val_arg
        ON a.pipeline_id = fn_val_arg.pipeline_id
        AND a.rule_id = fn_val_arg.rule_id
        AND a.fncall_id = fn_val_arg.fncall_id
    WHERE a.aggregated
    GROUP BY a.pipeline_id, a.rule_id, a.expr_id
    
    UNION
    
    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        0 AS val_arg_count
    FROM fncall_expr AS a
    WHERE a.aggregated
    AND NOT EXISTS (
        SELECT 1
        FROM fn_val_arg
        WHERE a.pipeline_id = fn_val_arg.pipeline_id
        AND a.rule_id = fn_val_arg.rule_id
        AND a.fncall_id = fn_val_arg.fncall_id
    );

/*
aggr_expr_matching_signature(
    pipeline_id:, rule_id:, expr_id:, fncall_id:,
    fn_name:, sql_name:, val_arg_count:, kv_arg_keys: [],
) <-
    fncall_expr(pipeline_id:, rule_id:, expr_id:, fncall_id:, aggregated: true)
    aggr_fn_signature(fn_name:, val_arg_count:, kv_arg_keys: [], sql_name:)
    aggr_expr_val_arg_count(pipeline_id:, rule_id:, expr_id:, val_arg_count:)
    not fn_kv_arg(pipeline_id:, rule_id:, fncall_id:)
aggr_expr_matching_signature(
    pipeline_id:, rule_id:, expr_id:, fncall_id:,
    fn_name:, sql_name:, val_arg_count:, kv_arg_keys:,
) <-
    fncall_expr(pipeline_id:, rule_id:, expr_id:, fncall_id:, aggregated: true)
    aggr_fn_signature(fn_name:, val_arg_count:, kv_arg_keys:, sql_name:)
    aggr_expr_val_arg_count(pipeline_id:, rule_id:, expr_id:, val_arg_count:)
    fn_kv_arg(pipeline_id:, rule_id:, fncall_id:, key:)
    sort(array<key>) = kv_arg_keys
*/
CREATE MATERIALIZED VIEW aggr_expr_matching_signature AS
    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        a.fncall_id,
        b.fn_name,
        b.sql_name,
        b.val_arg_count,
        b.kv_arg_keys
    FROM fncall_expr AS a
    JOIN aggr_fn_signature AS b
        ON a.fn_name = b.fn_name
    JOIN aggr_expr_val_arg_count AS c
        ON a.pipeline_id = c.pipeline_id
        AND a.rule_id = c.rule_id
        AND a.expr_id = c.expr_id
        AND b.val_arg_count = c.val_arg_count
    WHERE ARRAY_SIZE(b.kv_arg_keys) = 0
    AND a.aggregated
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
        b.kv_arg_keys
    FROM fncall_expr AS a
    JOIN aggr_fn_signature AS b
        ON a.fn_name = b.fn_name
    JOIN aggr_expr_val_arg_count AS c
        ON a.pipeline_id = c.pipeline_id
        AND a.rule_id = c.rule_id
        AND a.expr_id = c.expr_id
        AND b.val_arg_count = c.val_arg_count
    JOIN fn_kv_arg AS d
        ON a.pipeline_id = d.pipeline_id
        AND a.rule_id = d.rule_id
        AND a.fncall_id = d.fncall_id
    WHERE a.aggregated
    GROUP BY a.pipeline_id, a.rule_id, a.expr_id, b.kv_arg_keys, b.fn_name, b.val_arg_count, b.sql_name, a.fncall_id
    HAVING b.kv_arg_keys = SORT_ARRAY(ARRAY_AGG(d.key));
