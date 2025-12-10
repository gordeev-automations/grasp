/*
array_expr_last_index(
    pipeline_id:, rule_id:, expr_id:, index: max<index>
) <-
    array_expr(pipeline_id:, rule_id:, expr_id:, array_id:)
    array_entry(pipeline_id:, rule_id:, array_id:, index:)
*/
CREATE MATERIALIZED VIEW array_expr_last_index AS
    SELECT DISTINCT
        array_expr.pipeline_id,
        array_expr.rule_id,
        array_expr.expr_id,
        MAX(array_entry."index") AS "index"
    FROM array_expr
    JOIN array_entry
        ON array_expr.pipeline_id = array_entry.pipeline_id
        AND array_expr.rule_id = array_entry.rule_id
        AND array_expr.array_id = array_entry.array_id
    GROUP BY array_expr.pipeline_id, array_expr.rule_id, array_expr.expr_id;

/*
table_dependency1(pipeline_id:, table_name:, parent_table_name:) <-
    rule(pipeline_id:, table_name:, rule_id:)
    body_fact(pipeline_id:, rule_id:, table_name: parent_table_name)
*/
CREATE MATERIALIZED VIEW table_dependency1 AS
    SELECT DISTINCT
        rule.pipeline_id,
        rule.table_name,
        body_fact.table_name AS parent_table_name
    FROM rule
    JOIN body_fact
        ON rule.pipeline_id = body_fact.pipeline_id
        AND rule.rule_id = body_fact.rule_id;

/*
table_dependency(pipeline_id:, table_name:, parent_table_name:) <-
    table_dependency1(pipeline_id:, table_name:, parent_table_name:)
table_dependency(pipeline_id:, table_name:, parent_table_name:) <-
    table_dependency(pipeline_id:, table_name:, parent_table_name:)
    table_dependency(pipeline_id:, table_name: parent_table_name, parent_table_name:)
*/
DECLARE RECURSIVE VIEW table_dependency (pipeline_id TEXT, table_name TEXT, parent_table_name TEXT);
CREATE MATERIALIZED VIEW table_dependency AS
    SELECT DISTINCT
        table_dependency1.pipeline_id,
        table_dependency1.table_name,
        table_dependency1.parent_table_name
    FROM table_dependency1

    UNION

    SELECT DISTINCT
        t1.pipeline_id,
        t1.table_name,
        t2.parent_table_name
    FROM table_dependency AS t1
    JOIN table_dependency AS t2
        ON t1.pipeline_id = t2.parent_table_name
        AND t1.parent_table_name = t2.table_name;

/*
table_dependency(pipeline_id:, table_name:, parent_table_name:) <-
    rule(pipeline_id:, table_name:, rule_id:)
    body_fact(pipeline_id:, rule_id:, table_name: parent_table_name)
table_dependency(pipeline_id:, table_name:, parent_table_name:) <-
    table_dependency(pipeline_id:, table_name:, parent_table_name:)
    table_dependency(pipeline_id:, table_name: parent_table_name, parent_table_name:)
*/
-- DECLARE RECURSIVE VIEW table_dependency (pipeline_id TEXT, table_name TEXT, parent_table_name TEXT);
-- CREATE MATERIALIZED VIEW table_dependency AS
--     SELECT DISTINCT
--         rule.pipeline_id,
--         rule.table_name,
--         body_fact.table_name AS parent_table_name
--     FROM rule
--     JOIN body_fact
--         ON rule.pipeline_id = body_fact.pipeline_id
--         AND rule.rule_id = body_fact.rule_id

--     UNION

--     SELECT DISTINCT
--         t1.pipeline_id,
--         t1.table_name,
--         t2.parent_table_name
--     FROM table_dependency AS t1
--     JOIN table_dependency AS t2
--         ON t1.pipeline_id = t2.parent_table_name
--         AND t1.parent_table_name = t2.table_name;

/*
table_without_dependencies(pipeline_id:, table_name:) <-
    table_dependency(pipeline_id:, parent_table_name: table_name)
    not table_dependency(pipeline_id:, rule_id:, table_name:)
*/
CREATE MATERIALIZED VIEW table_without_dependencies AS
    SELECT DISTINCT
        table_dependency.pipeline_id,
        table_dependency.parent_table_name AS table_name
    FROM table_dependency
    WHERE NOT EXISTS (
        SELECT 1
        FROM table_dependency AS t
        WHERE t.pipeline_id = table_dependency.pipeline_id
        AND t.table_name = table_dependency.parent_table_name
    );

/*
table_output_order(pipeline_id:, table_name:, order: 0) <-
    schema_table(pipeline_id:, table_name:)
table_output_order(pipeline_id:, table_name:, order: 0) <-
    table_without_dependencies(pipeline_id:, table_name:)
table_output_order(pipeline_id:, table_name:, order: max<order>+1) <-
    table_dependency(pipeline_id:, table_name:, parent_table_name:)
    table_output_order(pipeline_id:, table_name: parent_table_name, order:)
    table_name != parent_table_name
*/
DECLARE RECURSIVE VIEW table_output_order (pipeline_id TEXT, table_name TEXT, "order" INTEGER);
CREATE MATERIALIZED VIEW table_output_order AS
    SELECT DISTINCT
        pipeline_id,
        table_name,
        0 AS "order"
    FROM schema_table

    UNION

    SELECT DISTINCT
        table_without_dependencies.pipeline_id,
        table_without_dependencies.table_name,
        0 AS "order"
    FROM table_without_dependencies

    UNION

    SELECT DISTINCT
        table_dependency.pipeline_id,
        table_dependency.table_name,
        MAX(table_output_order."order")+1 AS "order"
    FROM table_output_order
    JOIN table_dependency
        ON table_dependency.pipeline_id = table_output_order.pipeline_id
        AND table_dependency.parent_table_name = table_output_order.table_name
    WHERE table_dependency.parent_table_name != table_dependency.table_name
    GROUP BY table_dependency.pipeline_id, table_dependency.table_name;

/*
output_table_name(pipeline_id:, table_name:, output_table_name:) <-
    table_output_order(pipeline_id:, table_name:)
    output_table_name := table_name
    not table_name_prefix(pipeline_id:)
output_table_name(pipeline_id:, table_name:, output_table_name:) <-
    table_output_order(pipeline_id:, table_name:)
    table_name_prefix(pipeline_id:, prefix:)
    output_table_name := `${prefix}:${table_name}`
*/
CREATE MATERIALIZED VIEW output_table_name AS
    SELECT DISTINCT
        table_output_order.pipeline_id,
        table_output_order.table_name,
        table_output_order.table_name AS output_table_name
    FROM table_output_order
    WHERE NOT EXISTS (
        SELECT 1
        FROM table_name_prefix
        WHERE table_output_order.pipeline_id = table_name_prefix.pipeline_id
    )

    UNION

    SELECT DISTINCT
        table_output_order.pipeline_id,
        table_output_order.table_name,
        (table_name_prefix.prefix || ':' || table_output_order.table_name) AS output_table_name
    FROM table_output_order
    JOIN table_name_prefix
        ON table_output_order.pipeline_id = table_name_prefix.pipeline_id;

/*
first_table(pipeline_id:, table_name: min<table_name>, order:) <-
    table_output_order(pipeline_id:, table_name:, order:)
    #all_records_inserted(pipeline_id:)
*/
CREATE MATERIALIZED VIEW first_table AS
    SELECT DISTINCT
        table_output_order.pipeline_id,
        MIN(table_output_order.table_name) AS table_name,
        table_output_order."order"
    FROM table_output_order
    -- JOIN all_records_inserted
    --     ON table_output_order.pipeline_id = all_records_inserted.pipeline_id
    GROUP BY table_output_order.pipeline_id, table_output_order."order";

/*
only_one_table_in_group(pipeline_id:, order:) <-
    table_output_order(pipeline_id:, table_name:, order:)
    count<> = 1
*/
CREATE MATERIALIZED VIEW only_one_table_in_group AS
    SELECT DISTINCT
        table_output_order.pipeline_id,
        table_output_order."order"
    FROM table_output_order
    GROUP BY table_output_order.pipeline_id, table_output_order."order"
    HAVING COUNT(DISTINCT table_output_order.table_name) = 1;

/*
next_table_within_same_order(pipeline_id:, prev_table_name:, next_table_name:) <-
    table_output_order(pipeline_id:, table_name: prev_table_name, order:)
    table_output_order(pipeline_id:, table_name:, order:)
    next_table_name := min<table_name>
    prev_table_name < table_name
*/
CREATE MATERIALIZED VIEW next_table_within_same_order AS
    SELECT DISTINCT
        prev.pipeline_id,
        prev.table_name AS prev_table_name,
        MIN("next".table_name) AS next_table_name
    FROM table_output_order AS prev
    JOIN table_output_order AS "next"
        ON prev.pipeline_id = "next".pipeline_id
        AND prev."order" = "next"."order"
    WHERE prev.table_name < "next".table_name
    GROUP BY prev.pipeline_id, prev.table_name;

/*
next_table(pipeline_id:, prev_table_name:, next_table_name:) <-
    first_table(pipeline_id:, table_name: prev_table_name, order: 0)
    next_table_within_same_order(pipeline_id:, prev_table_name:, next_table_name:)
next_table(pipeline_id:, prev_table_name:, next_table_name:) <-
    first_table(pipeline_id:, table_name: prev_table_name, order: 0)
    first_table(pipeline_id:, table_name: next_table_name, order: 1)
    not next_table_within_same_order(pipeline_id:, prev_table_name:)
next_table(pipeline_id:, prev_table_name:, next_table_name:) <-
    next_table(pipeline_id:, next_table_name: prev_table_name)
    next_table_within_same_order(pipeline_id:, prev_table_name:, next_table_name:)
next_table(pipeline_id:, prev_table_name:, next_table_name:) <-
    next_table(pipeline_id:, next_table_name: prev_table_name)
    table_output_order(pipeline_id:, table_name: prev_table_name, order:)
    first_table(pipeline_id:, table_name: next_table_name, order: order+1)
    not next_table_within_same_order(pipeline_id:, prev_table_name:)
*/
DECLARE RECURSIVE VIEW next_table (pipeline_id TEXT, prev_table_name TEXT, next_table_name TEXT);
CREATE MATERIALIZED VIEW next_table AS
    SELECT DISTINCT
        first_table.pipeline_id,
        next_table_within_same_order.prev_table_name,
        next_table_within_same_order.next_table_name
    FROM first_table
    JOIN next_table_within_same_order
        ON first_table.pipeline_id = next_table_within_same_order.pipeline_id
        AND first_table.table_name = next_table_within_same_order.prev_table_name
    WHERE first_table."order" = 0
    
    UNION

    SELECT DISTINCT
        prev.pipeline_id,
        prev.table_name AS prev_table_name,
        "next".table_name AS next_table_name
    FROM first_table AS prev
    JOIN first_table AS "next"
        ON prev.pipeline_id = "next".pipeline_id
    WHERE prev."order" = 0 AND "next"."order" = 1
    AND NOT EXISTS (
        SELECT 1
        FROM next_table_within_same_order
        WHERE prev.pipeline_id = next_table_within_same_order.pipeline_id
        AND prev.table_name = next_table_within_same_order.prev_table_name
    )

    UNION

    SELECT DISTINCT
        next_table.pipeline_id,
        next_table_within_same_order.prev_table_name,
        next_table_within_same_order.next_table_name
    FROM next_table
    JOIN next_table_within_same_order
        ON next_table.pipeline_id = next_table_within_same_order.pipeline_id
        AND next_table.next_table_name = next_table_within_same_order.prev_table_name
    
    UNION

    SELECT DISTINCT
        next_table.pipeline_id,
        next_table.next_table_name AS prev_table_name,
        first_table.table_name AS next_table_name
    FROM next_table
    JOIN table_output_order
        ON next_table.pipeline_id = table_output_order.pipeline_id
        AND next_table.next_table_name = table_output_order.table_name
    JOIN first_table
        ON next_table.pipeline_id = first_table.pipeline_id
        AND table_output_order."order"+1 = first_table."order"
    WHERE NOT EXISTS (
        SELECT 1
        FROM next_table_within_same_order
        WHERE next_table.pipeline_id = next_table_within_same_order.pipeline_id
        AND next_table.next_table_name = next_table_within_same_order.prev_table_name
    );

/*
fact_alias(pipeline_id:, rule_id:, table_name:, alias:, negated:, fact_index:) <-
    body_fact(pipeline_id:, rule_id:, fact_id:, table_name:, negated:, index: fact_index)
    alias := `{{fact_id}}:{{table_name}}` 
*/
CREATE MATERIALIZED VIEW fact_alias AS
    SELECT DISTINCT
        body_fact.pipeline_id,
        body_fact.rule_id,
        body_fact.fact_id,
        body_fact."index" AS fact_index,
        body_fact.table_name,
        body_fact.negated,
        (body_fact.fact_id || ':' || body_fact.table_name) AS alias
    FROM body_fact;

/*
first_fact_alias(
    pipeline_id:, rule_id:,
    fact_id: argmin<fact_id, fact_index>,
    table_name: argmin<table_name, fact_index>,
    alias: argmin<alias, fact_index>,
    negated:,
    fact_index: min<fact_index>
) <-
    fact_alias(pipeline_id:, rule_id:, table_name:, alias:, negated:, fact_index:)
    #all_records_inserted(pipeline_id:)
*/
CREATE MATERIALIZED VIEW first_fact_alias AS
    SELECT DISTINCT
        fact_alias.pipeline_id,
        fact_alias.rule_id,
        ARG_MIN(fact_alias.fact_id, fact_alias.fact_index) AS fact_id,
        ARG_MIN(fact_alias.table_name, fact_alias.fact_index) AS table_name,
        ARG_MIN(fact_alias.alias, fact_alias.fact_index) AS alias,
        fact_alias.negated,
        MIN(fact_alias.fact_index) AS fact_index
    FROM fact_alias
    -- JOIN all_records_inserted
    --     ON fact_alias.pipeline_id = all_records_inserted.pipeline_id
    GROUP BY fact_alias.pipeline_id, fact_alias.rule_id, fact_alias.negated;

/*
adjacent_facts(
    pipeline_id:, rule_id:, negated:, prev_fact_id:, next_fact_id: argmin<next_fact_id, next_fact_index>
) <-
    fact_alias(pipeline_id:, rule_id:, fact_id: prev_fact_id, negated:, fact_index: prev_fact_index)
    fact_alias(pipeline_id:, rule_id:, fact_id: next_fact_id, negated:, fact_index: next_fact_index)
    prev_fact_index < next_fact_index
*/
CREATE MATERIALIZED VIEW adjacent_facts AS
    SELECT DISTINCT
        prev_fact.pipeline_id,
        prev_fact.rule_id,
        prev_fact.negated,
        prev_fact.fact_id AS prev_fact_id,
        ARG_MIN(next_fact.fact_id, next_fact.fact_index) AS next_fact_id
    FROM fact_alias AS prev_fact
    JOIN fact_alias AS next_fact
        ON prev_fact.pipeline_id = next_fact.pipeline_id
        AND prev_fact.rule_id = next_fact.rule_id
        AND prev_fact.negated = next_fact.negated
    WHERE prev_fact.fact_index < next_fact.fact_index
    GROUP BY prev_fact.pipeline_id, prev_fact.rule_id, prev_fact.negated, prev_fact.fact_id;

/*
last_fact_alias(pipeline_id:, rule_id:, fact_id:, negated:) <-
    adjacent_facts(pipeline_id:, rule_id:, negated:, next_fact_id: fact_id)
    not adjacent_facts(pipeline_id:, rule_id:, negated:, prev_fact_id: fact_id)
last_fact_alias(pipeline_id:, rule_id:, fact_id:, negated:) <-
    first_fact_alias(pipeline_id:, rule_id:, fact_id:, negated:)
    not adjacent_facts(pipeline_id:, rule_id:, negated:)
*/
CREATE MATERIALIZED VIEW last_fact_alias AS
    SELECT DISTINCT
        adjacent_facts.pipeline_id,
        adjacent_facts.rule_id,
        adjacent_facts.next_fact_id AS fact_id,
        adjacent_facts.negated
    FROM adjacent_facts
    WHERE NOT EXISTS (
        SELECT 1
        FROM adjacent_facts AS prev_adjacent_facts
        WHERE adjacent_facts.pipeline_id = prev_adjacent_facts.pipeline_id
        AND adjacent_facts.rule_id = prev_adjacent_facts.rule_id
        AND adjacent_facts.negated = prev_adjacent_facts.negated
        AND prev_adjacent_facts.prev_fact_id = adjacent_facts.next_fact_id
    )
    
    UNION
    
    SELECT DISTINCT
        first_fact_alias.pipeline_id,
        first_fact_alias.rule_id,
        first_fact_alias.fact_id,
        first_fact_alias.negated
    FROM first_fact_alias
    WHERE NOT EXISTS (
        SELECT 1
        FROM adjacent_facts
        WHERE first_fact_alias.pipeline_id = adjacent_facts.pipeline_id
        AND first_fact_alias.rule_id = adjacent_facts.rule_id
        AND first_fact_alias.negated = adjacent_facts.negated
    );

/*
constant_rule(pipeline_id:, rule_id:) <-
    rule_param(pipeline_id:, rule_id:)
    not fact_alias(pipeline_id:, rule_id:)
*/
CREATE MATERIALIZED VIEW constant_rule AS
    SELECT DISTINCT
        rule_param.pipeline_id,
        rule_param.rule_id
    FROM rule_param
    WHERE NOT EXISTS (
        SELECT 1
        FROM fact_alias
        WHERE rule_param.pipeline_id = fact_alias.pipeline_id
        AND rule_param.rule_id = fact_alias.rule_id
    );

/*
table_first_rule(pipeline_id:, table_name:, rule_id: min<rule_id>) <-
    rule(pipeline_id:, table_name:, rule_id:)
    #all_records_inserted(pipeline_id:)
*/
CREATE MATERIALIZED VIEW table_first_rule AS
    SELECT DISTINCT
        rule.pipeline_id,
        rule.table_name,
        MIN(rule.rule_id) AS rule_id
    FROM rule
    -- JOIN all_records_inserted
    --     ON rule.pipeline_id = all_records_inserted.pipeline_id
    GROUP BY rule.pipeline_id, rule.table_name;

/*
table_next_rule(pipeline_id:, table_name:, prev_rule_id:, next_rule_id:) <-
    table_first_rule(pipeline_id:, table_name:, rule_id: prev_rule_id)
    rule(pipeline_id:, table_name:, rule_id:)
    next_rule_id := min<rule_id>
    prev_rule_id < rule_id
table_next_rule(pipeline_id:, table_name:, prev_rule_id:, next_rule_id:) <-
    table_next_rule(pipeline_id:, table_name:, next_rule_id: prev_rule_id)
    rule(pipeline_id:, table_name:, rule_id:)
    next_rule_id := min<rule_id>
    prev_rule_id < rule_id
*/
DECLARE RECURSIVE VIEW table_next_rule (pipeline_id TEXT, table_name TEXT, prev_rule_id TEXT, next_rule_id TEXT);
CREATE MATERIALIZED VIEW table_next_rule AS
    SELECT DISTINCT
        table_first_rule.pipeline_id,
        table_first_rule.table_name,
        table_first_rule.rule_id AS prev_rule_id,
        MIN(rule.rule_id) AS next_rule_id
    FROM table_first_rule
    JOIN rule
        ON rule.pipeline_id = table_first_rule.pipeline_id
        AND rule.table_name = table_first_rule.table_name
    WHERE table_first_rule.rule_id < rule.rule_id
    GROUP BY table_first_rule.pipeline_id, table_first_rule.table_name, table_first_rule.rule_id

    UNION

    SELECT DISTINCT
        table_next_rule.pipeline_id,
        table_next_rule.table_name,
        table_next_rule.next_rule_id AS prev_rule_id,
        MIN(rule.rule_id) AS next_rule_id
    FROM table_next_rule
    JOIN rule
        ON rule.pipeline_id = table_next_rule.pipeline_id
        AND rule.table_name = table_next_rule.table_name
    WHERE table_next_rule.next_rule_id < rule.rule_id
    GROUP BY table_next_rule.pipeline_id, table_next_rule.table_name, table_next_rule.next_rule_id;

/*
table_last_rule(pipeline_id:, table_name:, rule_id:) <-
    table_first_rule(pipeline_id:, table_name:, rule_id:)
    not table_next_rule(pipeline_id:, table_name:)
table_last_rule(pipeline_id:, table_name:, rule_id:) <-
    table_next_rule(pipeline_id:, table_name:, next_rule_id: rule_id)
    not table_next_rule(pipeline_id:, table_name:, prev_rule_id: rule_id)
*/
CREATE MATERIALIZED VIEW table_last_rule AS
    SELECT DISTINCT
        table_first_rule.pipeline_id,
        table_first_rule.table_name,
        table_first_rule.rule_id
    FROM table_first_rule
    WHERE NOT EXISTS (
        SELECT 1
        FROM table_next_rule
        WHERE table_first_rule.pipeline_id = table_next_rule.pipeline_id
        AND table_first_rule.table_name = table_next_rule.table_name
    )

    UNION

    SELECT DISTINCT
        tn.pipeline_id,
        tn.table_name,
        tn.next_rule_id
    FROM table_next_rule AS tn
    WHERE NOT EXISTS (
        SELECT 1
        FROM table_next_rule
        WHERE tn.pipeline_id = table_next_rule.pipeline_id
        AND tn.table_name = table_next_rule.table_name
        AND tn.next_rule_id = table_next_rule.prev_rule_id
    );

/*
array_expr_length(pipeline_id:, rule_id:, expr_id:, length: count<>) <-
    array_expr(pipeline_id:, rule_id:, expr_id:, array_id:)
    array_entry(pipeline_id:, rule_id:, array_id:)
*/
CREATE MATERIALIZED VIEW array_expr_length AS
    SELECT DISTINCT
        array_expr.pipeline_id,
        array_expr.rule_id,
        array_expr.expr_id,
        COUNT(*) AS length
    FROM array_expr
    JOIN array_entry
        ON array_expr.pipeline_id = array_entry.pipeline_id
        AND array_expr.rule_id = array_entry.rule_id
        AND array_expr.array_id = array_entry.array_id
    GROUP BY array_expr.pipeline_id, array_expr.rule_id, array_expr.expr_id;

/*
sql_expr_template_part(pipeline_id:, rule_id:, expr_id:, part:, index:) <-
    sql_expr(pipeline_id:, rule_id:, expr_id:, template:)
    (element: part, index:) <- template
*/
CREATE MATERIALIZED VIEW sql_expr_template_part AS
    SELECT DISTINCT
        sql_expr.pipeline_id,
        sql_expr.rule_id,
        sql_expr.expr_id,
        t.part,
        t."index"
    FROM sql_expr
    CROSS JOIN UNNEST(sql_expr.template) WITH ORDINALITY AS t (part, "index");

-- /*
-- var_mentioned_in_sql_expr(pipeline_id:, rule_id:, expr_id:, var_name:) <-
--     sql_expr(pipeline_id:, rule_id:, expr_id:, template:)
--     sql_expr_template_part(pipeline_id:, rule_id:, expr_id:, part:)
--     part ~ "^{{[a-z_][a-zA-Z0-9_]*}}$"
--     var_name := part[2:-2]
-- */
-- CREATE MATERIALIZED VIEW var_mentioned_in_sql_expr AS
--     SELECT DISTINCT
--         sql_expr.pipeline_id,
--         sql_expr.rule_id,
--         sql_expr.expr_id,
--         SUBSTRING(t.part FROM 3 FOR (CHAR_LENGTH(t.part)-4)) AS var_name
--     FROM sql_expr
--     JOIN sql_expr_template_part AS t
--         ON sql_expr.pipeline_id = t.pipeline_id
--         AND sql_expr.rule_id = t.rule_id
--         AND sql_expr.expr_id = t.expr_id
--     WHERE t.part RLIKE '^\{\{[a-zA-Z_][A-Za-z0-9_:]*\}\}$';

/*
expr(pipeline_id:, rule_id:, expr_id:, expr_type: "fncall_expr") <-
    fncall_expr(pipeline_id:, rule_id:, expr_id:)
expr(pipeline_id:, rule_id:, expr_id:, expr_type: "int_expr") <-
    int_expr(pipeline_id:, rule_id:, expr_id:)
expr(pipeline_id:, rule_id:, expr_id:, expr_type: "str_expr") <-
    str_expr(pipeline_id:, rule_id:, expr_id:)
expr(pipeline_id:, rule_id:, expr_id:, expr_type: "var_expr") <-
    var_expr(pipeline_id:, rule_id:, expr_id:)
expr(pipeline_id:, rule_id:, expr_id:, expr_type: "array_expr") <-
    array_expr(pipeline_id:, rule_id:, expr_id:)
expr(pipeline_id:, rule_id:, expr_id:, expr_type: "dict_expr") <-
    dict_expr(pipeline_id:, rule_id:, expr_id:)
expr(pipeline_id:, rule_id:, expr_id:, expr_type: "null_expr") <-
    null_expr(pipeline_id:, rule_id:, expr_id:)
expr(pipeline_id:, rule_id:, expr_id:, expr_type: "binop_expr") <-
    binop_expr(pipeline_id:, rule_id:, expr_id:)
expr(pipeline_id:, rule_id:, expr_id:, expr_type: "sql_expr") <-
    sql_expr(pipeline_id:, rule_id:, expr_id:)
*/
CREATE MATERIALIZED VIEW expr AS
    SELECT DISTINCT
        fncall_expr.pipeline_id,
        fncall_expr.rule_id,
        fncall_expr.expr_id,
        'fncall_expr' AS expr_type
    FROM fncall_expr
    UNION
    SELECT DISTINCT
        int_expr.pipeline_id,
        int_expr.rule_id,
        int_expr.expr_id,
        'int_expr' AS expr_type
    FROM int_expr
    UNION
    SELECT DISTINCT
        str_expr.pipeline_id,
        str_expr.rule_id,
        str_expr.expr_id,
        'str_expr' AS expr_type
    FROM str_expr
    UNION
    SELECT DISTINCT
        var_expr.pipeline_id,
        var_expr.rule_id,
        var_expr.expr_id,
        'var_expr' AS expr_type
    FROM var_expr
    UNION
    SELECT DISTINCT
        array_expr.pipeline_id,
        array_expr.rule_id,
        array_expr.expr_id,
        'array_expr' AS expr_type
    FROM array_expr
    UNION
    SELECT DISTINCT
        dict_expr.pipeline_id,
        dict_expr.rule_id,
        dict_expr.expr_id,
        'dict_expr' AS expr_type
    FROM dict_expr
    UNION
    SELECT DISTINCT
        null_expr.pipeline_id,
        null_expr.rule_id,
        null_expr.expr_id,
        'null_expr' AS expr_type
    FROM null_expr
    UNION
    SELECT DISTINCT
        binop_expr.pipeline_id,
        binop_expr.rule_id,
        binop_expr.expr_id,
        'binop_expr' AS expr_type
    FROM binop_expr
    UNION
    SELECT DISTINCT
        sql_expr.pipeline_id,
        sql_expr.rule_id,
        sql_expr.expr_id,
        'sql_expr' AS expr_type
    FROM sql_expr;

/*
pattern_expr(pipeline_id:, rule_id:, expr_id:, expr_type:) <-
    fact_arg(pipeline_id:, rule_id:, expr_id:, expr_type:)
pattern_expr(pipeline_id:, rule_id:, expr_id:, expr_type:) <-
    body_match(
        pipeline_id:, rule_id:,
        left_expr_id: expr_id, left_expr_type: expr_type)
pattern_expr(pipeline_id:, rule_id:, expr_id:, expr_type:) <-
    pattern_expr(pipeline_id:, rule_id:, expr_id: array_expr_id, expr_type: "array_expr")
    array_expr(pipeline_id:, rule_id:, expr_id: array_expr_id, array_id:)
    array_entry(
        pipeline_id:, rule_id:, array_id:,
        expr_id:, expr_type:)
pattern_expr(pipeline_id:, rule_id:, expr_id:, expr_type:) <-
    pattern_expr(pipeline_id:, rule_id:, expr_id: dict_expr_id, expr_type: "dict_expr")
    dict_expr(pipeline_id:, rule_id:, expr_id: dict_expr_id, dict_id:)
    dict_entry(pipeline_id:, rule_id:, dict_id:, expr_id:, expr_type:)
*/
DECLARE RECURSIVE VIEW pattern_expr (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, expr_type TEXT);
CREATE MATERIALIZED VIEW pattern_expr AS
    SELECT DISTINCT
        fact_arg.pipeline_id,
        fact_arg.rule_id,
        fact_arg.expr_id,
        fact_arg.expr_type
    FROM fact_arg

    UNION

    SELECT DISTINCT
        body_match.pipeline_id,
        body_match.rule_id,
        body_match.left_expr_id AS expr_id,
        body_match.left_expr_type AS expr_type
    FROM body_match

    UNION

    SELECT DISTINCT
        array_entry.pipeline_id,
        array_entry.rule_id,
        array_entry.expr_id,
        array_entry.expr_type
    FROM pattern_expr
    JOIN array_expr
        ON pattern_expr.pipeline_id = array_expr.pipeline_id
        AND pattern_expr.rule_id = array_expr.rule_id
        AND pattern_expr.expr_id = array_expr.expr_id
    JOIN array_entry
        ON array_expr.pipeline_id = array_entry.pipeline_id
        AND array_expr.rule_id = array_entry.rule_id
        AND array_expr.array_id = array_entry.array_id
    WHERE pattern_expr.expr_type = 'array_expr'

    UNION

    SELECT DISTINCT
        dict_entry.pipeline_id,
        dict_entry.rule_id,
        dict_entry.expr_id,
        dict_entry.expr_type
    FROM pattern_expr
    JOIN dict_expr
        ON pattern_expr.pipeline_id = dict_expr.pipeline_id
        AND pattern_expr.rule_id = dict_expr.rule_id
        AND pattern_expr.expr_id = dict_expr.expr_id
    JOIN dict_entry
        ON dict_expr.pipeline_id = dict_entry.pipeline_id
        AND dict_expr.rule_id = dict_entry.rule_id
        AND dict_expr.dict_id = dict_entry.dict_id
    WHERE pattern_expr.expr_type = 'dict_expr';

/*
value_expr(pipeline_id:, rule_id:, expr_id:, expr_type:) <-
    rule_param(pipeline_id:, rule_id:, expr_id:, expr_type:)
value_expr(pipeline_id:, rule_id:, expr_id:, expr_type:) <-
    body_match(
        pipeline_id:, rule_id:,
        right_expr_id: expr_id, right_expr_type: expr_type)
value_expr(pipeline_id:, rule_id:, expr_id:, expr_type:) <-
    body_expr(pipeline_id:, rule_id:, expr_id:, expr_type:)
value_expr(pipeline_id:, rule_id:, expr_id:, expr_type:) <-
    value_expr(pipeline_id:, rule_id:, expr_id: array_expr_id, expr_type: "array_expr")
    array_expr(pipeline_id:, rule_id:, expr_id: array_expr_id, array_id:)
    array_entry(
        pipeline_id:, rule_id:, array_id:,
        expr_id:, expr_type:)
value_expr(pipeline_id:, rule_id:, expr_id:, expr_type:) <-
    value_expr(pipeline_id:, rule_id:, expr_id: dict_expr_id, expr_type: "dict_expr")
    dict_expr(pipeline_id:, rule_id:, expr_id: dict_expr_id, dict_id:)
    dict_entry(pipeline_id:, rule_id:, dict_id:, expr_id:, expr_type:)
value_expr(pipeline_id:, rule_id:, expr_id:, expr_type:) <-
    value_expr(pipeline_id:, rule_id:, expr_id: fncall_expr_id, expr_type: "fncall_expr")
    fncall_expr(pipeline_id:, rule_id:, expr_id: fncall_expr_id, fncall_id:)
    fn_val_arg(pipeline_id:, rule_id:, fncall_id:, expr_id:, expr_type:)
value_expr(pipeline_id:, rule_id:, expr_id:, expr_type:) <-
    value_expr(pipeline_id:, rule_id:, expr_id: sql_expr_id, expr_type: "fncall_expr")
    fncall_expr(pipeline_id:, rule_id:, expr_id: fncall_expr_id, fncall_id:)
    fn_kv_arg(pipeline_id:, rule_id:, fncall_id:, expr_id:, expr_type:)
value_expr(pipeline_id:, rule_id:, expr_id:, expr_type:) <-
    value_expr(pipeline_id:, rule_id:, expr_id: sql_expr_id, expr_type: "binop_expr")
    binop_expr(pipeline_id:, rule_id:, left_expr_id: expr_id, left_expr_type: expr_type)
value_expr(pipeline_id:, rule_id:, expr_id:, expr_type:) <-
    value_expr(pipeline_id:, rule_id:, expr_id: sql_expr_id, expr_type: "binop_expr")
    binop_expr(pipeline_id:, rule_id:, right_expr_id: expr_id, right_expr_type: expr_type)
*/
DECLARE RECURSIVE VIEW value_expr(pipeline_id TEXT, rule_id TEXT, expr_id TEXT, expr_type TEXT);
CREATE MATERIALIZED VIEW value_expr AS
    SELECT DISTINCT
        rule_param.pipeline_id,
        rule_param.rule_id,
        rule_param.expr_id,
        rule_param.expr_type
    FROM rule_param

    UNION

    SELECT DISTINCT
        body_match.pipeline_id,
        body_match.rule_id,
        body_match.right_expr_id AS expr_id,
        body_match.right_expr_type AS expr_type
    FROM body_match
    UNION
    SELECT DISTINCT
        body_expr.pipeline_id,
        body_expr.rule_id,
        body_expr.expr_id,
        body_expr.expr_type
    FROM body_expr

    UNION

    SELECT DISTINCT
        array_expr.pipeline_id,
        array_expr.rule_id,
        array_entry.expr_id,
        array_entry.expr_type
    FROM value_expr
    JOIN array_expr
        ON value_expr.pipeline_id = array_expr.pipeline_id
        AND value_expr.rule_id = array_expr.rule_id
        AND value_expr.expr_id = array_expr.expr_id
    JOIN array_entry
        ON array_expr.pipeline_id = array_entry.pipeline_id
        AND array_expr.rule_id = array_entry.rule_id
        AND array_expr.array_id = array_entry.array_id
    WHERE value_expr.expr_type = 'array_expr'

    UNION

    SELECT DISTINCT
        dict_expr.pipeline_id,
        dict_expr.rule_id,
        dict_entry.expr_id,
        dict_entry.expr_type
    FROM value_expr
    JOIN dict_expr
        ON value_expr.pipeline_id = dict_expr.pipeline_id
        AND value_expr.rule_id = dict_expr.rule_id
        AND value_expr.expr_id = dict_expr.expr_id
    JOIN dict_entry
        ON dict_expr.pipeline_id = dict_entry.pipeline_id
        AND dict_expr.rule_id = dict_entry.rule_id
        AND dict_expr.dict_id = dict_entry.dict_id
    WHERE value_expr.expr_type = 'dict_expr'

    UNION

    SELECT DISTINCT
        value_expr.pipeline_id,
        value_expr.rule_id,
        fn_val_arg.expr_id,
        fn_val_arg.expr_type
    FROM value_expr
    JOIN fncall_expr
        ON value_expr.pipeline_id = fncall_expr.pipeline_id
        AND value_expr.rule_id = fncall_expr.rule_id
        AND value_expr.expr_id = fncall_expr.expr_id
    JOIN fn_val_arg
        ON fncall_expr.pipeline_id = fn_val_arg.pipeline_id
        AND fncall_expr.rule_id = fn_val_arg.rule_id
        AND fncall_expr.fncall_id = fn_val_arg.fncall_id
    WHERE value_expr.expr_type = 'fncall_expr'

    UNION

    SELECT DISTINCT
        value_expr.pipeline_id,
        value_expr.rule_id,
        fn_kv_arg.expr_id,
        fn_kv_arg.expr_type
    FROM value_expr
    JOIN fncall_expr
        ON value_expr.pipeline_id = fncall_expr.pipeline_id
        AND value_expr.rule_id = fncall_expr.rule_id
        AND value_expr.expr_id = fncall_expr.expr_id
    JOIN fn_kv_arg
        ON fncall_expr.pipeline_id = fn_kv_arg.pipeline_id
        AND fncall_expr.rule_id = fn_kv_arg.rule_id
        AND fncall_expr.fncall_id = fn_kv_arg.fncall_id
    WHERE value_expr.expr_type = 'fncall_expr'

    UNION

    SELECT DISTINCT
        value_expr.pipeline_id,
        value_expr.rule_id,
        binop_expr.left_expr_id AS expr_id,
        binop_expr.left_expr_type AS expr_type
    FROM value_expr
    JOIN binop_expr
        ON value_expr.pipeline_id = binop_expr.pipeline_id
        AND value_expr.rule_id = binop_expr.rule_id
        AND value_expr.expr_id = binop_expr.expr_id

    UNION

    SELECT DISTINCT
        value_expr.pipeline_id,
        value_expr.rule_id,
        binop_expr.right_expr_id AS expr_id,
        binop_expr.right_expr_type AS expr_type
    FROM value_expr
    JOIN binop_expr
        ON value_expr.pipeline_id = binop_expr.pipeline_id
        AND value_expr.rule_id = binop_expr.rule_id
        AND value_expr.expr_id = binop_expr.expr_id;

/*
asterisk_array_var_offsets(
    pipeline_id:, rule_id:, array_id:, expr_id:,
    left_offset:, right_offset:, var_name:,
) <-
    array_expr(pipeline_id:, rule_id:, expr_id: array_expr_id, array_id:)
    array_entry(pipeline_id:, rule_id:, array_id:, index:, expr_id:, expr_type: "var_expr")
    var_expr(pipeline_id:, rule_id:, expr_id:, var_name:, special_prefix: "*")
    array_expr_last_index(pipeline_id:, rule_id:, expr_id: array_expr_id, index: last_index)
    left_offset := index - 1
    right_offset := last_index - index
*/
CREATE MATERIALIZED VIEW asterisk_array_var_offsets AS
    SELECT DISTINCT
        array_expr.pipeline_id,
        array_expr.rule_id,
        array_expr.array_id,
        array_entry.expr_id,
        (array_entry."index" - 1) AS left_offset,
        (array_expr_last_index."index" - array_entry."index") AS right_offset,
        var_expr.var_name
    FROM array_expr
    JOIN array_entry
        ON array_expr.pipeline_id = array_entry.pipeline_id
        AND array_expr.rule_id = array_entry.rule_id
        AND array_expr.array_id = array_entry.array_id
    JOIN var_expr
        ON array_entry.pipeline_id = var_expr.pipeline_id
        AND array_entry.rule_id = var_expr.rule_id
        AND array_entry.expr_id = var_expr.expr_id
    JOIN array_expr_last_index
        ON array_expr.pipeline_id = array_expr_last_index.pipeline_id
        AND array_expr.rule_id = array_expr_last_index.rule_id
        AND array_expr.expr_id = array_expr_last_index.expr_id
    WHERE var_expr.special_prefix = '*';

/*
fact_oexpr(
    pipeline_id:, rule_id:, pattern_expr_id:, pattern_expr_type:,
    sql:, negated:, fact_index:, fact_id:,
) <-
    fact_alias(pipeline_id:, rule_id:, fact_id:, negated:, fact_index:)
    fact_arg(pipeline_id:, rule_id:, fact_id:, expr_id: pattern_expr_id, expr_type: pattern_expr_type)
    sql := `"{{alias}}"."{{key}}"`
fact_oexpr(
    pipeline_id:, rule_id:, pattern_expr_id:, pattern_expr_type:,
    sql:, negated:, fact_index:, fact_id:,
) <-
    fact_oexpr(
        pipeline_id:, rule_id:, pattern_expr_id: array_expr_id, pattern_expr_type: "array_expr",
        negated:, fact_index:, fact_id:)
    array_expr(pipeline_id:, rule_id:, expr_id: array_expr_id, array_id:)
    array_entry(pipeline_id:, rule_id:, array_id:, index:, expr_id: pattern_expr_id, expr_type: pattern_expr_type)
    not var_expr(pipeline_id:, rule_id:, expr_id: pattern_expr_id, special_prefix: "*")
    sql := `{{parent_sql}}[{{index}}]`
fact_oexpr(
    pipeline_id:, rule_id:, pattern_expr_id:, pattern_expr_type:,
    sql:, negated:, fact_index:, fact_id:,
) <-
    fact_oexpr(
        pipeline_id:, rule_id:, pattern_expr_id: dict_expr_id, pattern_expr_type: "dict_expr",
        negated:, fact_index:, fact_id:)
    dict_expr(pipeline_id:, rule_id:, expr_id: dict_expr_id, dict_id:)
    dict_entry(pipeline_id:, rule_id:, dict_id:, key:, expr_id: pattern_expr_id, expr_type: pattern_expr_type)
    sql := `{{parent_sql}}['{{key}}']`
*/
DECLARE RECURSIVE VIEW fact_oexpr (pipeline_id TEXT, rule_id TEXT, pattern_expr_id TEXT, pattern_expr_type TEXT, sql TEXT, negated BOOLEAN, fact_index INTEGER, fact_id TEXT);
CREATE MATERIALIZED VIEW fact_oexpr AS
    SELECT DISTINCT
        fact_alias.pipeline_id,
        fact_alias.rule_id,
        fact_arg.expr_id AS pattern_expr_id,
        fact_arg.expr_type AS pattern_expr_type,
        ('"' || fact_alias.alias || '"."' || fact_arg.key || '"') AS sql,
        fact_alias.negated,
        fact_alias.fact_index,
        fact_alias.fact_id
    FROM fact_alias
    JOIN fact_arg
        ON fact_alias.pipeline_id = fact_arg.pipeline_id
        AND fact_alias.rule_id = fact_arg.rule_id
        AND fact_alias.fact_id = fact_arg.fact_id

    UNION

    SELECT DISTINCT
        fact_oexpr.pipeline_id,
        fact_oexpr.rule_id,
        array_entry.expr_id AS pattern_expr_id,
        array_entry.expr_type AS pattern_expr_type,
        (fact_oexpr.sql || '[' || array_entry."index" || ']') AS sql,
        fact_oexpr.negated,
        fact_oexpr.fact_index,
        fact_oexpr.fact_id
    FROM fact_oexpr
    JOIN array_expr
        ON fact_oexpr.pipeline_id = array_expr.pipeline_id
        AND fact_oexpr.rule_id = array_expr.rule_id
        AND fact_oexpr.pattern_expr_id = array_expr.expr_id
    JOIN array_entry
        ON array_expr.pipeline_id = array_entry.pipeline_id
        AND array_expr.rule_id = array_entry.rule_id
        AND array_expr.array_id = array_entry.array_id
    WHERE fact_oexpr.pattern_expr_type = 'array_expr'
    AND NOT EXISTS (
        SELECT *
        FROM var_expr
        WHERE array_entry.pipeline_id = var_expr.pipeline_id
        AND array_entry.rule_id = var_expr.rule_id
        AND array_entry.expr_id = var_expr.expr_id
        AND var_expr.special_prefix = '*'
    )

    UNION

    SELECT DISTINCT
        fact_oexpr.pipeline_id,
        fact_oexpr.rule_id,
        dict_entry.expr_id AS pattern_expr_id,
        dict_entry.expr_type AS pattern_expr_type,
        (fact_oexpr.sql || '[' || '''' || dict_entry.key || '''' || ']') AS sql,
        fact_oexpr.negated,
        fact_oexpr.fact_index,
        fact_oexpr.fact_id
    FROM fact_oexpr
    JOIN dict_expr
        ON fact_oexpr.pipeline_id = dict_expr.pipeline_id
        AND fact_oexpr.rule_id = dict_expr.rule_id
        AND fact_oexpr.pattern_expr_id = dict_expr.expr_id
    JOIN dict_entry
        ON dict_expr.pipeline_id = dict_entry.pipeline_id
        AND dict_expr.rule_id = dict_entry.rule_id
        AND dict_expr.dict_id = dict_entry.dict_id
    WHERE fact_oexpr.pattern_expr_type = 'dict_expr';

/*
fact_oexpr_array_drop_sides(
    pipeline_id:, rule_id:, var_pattern_expr_id:,
    negated:, fact_index:, fact_id:, var_name:,
    array_sql:, left_offset:, right_offset:,
) <-
    fact_oexpr(
        pipeline_id:, rule_id:, pattern_expr_id:, pattern_expr_type: "array_expr",
        negated:, fact_index:, fact_id:, sql: array_sql)
    array_expr(pipeline_id:, rule_id:, expr_id: pattern_expr_id, array_id:)
    asterisk_array_var_offsets(
        pipeline_id:, rule_id:, array_id:, var_name:,
        expr_id: var_pattern_expr_id, left_offset:, right_offset:)
*/
CREATE MATERIALIZED VIEW fact_oexpr_array_drop_sides AS
    SELECT DISTINCT
        fact_oexpr.pipeline_id,
        fact_oexpr.rule_id,
        asterisk_array_var_offsets.expr_id AS var_pattern_expr_id,
        fact_oexpr.negated,
        fact_oexpr.fact_index,
        fact_oexpr.fact_id,
        asterisk_array_var_offsets.var_name,
        fact_oexpr.sql AS array_sql,
        asterisk_array_var_offsets.left_offset,
        asterisk_array_var_offsets.right_offset
    FROM fact_oexpr
    JOIN array_expr
        ON fact_oexpr.pipeline_id = array_expr.pipeline_id
        AND fact_oexpr.rule_id = array_expr.rule_id
        AND fact_oexpr.pattern_expr_id = array_expr.expr_id
    JOIN asterisk_array_var_offsets
        ON array_expr.pipeline_id = asterisk_array_var_offsets.pipeline_id
        AND array_expr.rule_id = asterisk_array_var_offsets.rule_id
        AND array_expr.array_id = asterisk_array_var_offsets.array_id
    WHERE fact_oexpr.pattern_expr_type = 'array_expr';

/*
var_bound_in_fact(
    pipeline_id:, rule_id:, negated:, var_name:, fact_index:, fact_id:, sql:
) <-
    fact_oexpr(
        pipeline_id:, rule_id:, pattern_expr_id:, pattern_expr_type: "var_expr",
        sql:, negated: false, fact_index:, fact_id:)
    var_expr(pipeline_id:, rule_id:, expr_id: pattern_expr_id, var_name:)
    false = (var_name ~ "^_.*$")
var_bound_in_fact(
    pipeline_id:, rule_id:, negated:, var_name:, fact_index:, fact_id:, sql:
) <-
    fact_oexpr_array_drop_sides(
        pipeline_id:, rule_id:, var_pattern_expr_id:,
        negated: false, fact_index:, fact_id:,
        array_sql:, left_offset:, right_offset:)
    var_expr(pipeline_id:, rule_id:, expr_id: var_pattern_expr_id, var_name:)
    false = (var_name ~ "^_.*$")
    sql := `GRASP_VARIANT_ARRAY_DROP_SIDES(CAST({{array_sql}} AS VARIANT ARRAY), CAST({{left_offset}} AS INTEGER UNSIGNED), CAST({{right_offset}} AS INTEGER UNSIGNED))`
*/
CREATE MATERIALIZED VIEW var_bound_in_fact AS
    SELECT DISTINCT
        fact_oexpr.pipeline_id,
        fact_oexpr.rule_id,
        fact_oexpr.negated,
        var_expr.var_name,
        fact_oexpr.fact_index,
        fact_oexpr.fact_id,
        fact_oexpr.sql
    FROM fact_oexpr
    JOIN var_expr
        ON fact_oexpr.pipeline_id = var_expr.pipeline_id
        AND fact_oexpr.rule_id = var_expr.rule_id
        AND fact_oexpr.pattern_expr_id = var_expr.expr_id
    WHERE NOT var_expr.var_name RLIKE '^_.*$'
    AND NOT fact_oexpr.negated

    UNION

    SELECT DISTINCT
        fact_oexpr_array_drop_sides.pipeline_id,
        fact_oexpr_array_drop_sides.rule_id,
        fact_oexpr_array_drop_sides.negated,
        var_expr.var_name,
        fact_oexpr_array_drop_sides.fact_index,
        fact_oexpr_array_drop_sides.fact_id,
        ('GRASP_VARIANT_ARRAY_DROP_SIDES(CAST(' || fact_oexpr_array_drop_sides.array_sql || ' AS VARIANT ARRAY), CAST(' || fact_oexpr_array_drop_sides.left_offset || ' AS INTEGER UNSIGNED), CAST(' || fact_oexpr_array_drop_sides.right_offset || ' AS INTEGER UNSIGNED))') AS sql
    FROM fact_oexpr_array_drop_sides
    JOIN var_expr
        ON fact_oexpr_array_drop_sides.pipeline_id = var_expr.pipeline_id
        AND fact_oexpr_array_drop_sides.rule_id = var_expr.rule_id
        AND fact_oexpr_array_drop_sides.var_pattern_expr_id = var_expr.expr_id
    WHERE NOT var_expr.var_name RLIKE '^_.*$'
    AND NOT fact_oexpr_array_drop_sides.negated;

/*
canonical_fact_var_sql(
    pipeline_id:, rule_id:, var_name:,
    sql: argmin<sql, fact_index>, fact_index: min<fact_index>
) <-
    var_bound_in_fact(pipeline_id:, rule_id:, var_name:, negated: false, sql:, fact_index:)
    #all_records_inserted(pipeline_id:)
*/
CREATE MATERIALIZED VIEW canonical_fact_var_sql AS
    SELECT DISTINCT
        var_bound_in_fact.pipeline_id,
        var_bound_in_fact.rule_id,
        var_bound_in_fact.var_name,
        MIN(var_bound_in_fact.fact_index) AS fact_index,
        ARG_MIN(var_bound_in_fact.sql, var_bound_in_fact.fact_index) AS sql
    FROM var_bound_in_fact
    -- JOIN all_records_inserted
    --     ON var_bound_in_fact.pipeline_id = all_records_inserted.pipeline_id
    WHERE NOT var_bound_in_fact.negated
    GROUP BY var_bound_in_fact.pipeline_id, var_bound_in_fact.rule_id, var_bound_in_fact.var_name;
