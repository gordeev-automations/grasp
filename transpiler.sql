CREATE TABLE schema_table (
    pipeline_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    "materialized" BOOLEAN NOT NULL,
    has_computed_id BOOLEAN NOT NULL,
    has_tenant BOOLEAN NOT NULL,
    read_only BOOLEAN NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE schema_table_column (
    pipeline_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    data_type TEXT NOT NULL,
    nullable BOOLEAN NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE schema_table_archive_from_file (
    pipeline_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    filename TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE schema_table_archive_pg (
    pipeline_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    pg_url TEXT NOT NULL,
    pg_query TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE rule (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    "materialized" BOOLEAN NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE rule_param (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    "key" TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    expr_type TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE aggr_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    fn_name TEXT NOT NULL,
    arg_var TEXT
) WITH ('materialized' = 'true');

CREATE TABLE int_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    value BIGINT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE str_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    value TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE var_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    var_name TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE sql_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    template TEXT ARRAY NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE dict_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    dict_id TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE dict_entry (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    dict_id TEXT NOT NULL,
    key TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    expr_type TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE array_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    array_id TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE array_entry (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    array_id TEXT NOT NULL,
    "index" INTEGER NOT NULL,
    expr_id TEXT NOT NULL,
    expr_type TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE body_goal (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    goal_id TEXT NOT NULL,
    "index" INTEGER NOT NULL,
    table_name TEXT NOT NULL,
    negated BOOLEAN NOT NULL,
    id_var TEXT
) WITH ('materialized' = 'true');

CREATE TABLE goal_arg (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    goal_id TEXT NOT NULL,
    "key" TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    expr_type TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE body_match (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    match_id TEXT NOT NULL,
    left_expr_id TEXT NOT NULL,
    left_expr_type TEXT NOT NULL,
    right_expr_id TEXT NOT NULL,
    right_expr_type TEXT NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE body_sql_cond (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    cond_id TEXT NOT NULL,
    sql_expr_id TEXT NOT NULL
) WITH ('materialized' = 'true');

/*
table_dependency(pipeline_id:, table_name:, parent_table_name:) <-
    rule(pipeline_id:, table_name:, rule_id:)
    body_goal(pipeline_id:, rule_id:, table_name: parent_table_name)
table_dependency(pipeline_id:, table_name:, parent_table_name:) <-
    table_dependency(pipeline_id:, table_name:, parent_table_name:)
    table_dependency(pipeline_id:, table_name: parent_table_name, parent_table_name:)
*/
DECLARE RECURSIVE VIEW table_dependency (pipeline_id TEXT, table_name TEXT, parent_table_name TEXT);
CREATE MATERIALIZED VIEW table_dependency AS
    SELECT DISTINCT
        rule.pipeline_id,
        rule.table_name,
        body_goal.table_name AS parent_table_name
    FROM rule
    JOIN body_goal
        ON rule.pipeline_id = body_goal.pipeline_id
        AND rule.rule_id = body_goal.rule_id
    
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
table_output_order(pipeline_id:, table_name:, order: 0) <-
    schema_table(pipeline_id:, table_name:)
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
goal_alias(pipeline_id:, rule_id:, table_name:, alias:, negated:, goal_index:) <-
    body_goal(pipeline_id:, rule_id:, goal_id:, table_name:, negated:, index: goal_index)
    alias := `{{goal_id}}:{{table_name}}` 
*/
CREATE MATERIALIZED VIEW goal_alias AS
    SELECT DISTINCT
        body_goal.pipeline_id,
        body_goal.rule_id,
        body_goal.goal_id,
        body_goal."index" AS goal_index,
        body_goal.table_name,
        body_goal.negated,
        (body_goal.goal_id || ':' || body_goal.table_name) AS alias
    FROM body_goal;

/*
first_goal_alias(
    pipeline_id:, rule_id:, table_name: argmin<table_name, goal_index>, alias: argmin<alias, goal_index>,
    negated:, goal_index: min<goal_index>
) <-
    goal_alias(pipeline_id:, rule_id:, table_name:, alias:, negated:, goal_index:)
*/
CREATE MATERIALIZED VIEW first_goal_alias AS
    SELECT DISTINCT
        goal_alias.pipeline_id,
        goal_alias.rule_id,
        ARG_MIN(goal_alias.goal_id, goal_alias.goal_index) AS goal_id,
        ARG_MIN(goal_alias.table_name, goal_alias.goal_index) AS table_name,
        ARG_MIN(goal_alias.alias, goal_alias.goal_index) AS alias,
        goal_alias.negated,
        MIN(goal_alias.goal_index) AS goal_index
    FROM goal_alias
    GROUP BY goal_alias.pipeline_id, goal_alias.rule_id, goal_alias.negated;

/*
adjacent_goals(
    pipeline_id:, rule_id:, negated:, prev_goal_id:, next_goal_id: argmin<next_goal_id, next_goal_index>
) <-
    goal_alias(pipeline_id:, rule_id:, goal_id: prev_goal_id, negated:, goal_index: prev_goal_index)
    goal_alias(pipeline_id:, rule_id:, goal_id: next_goal_id, negated:, goal_index: next_goal_index)
    prev_goal_index < next_goal_index
*/
CREATE MATERIALIZED VIEW adjacent_goals AS
    SELECT DISTINCT
        prev_goal.pipeline_id,
        prev_goal.rule_id,
        prev_goal.negated,
        prev_goal.goal_id AS prev_goal_id,
        ARG_MIN(next_goal.goal_id, next_goal.goal_index) AS next_goal_id
    FROM goal_alias AS prev_goal
    JOIN goal_alias AS next_goal
        ON prev_goal.pipeline_id = next_goal.pipeline_id
        AND prev_goal.rule_id = next_goal.rule_id
        AND prev_goal.negated = next_goal.negated
    WHERE prev_goal.goal_index < next_goal.goal_index
    GROUP BY prev_goal.pipeline_id, prev_goal.rule_id, prev_goal.negated, prev_goal.goal_id;

/*
table_first_rule(pipeline_id:, table_name:, rule_id: min<rule_id>) <-
    rule(pipeline_id:, table_name:, rule_id:)
*/
CREATE MATERIALIZED VIEW table_first_rule AS
    SELECT DISTINCT
        rule.pipeline_id,
        rule.table_name,
        MIN(rule.rule_id) AS rule_id
    FROM rule
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
    (part, index) <- unnest(template)
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

/*
var_mentioned_in_sql_expr(pipeline_id:, rule_id:, expr_id:, var_name:) <-
    sql_expr(pipeline_id:, rule_id:, expr_id:, template:)
    sql_expr_template_part(pipeline_id:, rule_id:, expr_id:, part:)
    part ~ "{{[a-z_][a-zA-Z0-9_]*}}"
    var_name := part[2:-2]
*/
CREATE MATERIALIZED VIEW var_mentioned_in_sql_expr AS
    SELECT DISTINCT
        sql_expr.pipeline_id,
        sql_expr.rule_id,
        sql_expr.expr_id,
        SUBSTRING(t.part FROM 3 FOR (CHAR_LENGTH(t.part)-4)) AS var_name
    FROM sql_expr
    JOIN sql_expr_template_part AS t
        ON sql_expr.pipeline_id = t.pipeline_id
        AND sql_expr.rule_id = t.rule_id
        AND sql_expr.expr_id = t.expr_id
    WHERE t.part RLIKE '^\{\{[a-zA-Z_][A-Za-z0-9_:]*\}\}$';

/*
var_mentioned_in_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, var_name:, access_prefix: "") <-
    var_expr(pipeline_id:, rule_id:, expr_id:, var_name:)
    expr_type := "var_expr"
var_mentioned_in_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, var_name:, access_prefix: "") <-
    aggr_expr(pipeline_id:, rule_id:, expr_id:, arg_var: var_name)
    expr_type := 'aggr_expr'
var_mentioned_in_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, var_name:, access_prefix: "") <-
    var_mentioned_in_sql_expr(pipeline_id:, rule_id:, expr_id:, var_name:)
    expr_type := 'sql_expr'
var_mentioned_in_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "array_expr", var_name:, access_prefix:) <-
    array_expr(pipeline_id:, rule_id:, expr_id:, array_id:)
    array_entry(pipeline_id:, rule_id:, array_id:, expr_id: entry_expr_id, expr_type: entry_expr_type, index:)
    var_mentioned_in_expr(
        pipeline_id:, rule_id:, expr_id: entry_expr_id, expr_type: entry_expr_type,
        var_name:, access_prefix: prev_access_prefix)
    access_prefix := `[{{index}}]{{prev_access_prefix}}`
var_mentioned_in_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "dict_expr", var_name:, access_prefix:) <-
    dict_expr(pipeline_id:, rule_id:, expr_id:, dict_id:)
    dict_entry(pipeline_id:, rule_id:, dict_id:, key:, expr_id: entry_expr_id, expr_type: entry_expr_type)
    var_mentioned_in_expr(
        pipeline_id:, rule_id:, expr_id: entry_expr_id, expr_type: entry_expr_type,
        var_name:, access_prefix: prev_access_prefix)
    access_prefix := `['{{key}}']{{prev_access_prefix}}`
*/
DECLARE RECURSIVE VIEW var_mentioned_in_expr (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, expr_type TEXT, var_name TEXT, access_prefix TEXT);
CREATE MATERIALIZED VIEW var_mentioned_in_expr AS
    SELECT DISTINCT
        var_expr.pipeline_id,
        var_expr.rule_id,
        var_expr.expr_id,
        'var_expr' AS expr_type,
        var_expr.var_name,
        '' AS access_prefix
    FROM var_expr

    UNION

    SELECT DISTINCT
        aggr_expr.pipeline_id,
        aggr_expr.rule_id,
        aggr_expr.expr_id,
        'aggr_expr' AS expr_type,
        aggr_expr.arg_var AS var_name,
        '' AS access_prefix
    FROM aggr_expr

    UNION

    SELECT DISTINCT
        var_mentioned_in_sql_expr.pipeline_id,
        var_mentioned_in_sql_expr.rule_id,
        var_mentioned_in_sql_expr.expr_id,
        'sql_expr' AS expr_type,
        var_mentioned_in_sql_expr.var_name,
        '' AS access_prefix
    FROM var_mentioned_in_sql_expr

    UNION

    SELECT DISTINCT
        array_expr.pipeline_id,
        array_expr.rule_id,
        array_expr.expr_id,
        CAST('array_expr' AS TEXT) AS expr_type,
        var_mentioned_in_expr.var_name,
        ('[' || array_entry."index" || ']' || var_mentioned_in_expr.access_prefix) AS access_prefix
    FROM array_expr
    JOIN array_entry
        ON array_expr.pipeline_id = array_entry.pipeline_id
        AND array_expr.rule_id = array_entry.rule_id
        AND array_entry.array_id = array_expr.array_id
    JOIN var_mentioned_in_expr
        ON array_expr.pipeline_id = var_mentioned_in_expr.pipeline_id
        AND array_expr.rule_id = var_mentioned_in_expr.rule_id
        AND array_entry.expr_id = var_mentioned_in_expr.expr_id
        AND array_entry.expr_type = var_mentioned_in_expr.expr_type

    UNION

    SELECT DISTINCT
        dict_expr.pipeline_id,
        dict_expr.rule_id,
        dict_expr.expr_id,
        'dict_expr' AS expr_type,
        var_mentioned_in_expr.var_name,
        ('[''' || dict_entry.key || ''']' || var_mentioned_in_expr.access_prefix) AS access_prefix
    FROM dict_expr
    JOIN dict_entry
        ON dict_expr.pipeline_id = dict_entry.pipeline_id
        AND dict_expr.rule_id = dict_entry.rule_id
        AND dict_entry.dict_id = dict_expr.dict_id
    JOIN var_mentioned_in_expr
        ON dict_expr.pipeline_id = var_mentioned_in_expr.pipeline_id
        AND dict_expr.rule_id = var_mentioned_in_expr.rule_id
        AND dict_entry.expr_id = var_mentioned_in_expr.expr_id
        AND dict_entry.expr_type = var_mentioned_in_expr.expr_type;

/*
var_bound_in_goal(pipeline_id:, rule_id:, goal_id:, key:, negated:, var_name:, goal_index:, sql:) <-
    goal_arg(pipeline_id:, goal_id:, expr_id:, expr_type:)
    var_mentioned_in_expr(pipeline_id:, expr_id:, expr_type:, var_name:)
    goal_alias(pipeline_id:, rule_id:, goal_id:, negated:, goal_index:)
    sql := `"{{alias}}"."{{key}}"{{access_prefix}}`
*/
CREATE MATERIALIZED VIEW var_bound_in_goal AS
    SELECT DISTINCT
        goal_arg.pipeline_id,
        goal_alias.rule_id,
        goal_arg.goal_id,
        goal_arg.key,
        goal_alias.negated,
        goal_alias.goal_index,
        var_mentioned_in_expr.var_name,
        ('"' || goal_alias.alias || '"."' || goal_arg.key || '"' || var_mentioned_in_expr.access_prefix) AS sql
    FROM goal_arg
    JOIN var_mentioned_in_expr
        ON goal_arg.pipeline_id = var_mentioned_in_expr.pipeline_id
        AND goal_arg.expr_id = var_mentioned_in_expr.expr_id
        AND goal_arg.expr_type = var_mentioned_in_expr.expr_type
    JOIN goal_alias
        ON goal_arg.pipeline_id = goal_alias.pipeline_id
        AND goal_arg.goal_id = goal_alias.goal_id;

/*
error:unbound_var_in_negative_goal(pipeline_id:, rule_id:, goal_id:, var_name:) <-
    var_bound_in_goal(pipeline_id:, rule_id:, goal_id:, var_name:, negated: true)
    not var_bound_in_goal(pipeline_id:, rule_id:, var_name:, negated: false)
*/
CREATE MATERIALIZED VIEW "error:unbound_var_in_negative_goal" AS
    SELECT DISTINCT
        var_bound_in_goal.pipeline_id,
        var_bound_in_goal.rule_id,
        var_bound_in_goal.goal_id,
        var_bound_in_goal.var_name
    FROM var_bound_in_goal
    WHERE var_bound_in_goal.negated
    AND NOT EXISTS (
        SELECT 1
        FROM var_bound_in_goal
        WHERE var_bound_in_goal.pipeline_id = var_bound_in_goal.pipeline_id
        AND var_bound_in_goal.rule_id = var_bound_in_goal.rule_id
        AND var_bound_in_goal.var_name = var_bound_in_goal.var_name
        AND NOT var_bound_in_goal.negated
    );

/*
canonical_goal_var_sql(pipeline_id:, rule_id:, var_name:, sql: argmin<sql, goal_index>, goal_index: min<goal_index>) <-
    var_bound_in_goal(pipeline_id:, rule_id:, var_name:, negated: false, sql:, goal_index:)
*/
CREATE MATERIALIZED VIEW canonical_goal_var_sql AS
    SELECT DISTINCT
        var_bound_in_goal.pipeline_id,
        var_bound_in_goal.rule_id,
        var_bound_in_goal.var_name,
        MIN(var_bound_in_goal.goal_index) AS goal_index,
        ARG_MIN(var_bound_in_goal.sql, var_bound_in_goal.goal_index) AS sql
    FROM var_bound_in_goal
    WHERE NOT var_bound_in_goal.negated
    GROUP BY var_bound_in_goal.pipeline_id, var_bound_in_goal.rule_id, var_bound_in_goal.var_name;

/*
match_var_dependency(pipeline_id:, rule_id:, var_name:, parent_var_name:) <-
    body_match(pipeline_id:, rule_id:, match_id:, left_expr_id:, left_expr_type:, right_expr_id:, right_expr_type:)
    var_mentioned_in_expr(pipeline_id:, expr_id: left_expr_id, expr_type: left_expr_type, var_name:)
    var_mentioned_in_expr(pipeline_id:, expr_id: right_expr_id, expr_type: right_expr_type, var_name: parent_var_name)
    not var_bound_in_goal(pipeline_id:, rule_id:, var_name:, negated: false)
match_var_dependency(pipeline_id:, rule_id:, var_name:, parent_var_name:) <-
    match_var_dependency(pipeline_id:, rule_id:, var_name:, parent_var_name: middle_var_name)
    match_var_dependency(pipeline_id:, rule_id:, var_name: middle_var_name, parent_var_name:)
*/
DECLARE RECURSIVE VIEW match_var_dependency (pipeline_id TEXT, rule_id TEXT, var_name TEXT, parent_var_name TEXT);
CREATE MATERIALIZED VIEW match_var_dependency AS
    SELECT DISTINCT
        body_match.pipeline_id,
        body_match.rule_id,
        l.var_name,
        r.var_name AS parent_var_name
    FROM body_match
    JOIN var_mentioned_in_expr AS l
        ON body_match.pipeline_id = l.pipeline_id
        AND body_match.left_expr_id = l.expr_id
        AND body_match.left_expr_type = l.expr_type
    JOIN var_mentioned_in_expr AS r
        ON body_match.pipeline_id = r.pipeline_id
        AND body_match.right_expr_id = r.expr_id
        AND body_match.right_expr_type = r.expr_type
    WHERE NOT EXISTS (
        SELECT 1
        FROM var_bound_in_goal
        WHERE var_bound_in_goal.pipeline_id = body_match.pipeline_id
        AND var_bound_in_goal.rule_id = body_match.rule_id
        AND var_bound_in_goal.var_name = l.var_name
        AND NOT var_bound_in_goal.negated)

    UNION

    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.var_name,
        b.parent_var_name
    FROM match_var_dependency AS a
    JOIN match_var_dependency AS b
        ON a.pipeline_id = b.pipeline_id
        AND a.rule_id = b.rule_id
        AND a.parent_var_name = b.var_name;

/*

# CODEGENERATION

*/

/*
canonical_var_bound_sql(pipeline_id:, rule_id:, var_name:, sql:, aggregated: false) <-
    canonical_goal_var_sql(pipeline_id:, rule_id:, var_name:, sql:)
canonical_var_bound_sql(pipeline_id:, rule_id:, var_name:, sql: min<sql>, aggregated: some<aggregated>) <-
    var_bound_via_match(pipeline_id:, rule_id:, var_name:, sql:, aggregated:)
    not canonical_goal_var_sql(pipeline_id:, rule_id:, var_name:)
*/
DECLARE RECURSIVE VIEW var_bound_via_match (pipeline_id TEXT, rule_id TEXT, match_id TEXT, var_name TEXT, sql TEXT, aggregated BOOLEAN);
DECLARE RECURSIVE VIEW canonical_var_bound_sql (pipeline_id TEXT, rule_id TEXT, var_name TEXT, sql TEXT, aggregated BOOLEAN);
CREATE MATERIALIZED VIEW canonical_var_bound_sql AS
    SELECT DISTINCT
        canonical_goal_var_sql.pipeline_id,
        canonical_goal_var_sql.rule_id,
        canonical_goal_var_sql.var_name,
        canonical_goal_var_sql.sql,
        false AS aggregated
    FROM canonical_goal_var_sql

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
        FROM canonical_goal_var_sql
        WHERE canonical_goal_var_sql.pipeline_id = var_bound_via_match.pipeline_id
        AND canonical_goal_var_sql.rule_id = var_bound_via_match.rule_id
        AND canonical_goal_var_sql.var_name = var_bound_via_match.var_name
    )
    GROUP BY var_bound_via_match.pipeline_id, var_bound_via_match.rule_id, var_bound_via_match.var_name;

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
DECLARE RECURSIVE VIEW sql_expr_template_part_with_substitution (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, part TEXT, "index" INTEGER);
CREATE MATERIALIZED VIEW sql_expr_template_part_with_substitution AS
    SELECT DISTINCT
        sql_expr_template_part.pipeline_id,
        sql_expr_template_part.rule_id,
        sql_expr_template_part.expr_id,
        canonical_var_bound_sql.sql AS part,
        sql_expr_template_part."index"
    FROM sql_expr_template_part
    JOIN canonical_var_bound_sql
        ON sql_expr_template_part.pipeline_id = canonical_var_bound_sql.pipeline_id
        AND sql_expr_template_part.rule_id = canonical_var_bound_sql.rule_id
        AND SUBSTRING(sql_expr_template_part.part FROM 3 FOR (CHAR_LENGTH(sql_expr_template_part.part)-4)) = canonical_var_bound_sql.var_name
    WHERE sql_expr_template_part.part RLIKE '^\{\{[a-zA-Z_][A-Za-z0-9_:]*\}\}$'

    UNION

    SELECT DISTINCT
        sql_expr_template_part.pipeline_id,
        sql_expr_template_part.rule_id,
        sql_expr_template_part.expr_id,
        sql_expr_template_part.part,
        sql_expr_template_part."index"
    FROM sql_expr_template_part
    WHERE NOT (sql_expr_template_part.part RLIKE '^\{\{[a-zA-Z_][A-Za-z0-9_:]*\}\}$');

/*
sql_expr_substitution_status(pipeline_id:, rule_id:, expr_id:, count: count<>) <-
    sql_expr_template_part_with_substitution(pipeline_id:, rule_id:, expr_id:, index:)
*/
DECLARE RECURSIVE VIEW sql_expr_substitution_status (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, count BIGINT);
CREATE MATERIALIZED VIEW sql_expr_substitution_status AS
    SELECT DISTINCT
        t.pipeline_id,
        t.rule_id,
        t.expr_id,
        COUNT(*) AS count
    FROM sql_expr_template_part_with_substitution AS t
    GROUP BY t.pipeline_id, t.rule_id, t.expr_id;

/*
sql_expr_all_vars_are_bound(pipeline_id:, rule_id:, expr_id:) <-
    sql_expr(pipeline_id:, rule_id:, expr_id:, template:)
    sql_expr_substitution_status(pipeline_id:, rule_id:, expr_id:, count:)
    count = array_length(template)
*/
DECLARE RECURSIVE VIEW sql_expr_all_vars_are_bound (pipeline_id TEXT, rule_id TEXT, expr_id TEXT);
CREATE MATERIALIZED VIEW sql_expr_all_vars_are_bound AS
    SELECT DISTINCT
        sql_expr.pipeline_id,
        sql_expr.rule_id,
        sql_expr.expr_id
    FROM sql_expr
    JOIN sql_expr_substitution_status
        ON sql_expr.pipeline_id = sql_expr_substitution_status.pipeline_id
        AND sql_expr.rule_id = sql_expr_substitution_status.rule_id
        AND sql_expr.expr_id = sql_expr_substitution_status.expr_id
    WHERE sql_expr_substitution_status.count = ARRAY_LENGTH(sql_expr.template);

/*
substituted_sql_expr(pipeline_id:, rule_id:, expr_id:, sql:) <-
    sql_expr_all_vars_are_bound(pipeline_id:, rule_id:, expr_id:)
    sql_expr_template_part_with_substitution(pipeline_id:, rule_id:, expr_id:, part:, index:)
    sql := join(array<part, order_by: [index]>, "")
*/
DECLARE RECURSIVE VIEW substituted_sql_expr (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, sql TEXT);
CREATE MATERIALIZED VIEW substituted_sql_expr AS
    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        ARRAY_TO_STRING(ARRAY_AGG(b.part ORDER BY b."index"), '') AS sql
    FROM sql_expr_all_vars_are_bound AS a
    JOIN sql_expr_template_part_with_substitution AS b
        ON a.pipeline_id = b.pipeline_id
        AND a.rule_id = b.rule_id
        AND a.expr_id = b.expr_id
    GROUP BY a.pipeline_id, a.rule_id, a.expr_id;

/*
aggr_fn_name(fn_name: "count", sql_fn_name: "COUNT")
*/
CREATE MATERIALIZED VIEW aggr_fn_name AS
    SELECT 'count' AS fn_name, 'COUNT' AS sql_fn_name;

/*
substituted_aggr_expr(pipeline_id:, rule_id:, expr_id:, sql:) <-
    aggr_expr(pipeline_id:, rule_id:, expr_id:, fn_name: "count", arg_var: NULL)
    sql := "COUNT(*)"
substituted_aggr_expr(pipeline_id:, rule_id:, expr_id:, sql:) <-
    aggr_expr(pipeline_id:, rule_id:, expr_id:, fn_name:, arg_var: var_name)
    canonical_var_bound_sql(pipeline_id:, rule_id:, var_name:, sql: var_sql)
    aggr_fn_name(fn_name:, sql_fn_name:)
    sql := `{{sql_fn_name}}({{var_sql}})`
*/
DECLARE RECURSIVE VIEW substituted_aggr_expr (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, sql TEXT);
CREATE MATERIALIZED VIEW substituted_aggr_expr AS
    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        'COUNT(*)' AS sql
    FROM aggr_expr AS a
    WHERE a.fn_name = 'count' AND a.arg_var IS NULL

    UNION

    SELECT DISTINCT
        a.pipeline_id,
        a.rule_id,
        a.expr_id,
        (aggr_fn_name.sql_fn_name || '(' || canonical_var_bound_sql.sql || ')') AS sql
    FROM aggr_expr AS a
    JOIN canonical_var_bound_sql
        ON a.pipeline_id = canonical_var_bound_sql.pipeline_id
        AND a.rule_id = canonical_var_bound_sql.rule_id
        AND a.arg_var = canonical_var_bound_sql.var_name
    JOIN aggr_fn_name
        ON a.fn_name = aggr_fn_name.fn_name;

DECLARE RECURSIVE VIEW substituted_expr (pipeline_id TEXT, rule_id TEXT, expr_id TEXT, expr_type TEXT, sql TEXT, aggregated BOOLEAN);

/*
substituted_array_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated: some<aggregated>) <-
    array_expr(pipeline_id:, rule_id:, expr_id:, array_id:)
    array_entry(
        pipeline_id:, rule_id:, array_id:, index:,
        expr_id: element_expr_id, expr_type: element_expr_type)
    substituted_expr(
        pipeline_id:, rule_id:, expr_id: element_expr_id, expr_type: element_expr_type,
        sql: element_sql, aggregated:)
    sql := "ARRAY[" + join(array<element_sql, order_by: [index]>, ", ") + "]"
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
    sql := "MAP[" + join(array<`'{{key}}', {{value_sql}}`>, ", ") + "]"
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
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "sql_expr", sql:, aggregated: false) <-
    substituted_sql_expr(pipeline_id:, rule_id:, expr_id:, sql:)
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "int_expr", sql:, aggregated: false) <-
    int_expr(pipeline_id:, rule_id:, expr_id:, value:)
    sql := string(value)
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "str_expr", sql:, aggregated: false) <-
    str_expr(pipeline_id:, rule_id:, expr_id:, value:)
    sql := `'{{value}}'`
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "var_expr", sql:, aggregated:) <-
    var_expr(pipeline_id:, rule_id:, expr_id:, var_name:)
    canonical_var_bound_sql(pipeline_id:, rule_id:, var_name:, sql:, aggregated:)
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "aggr_expr", sql:, aggregated: true) <-
    substituted_aggr_expr(pipeline_id:, rule_id:, expr_id:, sql:)
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "array_expr", sql:, aggregated:) <-
    substituted_array_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated:)
substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type: "dict_expr", sql:, aggregated:) <-
    substituted_dict_expr(pipeline_id:, rule_id:, expr_id:, sql:, aggregated:)
*/
CREATE MATERIALIZED VIEW substituted_expr AS
    SELECT a.pipeline_id, a.rule_id, a.expr_id, CAST('sql_expr' AS TEXT) AS expr_type, a.sql, false AS aggregated
    FROM substituted_sql_expr AS a
    
    UNION
    
    SELECT b.pipeline_id, b.rule_id, b.expr_id, 'int_expr' AS expr_type, b.value AS sql, false AS aggregated
    FROM int_expr AS b

    UNION

    SELECT c.pipeline_id, c.rule_id, c.expr_id, 'str_expr' AS expr_type, ('''' || c.value || '''') AS sql, false AS aggregated
    FROM str_expr AS c

    UNION

    SELECT d.pipeline_id, d.rule_id, d.expr_id, 'var_expr' AS expr_type, canonical_var_bound_sql.sql, canonical_var_bound_sql.aggregated
    FROM var_expr AS d
    JOIN canonical_var_bound_sql
        ON d.pipeline_id = canonical_var_bound_sql.pipeline_id
        AND d.rule_id = canonical_var_bound_sql.rule_id
        AND d.var_name = canonical_var_bound_sql.var_name

    UNION

    SELECT e.pipeline_id, e.rule_id, e.expr_id, 'aggr_expr' AS expr_type, e.sql, true AS aggregated
    FROM substituted_aggr_expr AS e

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
var_bound_via_match(pipeline_id:, rule_id:, match_id:, var_name:, sql:, aggregated:) <-
    body_match(pipeline_id:, rule_id:, match_id:, left_expr_id:, left_expr_type:)
    var_mentioned_in_expr(pipeline_id:, expr_id: left_expr_id, expr_type: left_expr_type, var_name:, access_prefix:)
    match_right_expr_sql(pipeline_id:, rule_id:, match_id:, sql: right_sql, aggregated:)
    sql := `{{right_sql}}{{access_prefix}}`
var_bound_via_match(pipeline_id:, rule_id:, match_id: NULL, var_name:, sql:, aggregated:) <-
    rule_param(pipeline_id:, rule_id:, key: var_name, expr_id:, expr_type:)
    substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, sql:, aggregated:)
*/
CREATE MATERIALIZED VIEW var_bound_via_match AS
    SELECT DISTINCT
        body_match.pipeline_id,
        body_match.rule_id,
        body_match.match_id,
        var_mentioned_in_expr.var_name,
        (match_right_expr_sql.sql || var_mentioned_in_expr.access_prefix) AS sql,
        match_right_expr_sql.aggregated
    FROM body_match
    JOIN var_mentioned_in_expr
        ON body_match.pipeline_id = var_mentioned_in_expr.pipeline_id
        AND body_match.rule_id = var_mentioned_in_expr.rule_id
        AND body_match.left_expr_id = var_mentioned_in_expr.expr_id
        AND body_match.left_expr_type = var_mentioned_in_expr.expr_type
    JOIN match_right_expr_sql
        ON body_match.pipeline_id = match_right_expr_sql.pipeline_id
        AND body_match.rule_id = match_right_expr_sql.rule_id
        AND body_match.match_id = match_right_expr_sql.match_id
    
    UNION
    
    SELECT DISTINCT
        rule_param.pipeline_id,
        rule_param.rule_id,
        NULL AS match_id,
        rule_param.key AS var_name,
        substituted_expr.sql AS sql,
        substituted_expr.aggregated
    FROM rule_param
    JOIN substituted_expr
        ON rule_param.pipeline_id = substituted_expr.pipeline_id
        AND rule_param.rule_id = substituted_expr.rule_id
        AND rule_param.expr_id = substituted_expr.expr_id
        AND rule_param.expr_type = substituted_expr.expr_type;

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
error:neg_goal_sql_unresolved(pipeline_id:, rule_id:, goal_id:) <-
    body_goal(pipeline_id:, rule_id:, goal_id:, negated: true)
    goal_arg(pipeline_id:, rule_id:, goal_id:, expr_id:, expr_type:)
    not substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type:)
*/
CREATE MATERIALIZED VIEW "error:neg_goal_sql_unresolved" AS
    SELECT DISTINCT
        body_goal.pipeline_id,
        body_goal.rule_id,
        body_goal.goal_id
    FROM body_goal
    JOIN goal_arg
        ON body_goal.pipeline_id = goal_arg.pipeline_id
        AND body_goal.rule_id = goal_arg.rule_id
        AND body_goal.goal_id = goal_arg.goal_id
    WHERE body_goal.negated
    AND NOT EXISTS (
        SELECT 1
        FROM substituted_expr
        WHERE pipeline_id = goal_arg.pipeline_id
        AND rule_id = goal_arg.rule_id
        AND expr_id = goal_arg.expr_id
        AND expr_type = goal_arg.expr_type);

/*
neg_goal_where_cond(pipeline_id:, rule_id:, goal_id:, sql:) <-
    goal_alias(pipeline_id:, rule_id:, goal_id:, alias:, table_name:, negated: true)
    goal_arg(pipeline_id:, rule_id:, goal_id:, key:, expr_id:, expr_type:)
    substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, sql: expr_sql)
    cond_sql := join(array<`"{{alias}}"."{{key}}" = {{expr_sql}}`>, " AND ")
    sql := `NOT EXISTS (SELECT 1 FROM "{{table_name}}" AS "{{alias}})" WHERE {{cond_sql}}`
*/
CREATE MATERIALIZED VIEW neg_goal_where_cond AS
    SELECT DISTINCT
        goal_alias.pipeline_id,
        goal_alias.rule_id,
        goal_alias.goal_id,
        ('NOT EXISTS (SELECT 1 FROM "' || goal_alias.table_name || '" AS "' || goal_alias.alias || '" WHERE ' || ARRAY_TO_STRING(ARRAY_AGG(('"' || goal_alias.alias || '"."' || goal_arg.key || '" = ' || substituted_expr.sql)), ' AND ') || ')') AS sql
    FROM goal_alias
    JOIN goal_arg
        ON goal_alias.pipeline_id = goal_arg.pipeline_id
        AND goal_alias.rule_id = goal_arg.rule_id
        AND goal_alias.goal_id = goal_arg.goal_id
    JOIN substituted_expr
        ON goal_arg.pipeline_id = substituted_expr.pipeline_id
        AND goal_arg.rule_id = substituted_expr.rule_id
        AND goal_arg.expr_id = substituted_expr.expr_id
        AND goal_arg.expr_type = substituted_expr.expr_type
    WHERE goal_alias.negated
    GROUP BY goal_alias.pipeline_id, goal_alias.rule_id, goal_alias.goal_id, goal_alias.alias, goal_alias.table_name;

/*
sql_where_cond(pipeline_id:, rule_id:, cond_id:, sql:) <-
    body_sql_cond(pipeline_id:, rule_id:, cond_id:, sql_expr_id: expr_id)
    substituted_sql_expr(pipeline_id:, rule_id:, expr_id:, sql:)
*/
CREATE MATERIALIZED VIEW sql_where_cond AS
    SELECT DISTINCT
        body_sql_cond.pipeline_id,
        body_sql_cond.rule_id,
        body_sql_cond.cond_id,
        substituted_sql_expr.sql
    FROM body_sql_cond
    JOIN substituted_sql_expr
        ON body_sql_cond.pipeline_id = substituted_sql_expr.pipeline_id
        AND body_sql_cond.rule_id = substituted_sql_expr.rule_id
        AND body_sql_cond.sql_expr_id = substituted_sql_expr.expr_id;

/*
substituted_match_left_expr_with_right_expr(pipeline_id:, rule_id:, match_id:, expr_id:, expr_type_id:, sql:) <-
    body_match(
        pipeline_id:, rule_id:, match_id:, left_expr_id: expr_id, left_expr_type: expr_type,
        right_expr_id:, right_expr_type:)
    substituted_expr(pipeline_id:, rule_id:, expr_id: right_expr_id, expr_type: right_expr_type, sql:)
substituted_match_left_expr_with_right_expr(pipeline_id:, rule_id:, match_id:, expr_id:, expr_type_id:, sql:) <-
    substituted_match_left_expr_with_right_expr(
        pipeline_id:, rule_id:, match_id:, expr_id: right_expr_id, expr_type: right_expr_type,
        sql: right_expr_sql)
    right_expr_type = 'array_expr'
    array_expr(pipeline_id:, rule_id:, expr_id: right_expr_id, array_id:)
    array_entry(pipeline_id:, rule_id:, array_id:, index:, expr_id:, expr_type:)
    sql := `{{right_expr_sql}}[{{index}}]`
substituted_match_left_expr_with_right_expr(pipeline_id:, rule_id:, match_id:, expr_id:, expr_type_id:, sql:) <-
    substituted_match_left_expr_with_right_expr(
        pipeline_id:, rule_id:, match_id:, expr_id: right_expr_id, expr_type: right_expr_type,
        sql: right_expr_sql)
    right_expr_type = 'dict_expr'
    dict_expr(pipeline_id:, rule_id:, expr_id: right_expr_id, dict_id:)
    dict_entry(pipeline_id:, rule_id:, array_id:, key:, expr_id:, expr_type:)
    sql := `{{right_expr_sql}}['{{key}}']`
*/
DECLARE RECURSIVE VIEW substituted_match_left_expr_with_right_expr (pipeline_id TEXT, rule_id TEXT, match_id TEXT, expr_id TEXT, expr_type TEXT, sql TEXT);
CREATE MATERIALIZED VIEW substituted_match_left_expr_with_right_expr AS
    SELECT DISTINCT
        body_match.pipeline_id,
        body_match.rule_id,
        body_match.match_id,
        body_match.left_expr_id AS expr_id,
        body_match.left_expr_type AS expr_type,
        substituted_expr.sql
    FROM body_match
    JOIN substituted_expr
        ON substituted_expr.pipeline_id = body_match.pipeline_id
        AND substituted_expr.rule_id = body_match.rule_id
        AND substituted_expr.expr_id = body_match.right_expr_id
        AND substituted_expr.expr_type = body_match.right_expr_type
    
    UNION
    
    SELECT DISTINCT
        array_entry.pipeline_id,
        array_entry.rule_id,
        substituted_match_left_expr_with_right_expr.match_id,
        array_entry.expr_id,
        array_entry.expr_type,
        (substituted_match_left_expr_with_right_expr.sql || '[' || array_entry."index" || ']') AS sql
    FROM substituted_match_left_expr_with_right_expr
    JOIN array_expr
        ON array_expr.pipeline_id = substituted_match_left_expr_with_right_expr.pipeline_id
        AND array_expr.rule_id = substituted_match_left_expr_with_right_expr.rule_id
        AND array_expr.expr_id = substituted_match_left_expr_with_right_expr.expr_id
    JOIN array_entry
        ON array_expr.pipeline_id = array_entry.pipeline_id
        AND array_expr.rule_id = array_entry.rule_id
        AND array_expr.array_id = array_entry.array_id
    WHERE substituted_match_left_expr_with_right_expr.expr_type = 'array_expr'

    UNION

    SELECT DISTINCT
        dict_entry.pipeline_id,
        dict_entry.rule_id,
        substituted_match_left_expr_with_right_expr.match_id,
        dict_entry.expr_id,
        dict_entry.expr_type,
        (substituted_match_left_expr_with_right_expr.sql || '[' || '''' || dict_entry.key || '''' || ']') AS sql
    FROM substituted_match_left_expr_with_right_expr
    JOIN dict_expr
        ON dict_expr.pipeline_id = substituted_match_left_expr_with_right_expr.pipeline_id
        AND dict_expr.rule_id = substituted_match_left_expr_with_right_expr.rule_id
        AND dict_expr.expr_id = substituted_match_left_expr_with_right_expr.expr_id
    JOIN dict_entry
        ON dict_expr.pipeline_id = dict_entry.pipeline_id
        AND dict_expr.rule_id = dict_entry.rule_id
        AND dict_expr.dict_id = dict_entry.dict_id
    WHERE substituted_match_left_expr_with_right_expr.expr_type = 'dict_expr';

/*
match_where_cond(pipeline_id:, rule_id:, match_id:, sql:) <-
    body_match(pipeline_id:, rule_id:, match_id:, left_expr_id:, left_expr_type:, right_expr_id:, right_expr_type:)
    left_expr_type = 'array_expr'
    array_expr_length(pipeline_id:, rule_id:, expr_id:, length:)
    substituted_expr(pipeline_id:, rule_id:, expr_id: right_expr_id, expr_type: right_expr_type, sql: expr_sql)
    sql := `ARRAY_LENGTH({{expr_sql}}) = {{length}}`
match_where_cond(pipeline_id:, rule_id:, match_id:, sql:) <-
    substituted_match_left_expr_with_right_expr(
        pipeline_id:, rule_id:, match_id:, expr_id:, expr_type_id:, sql: match_expr_sql)
    substituted_expr(pipeline_id:, rule_id:, expr_id: left_expr_id, expr_type: left_expr_type, sql: left_expr_sql)
    sql := `{{match_expr_sql}} = {{left_expr_sql}}`
*/
CREATE MATERIALIZED VIEW match_where_cond AS
    SELECT DISTINCT
        body_match.pipeline_id,
        body_match.rule_id,
        body_match.match_id,
        ('ARRAY_LENGTH(' || substituted_expr.sql || ') = ' || array_expr_length.length) AS sql
    FROM body_match
    JOIN array_expr_length
        ON body_match.pipeline_id = array_expr_length.pipeline_id
        AND body_match.rule_id = array_expr_length.rule_id
        AND body_match.left_expr_id = array_expr_length.expr_id
    JOIN substituted_expr
        ON body_match.pipeline_id = substituted_expr.pipeline_id
        AND body_match.rule_id = substituted_expr.rule_id
        AND body_match.right_expr_id = substituted_expr.expr_id
        AND body_match.right_expr_type = substituted_expr.expr_type
    WHERE body_match.left_expr_type = 'array_expr'

    UNION

    SELECT DISTINCT
        substituted_expr.pipeline_id,
        substituted_expr.rule_id,
        substituted_match_left_expr_with_right_expr.match_id,
        (substituted_match_left_expr_with_right_expr.sql || ' = ' || substituted_expr.sql) AS sql
    FROM substituted_match_left_expr_with_right_expr
    JOIN substituted_expr
        ON substituted_expr.pipeline_id = substituted_match_left_expr_with_right_expr.pipeline_id
        AND substituted_expr.rule_id = substituted_match_left_expr_with_right_expr.rule_id;

/*
where_cond(pipeline_id:, rule_id:, sql:) <-
    sql_where_cond(pipeline_id:, rule_id:, sql:)
where_cond(pipeline_id:, rule_id:, sql:) <-
    match_where_cond(pipeline_id:, rule_id:, sql:)
where_cond(pipeline_id:, rule_id:, sql:) <-
    neg_goal_where_cond(pipeline_id:, rule_id:, sql:)

aggregated_where_cond(pipeline_id:, rule_id:, cond_id:, sql:) <-
    where_cond(pipeline_id:, rule_id:, sql:)
    sql := join(array<sql>, " AND ")
*/

/*
var_join(pipeline_id:, rule_id:, goal_id:, var_name:, sql:) <-
    canonical_goal_var_sql(pipeline_id:, rule_id:, goal_index: prev_goal_index, var_name:, sql: prev_sql)
    var_bound_in_goal(pipeline_id:, rule_id:, goal_id:, goal_index: next_goal_index, var_name:, sql: next_sql, negated: false)
    prev_goal_index < next_goal_index
    sql := `{{prev_sql}} = {{next_sql}}`
*/
CREATE MATERIALIZED VIEW var_join AS
    SELECT DISTINCT
        var_bound_in_goal.pipeline_id,
        var_bound_in_goal.rule_id,
        var_bound_in_goal.goal_id,
        canonical_goal_var_sql.var_name,
        (canonical_goal_var_sql.sql || ' = ' || var_bound_in_goal.sql) AS sql
    FROM canonical_goal_var_sql
    JOIN var_bound_in_goal
        ON canonical_goal_var_sql.pipeline_id = var_bound_in_goal.pipeline_id
        AND canonical_goal_var_sql.rule_id = var_bound_in_goal.rule_id
        AND canonical_goal_var_sql.var_name = var_bound_in_goal.var_name
        AND NOT var_bound_in_goal.negated
    WHERE canonical_goal_var_sql.goal_index < var_bound_in_goal.goal_index;

/*
join_cond_sql(pipeline_id:, rule_id:, goal_id:, sql:) <-
    var_join(pipeline_id:, rule_id:, goal_id:, sql: var_join_sql)
    sql := join(array<var_join_sql>, " AND ")
*/
CREATE MATERIALIZED VIEW join_cond_sql AS
    SELECT DISTINCT
        var_join.pipeline_id,
        var_join.rule_id,
        var_join.goal_id,
        ARRAY_TO_STRING(ARRAY_AGG(var_join.sql), ' AND ') AS sql
    FROM var_join
    GROUP BY var_join.pipeline_id, var_join.rule_id, var_join.goal_id;

/*
rule_join_sql(pipeline_id:, rule_id:, goal_id:, sql:) <-
    first_goal_alias(pipeline_id:, rule_id:, goal_id:, table_name:, alias:, negated: false)
    sql := `FROM "{{table_name}}" AS "{{alias}}"`
rule_join_sql(pipeline_id:, rule_id:, goal_id:, sql:) <-
    rule_join_sql(pipeline_id:, rule_id:, goal_id: prev_goal_id, sql: prev_sql)
    adjacent_goals(pipeline_id:, rule_id:, prev_goal_id:, next_goal_id:)
    goal_alias(pipeline_id:, rule_id:, table_name:, goal_id: next_goal_id, alias: next_alias)
    not join_cond_sql(pipeline_id:, rule_id:, goal_id: next_goal_id)
    sql := `{{prev_sql}} CROSS JOIN "{{table_name}}" AS "{{next_alias}}"`
rule_join_sql(pipeline_id:, rule_id:, goal_id:, sql:) <-
    rule_join_sql(pipeline_id:, rule_id:, goal_id: prev_goal_id, sql: prev_sql)
    adjacent_goals(pipeline_id:, rule_id:, prev_goal_id:, next_goal_id:)
    goal_alias(pipeline_id:, rule_id:, table_name:, goal_id: next_goal_id, alias: next_alias)
    join_cond_sql(pipeline_id:, rule_id:, goal_id: next_goal_id, sql: join_cond_sql)
    goal_id := next_goal_id
    sql := `{{prev_sql}} JOIN "{{table_name}}" AS "{{next_alias}}" ON {{join_cond_sql}}`
*/
DECLARE RECURSIVE VIEW rule_join_sql (pipeline_id TEXT, rule_id TEXT, goal_id TEXT, sql TEXT);
CREATE MATERIALIZED VIEW rule_join_sql AS
    SELECT DISTINCT
        first_goal_alias.pipeline_id,
        first_goal_alias.rule_id,
        first_goal_alias.goal_id,
        ('  FROM "' || first_goal_alias.table_name || '" AS "' || first_goal_alias.alias || '"') AS sql
    FROM first_goal_alias
    WHERE NOT first_goal_alias.negated
    
    UNION

    SELECT DISTINCT
        next_goal_alias.pipeline_id,
        next_goal_alias.rule_id,
        next_goal_alias.goal_id,
        (prev_rule_join_sql.sql || ' CROSS JOIN "' || next_goal_alias.table_name || '" AS "' || next_goal_alias.alias || '"') AS sql
    FROM rule_join_sql AS prev_rule_join_sql
    JOIN adjacent_goals
        ON prev_rule_join_sql.pipeline_id = adjacent_goals.pipeline_id
        AND prev_rule_join_sql.rule_id = adjacent_goals.rule_id
        AND prev_rule_join_sql.goal_id = adjacent_goals.prev_goal_id
    JOIN goal_alias AS next_goal_alias
        ON adjacent_goals.pipeline_id = next_goal_alias.pipeline_id
        AND adjacent_goals.rule_id = next_goal_alias.rule_id
        AND adjacent_goals.next_goal_id = next_goal_alias.goal_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM join_cond_sql
        WHERE prev_rule_join_sql.pipeline_id = join_cond_sql.pipeline_id
        AND prev_rule_join_sql.rule_id = join_cond_sql.rule_id
        AND prev_rule_join_sql.goal_id = join_cond_sql.goal_id)

    UNION

    SELECT DISTINCT
        next_goal_alias.pipeline_id,
        next_goal_alias.rule_id,
        next_goal_alias.goal_id,
        (prev_rule_join_sql.sql || ' JOIN "' || next_goal_alias.table_name || '" AS "' || next_goal_alias.alias || '" ON ' || join_cond_sql.sql || ')') AS sql
    FROM rule_join_sql AS prev_rule_join_sql
    JOIN adjacent_goals
        ON prev_rule_join_sql.pipeline_id = adjacent_goals.pipeline_id
        AND prev_rule_join_sql.rule_id = adjacent_goals.rule_id
        AND prev_rule_join_sql.goal_id = adjacent_goals.prev_goal_id
    JOIN goal_alias AS next_goal_alias
        ON adjacent_goals.pipeline_id = next_goal_alias.pipeline_id
        AND adjacent_goals.rule_id = next_goal_alias.rule_id
        AND adjacent_goals.next_goal_id = next_goal_alias.goal_id
    JOIN join_cond_sql
        ON next_goal_alias.pipeline_id = join_cond_sql.pipeline_id
        AND next_goal_alias.rule_id = join_cond_sql.rule_id
        AND next_goal_alias.goal_id = join_cond_sql.goal_id;

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
grouped_by_sql(pipeline_id:, rule_id:, sql:) <-
    unaggregated_param_expr(pipeline_id:, rule_id:, sql: param_sql)
    exprs_sql := join(array<param_sql>, ", ")
    sql := `GROUP BY {{exprs_sql}}`
*/
CREATE MATERIALIZED VIEW grouped_by_sql AS
    SELECT DISTINCT
        unaggregated_param_expr.pipeline_id,
        unaggregated_param_expr.rule_id,
        ('GROUP BY ' || ARRAY_TO_STRING(ARRAY_AGG(unaggregated_param_expr.sql), ', ')) AS sql
    FROM unaggregated_param_expr
    GROUP BY unaggregated_param_expr.pipeline_id, unaggregated_param_expr.rule_id;

/*
join_sql(pipeline_id:, rule_id:, sql:) <-
    # pick the last goal_id, for which there is no next one
    adjacent_goals(pipeline_id:, rule_id:, next_goal_id: last_goal_id)
    not adjacent_goals(pipeline_id:, rule_id:, prev_goal_id: last_goal_id)

    rule_join_sql(pipeline_id:, rule_id:, goal_id: last_goal_id, sql:)
*/
CREATE MATERIALIZED VIEW join_sql AS
    SELECT DISTINCT
        rule_join_sql.pipeline_id,
        rule_join_sql.rule_id,
        rule_join_sql.sql
    FROM rule_join_sql
    JOIN adjacent_goals AS a
        ON rule_join_sql.pipeline_id = a.pipeline_id
        AND rule_join_sql.rule_id = a.rule_id
        AND rule_join_sql.goal_id = a.prev_goal_id
    WHERE NOT EXISTS (
        SELECT 1
        FROM adjacent_goals
        WHERE rule_join_sql.pipeline_id = adjacent_goals.pipeline_id
        AND rule_join_sql.rule_id = adjacent_goals.rule_id
        AND adjacent_goals.prev_goal_id = a.next_goal_id);

/*
select_sql(pipeline_id:, rule_id:, sql:) <-
    rule_param(pipeline_id:, rule_id:, key:, expr_id:, expr_type:)
    substituted_expr(pipeline_id:, rule_id:, expr_id:, expr_type:, sql: expr_sql)
    grouped_by_sql(pipeline_id:, rule_id:, sql: group_by_sql)
    join_sql(pipeline_id:, rule_id:, sql: join_sql)
    columns_sql := join(array<`{{expr_sql}} AS "{{key}}"`>, ", ")
    sql := `SELECT {{columns_sql}} {{join_sql}} {{group_by_sql}}`
*/
CREATE MATERIALIZED VIEW select_sql AS
    SELECT DISTINCT
        rule_param.pipeline_id,
        rule_param.rule_id,
        ('SELECT ' || ARRAY_TO_STRING(ARRAY_AGG(substituted_expr.sql || ' AS "' || rule_param.key || '"'), ', ') || ' ' || join_sql.sql || ' ' || grouped_by_sql.sql) AS sql
    FROM rule_param
    JOIN substituted_expr
        ON rule_param.pipeline_id = substituted_expr.pipeline_id
        AND rule_param.rule_id = substituted_expr.rule_id
        AND rule_param.expr_id = substituted_expr.expr_id
        AND rule_param.expr_type = substituted_expr.expr_type
    JOIN grouped_by_sql
        ON rule_param.pipeline_id = grouped_by_sql.pipeline_id
        AND rule_param.rule_id = grouped_by_sql.rule_id
    JOIN join_sql
        ON rule_param.pipeline_id = join_sql.pipeline_id
        AND rule_param.rule_id = join_sql.rule_id
    GROUP BY rule_param.pipeline_id, rule_param.rule_id, join_sql.sql, grouped_by_sql.sql;
