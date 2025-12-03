CREATE TABLE schema_table (
    pipeline_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    "materialized" BOOLEAN NOT NULL
    -- has_computed_id BOOLEAN NOT NULL,
    -- has_tenant BOOLEAN NOT NULL,
    -- read_only BOOLEAN NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE schema_table_column (
    pipeline_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    column_type TEXT NOT NULL,
    nullable BOOLEAN NOT NULL
) WITH ('materialized' = 'true');

-- CREATE TABLE schema_table_archive_from_file (
--     pipeline_id TEXT NOT NULL,
--     table_name TEXT NOT NULL,
--     filename TEXT NOT NULL
-- ) WITH ('materialized' = 'true');

-- CREATE TABLE schema_table_archive_pg (
--     pipeline_id TEXT NOT NULL,
--     table_name TEXT NOT NULL,
--     pg_url TEXT NOT NULL,
--     pg_query TEXT NOT NULL
-- ) WITH ('materialized' = 'true');

/*
# Additional transpilation options.
*/

CREATE TABLE table_name_prefix (
    pipeline_id TEXT NOT NULL,
    prefix TEXT NOT NULL
);

/*
# AST records:
*/

CREATE TABLE rule (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    table_name TEXT NOT NULL,

    source_path TEXT NOT NULL,
    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE rule_param (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    "key" TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    expr_type TEXT NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE aggr_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    fn_name TEXT NOT NULL,
    fncall_id TEXT NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE fncall_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    fn_name TEXT NOT NULL,
    fncall_id TEXT NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE fn_val_arg (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    fncall_id TEXT NOT NULL,
    fncall_expr_type TEXT NOT NULL,
    arg_index INTEGER NOT NULL,
    expr_id TEXT NOT NULL,
    expr_type TEXT NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE fn_kv_arg (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    fncall_id TEXT NOT NULL,
    fncall_expr_type TEXT NOT NULL,
    "key" TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    expr_type TEXT NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE int_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    value BIGINT NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE str_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    value TEXT NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE var_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    var_name TEXT NOT NULL,
    -- for example "*", "**" or NULL
    -- used in pattern matching
    special_prefix TEXT,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE null_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE binop_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    left_expr_id TEXT NOT NULL,
    left_expr_type TEXT NOT NULL,
    right_expr_id TEXT NOT NULL,
    right_expr_type TEXT NOT NULL,
    op TEXT NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE sql_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    template TEXT ARRAY NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE dict_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    dict_id TEXT NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE dict_entry (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    dict_id TEXT NOT NULL,
    key TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    expr_type TEXT NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE array_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    array_id TEXT NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE array_entry (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    array_id TEXT NOT NULL,
    "index" INTEGER NOT NULL,
    expr_id TEXT NOT NULL,
    expr_type TEXT NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE body_fact (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    fact_id TEXT NOT NULL,
    "index" INTEGER NOT NULL,
    table_name TEXT NOT NULL,
    negated BOOLEAN NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE fact_arg (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    fact_id TEXT NOT NULL,
    "key" TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    expr_type TEXT NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE body_match (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    match_id TEXT NOT NULL,
    left_expr_id TEXT NOT NULL,
    left_expr_type TEXT NOT NULL,
    right_expr_id TEXT NOT NULL,
    right_expr_type TEXT NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE body_expr (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    expr_id TEXT NOT NULL,
    expr_type TEXT NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

CREATE TABLE body_sql_cond (
    pipeline_id TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    cond_id TEXT NOT NULL,
    sql_expr_id TEXT NOT NULL,

    start_line INTEGER NOT NULL,
    start_column INTEGER NOT NULL,
    end_line INTEGER NOT NULL,
    end_column INTEGER NOT NULL
) WITH ('materialized' = 'true');

/*
# Use these records to prevent computing recursive views till all records are inserted.
# Since with incomplete data they may get into an infinite loop.
*/
-- CREATE TABLE all_records_inserted (
--     pipeline_id TEXT NOT NULL
-- ) WITH ('materialized' = 'true');
