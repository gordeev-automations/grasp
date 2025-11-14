import enum
import string
import random



class Const(enum.Enum):
    NULL = 1
null = Const.NULL



def rule(table_name, params, body, materialized=False):
    return {'type': 'rule', 'table_name': table_name, 'params': params, 'body': body, 'materialized': materialized}

def goal(table_name, *args, id=None):
    return {'type': 'goal', 'table_name': table_name, 'args': list(args), 'id': id, 'negated': False}

def neg_goal(table_name, *args):
    return {'type': 'goal', 'table_name': table_name, 'args': list(args), 'id': None, 'negated': True}

def sql_cond(template):
    return {'type': 'sql_cond', 'template': template}

def sql_expr(template):
    return {'type': 'sql_expr', 'template': template}

def match(left_expr, right_expr):
    return {'type': 'match', 'left_expr': left_expr, 'right_expr': right_expr}

def strval(value):
    return {'type': 'str', 'value': value}

def dictval(*args):
    return {'type': 'dict', 'args': list(args)}

def aggr(fn_name, *args):
    return {'type': 'aggr', 'fn_name': fn_name, 'args': list(args)}

def array(*args):
    return {'type': 'array', 'args': list(args)}



def gen_random_id():
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for i in range(8))

def merge_records(records1, records2):
    result = {}
    for key, rows in records1.items():
        if key in records2:
            result[key] = [*rows, *records2[key]]
        else:
            result[key] = rows
    for key, rows in records2.items():
        if key not in result:
            result[key] = rows
    return result



def dict_entry_to_record(arg, pipeline_id, rule_id, dict_id):
    match arg:
        case _ if type(arg) == str:
            expr_id = gen_random_id()
            return {
                'var_expr': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'var_name': arg,
                }],
                'dict_entry': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'dict_id': dict_id,
                    'key': arg,
                    'expr_id': expr_id,
                    'expr_type': 'var_expr',
                }]
            }
        case (key, expr):
            records, expr_id, expr_type = expr_to_records(expr, pipeline_id, rule_id)
            return merge_records(records, {
                'dict_entry': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'dict_id': dict_id,
                    'key': key,
                    'expr_id': expr_id,
                    'expr_type': expr_type,
                }]
            })
        case _:
            raise Exception(f"Invalid dict entry {arg}")

def array_entry_to_record(index, arg, pipeline_id, rule_id, array_id):
    records, expr_id, expr_type = expr_to_records(arg, pipeline_id, rule_id)
    return merge_records(records, {
        'array_entry': [{
            'pipeline_id': pipeline_id,
            'rule_id': rule_id,
            'array_id': array_id,
            'index': index+1,
            'expr_id': expr_id,
            'expr_type': expr_type,
        }]
    })

def expr_to_records(expr, pipeline_id, rule_id):
    expr_id = gen_random_id()
    match expr:
        case Const.NULL:
            expr_type = 'sql_expr'
            records = {
                'sql_expr': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'template': ["NULL"],
                }]
            }
        case _ if type(expr) == bool:
            expr_type = 'sql_expr'
            records = {
                'sql_expr': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'template': [str(expr).upper()],
                }]
            }
        case _ if type(expr) == str:
            expr_type = 'var_expr'
            records = {
                'var_expr': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'var_name': expr,
                }]
            }
        case _ if type(expr) == int:
            expr_type = 'int_expr'
            records = {
                'int_expr': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'value': expr,
                }]
            }
        case {'type': 'str', 'value': value}:
            expr_type = 'str_expr'
            records = {
                'str_expr': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'value': value,
                }]
            }
        case {'type': 'aggr', 'fn_name': fn_name, 'args': args}:
            expr_type = 'aggr_expr'
            arg_var = None
            if len(args) == 1:
                arg_var = args[0]
            elif len(args) > 1:
                raise Exception(f"Invalid expr {expr}")
            records = {
                'aggr_expr': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'fn_name': fn_name,
                    'arg_var': arg_var,
                }]
            }
        case {'type': 'dict', 'args': args}:
            expr_type = 'dict_expr'
            dict_id = gen_random_id()
            records = {}
            for arg in args:
                records = merge_records(records, dict_entry_to_record(arg, pipeline_id, rule_id, dict_id))
            records = merge_records(records, {
                'dict_expr': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'dict_id': dict_id,
                }]
            })
        case {'type': 'array', 'args': args}:
            expr_type = 'array_expr'
            array_id = gen_random_id()
            records = {}
            for index, arg in enumerate(args):
                records = merge_records(records, array_entry_to_record(index, arg, pipeline_id, rule_id, array_id))
            records = merge_records(records, {
                'array_expr': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'array_id': array_id,
                }]
            })
        case {'type': 'sql_expr', 'template': template}:
            expr_type = 'sql_expr'
            records = {
                'sql_expr': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'template': template,
                }]
            }
        case _:
            raise Exception(f"Invalid expr {expr}")
    return records, expr_id, expr_type

def param_to_record(param, pipeline_id, rule_id):
    match param:
        case _ if type(param) == str:
            expr_id = gen_random_id()
            return {
                'var_expr': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'var_name': param,
                }],
                'rule_param': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'key': param,
                    'expr_id': expr_id,
                    'expr_type': 'var_expr',
                }]
            }
        case (key, expr):
            records, expr_id, expr_type = expr_to_records(expr, pipeline_id, rule_id)
            return merge_records(records, {
                'rule_param': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'key': key,
                    'expr_id': expr_id,
                    'expr_type': expr_type,
                }]
            })
        case _:
            raise Exception(f"Invalid param {param}")

def goal_arg_to_records(arg, goal_id, pipeline_id, rule_id):
    match arg:
        case _ if type(arg) == str:
            expr_id = gen_random_id()
            return {
                'var_expr': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'var_name': arg,
                }],
                'goal_arg': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'goal_id': goal_id,
                    'key': arg,
                    'expr_id': expr_id,
                    'expr_type': 'var_expr',
                }]
            }
        case (key, expr):
            records, expr_id, expr_type = expr_to_records(expr, pipeline_id, rule_id)
            return merge_records(records, {
                'goal_arg': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'goal_id': goal_id,
                    'key': key,
                    'expr_id': expr_id,
                    'expr_type': expr_type,
                }]
            })
        case _:
            raise Exception(f"Invalid goal arg {arg}")

def body_stmt_to_record(index, body_stmt, pipeline_id, rule_id):
    match body_stmt:
        case {'type': 'goal', 'table_name': table_name, 'args': args, 'id': id, 'negated': negated}:
            records = {}
            goal_id = gen_random_id()
            for arg in args:
                records0 = goal_arg_to_records(arg, goal_id, pipeline_id, rule_id)
                records = merge_records(records, records0)
            return merge_records(records, {
                'body_goal': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'goal_id': goal_id,
                    'index': index+1,
                    'table_name': table_name,
                    'negated': negated,
                    'id_var': id,
                }]
            })
        case {'type': 'sql_cond', 'template': template}:
            expr_id = gen_random_id()
            return {
                'sql_expr': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'template': template,
                }],
                'body_sql_cond': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'cond_id': gen_random_id(),
                    'sql_expr_id': expr_id,
                }]
            }
        case {'type': 'match', 'left_expr': left_expr, 'right_expr': right_expr}:
            records, left_expr_id, left_expr_type = expr_to_records(left_expr, pipeline_id, rule_id)
            records0, right_expr_id, right_expr_type = expr_to_records(right_expr, pipeline_id, rule_id)
            records = merge_records(records, records0)
            return merge_records(records, {
                'body_match': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'match_id': gen_random_id(),
                    'left_expr_id': left_expr_id,
                    'left_expr_type': left_expr_type,
                    'right_expr_id': right_expr_id,
                    'right_expr_type': right_expr_type,
                }]
            })
        case _:
            raise Exception(f"Invalid body_stmt {body_stmt}")



def rule_to_record(rule, pipeline_id):
    match rule:
        case {'type': 'rule', 'table_name': table_name, 'params': params, 'body': body, 'materialized': materialized}:
            rule_id = gen_random_id()
            records = {}
            for param in params:
                records = merge_records(records,
                    param_to_record(param, pipeline_id, rule_id))
            for index, body_stmt in enumerate(body):
                records = merge_records(records,
                    body_stmt_to_record(index, body_stmt, pipeline_id, rule_id))
            return merge_records(records, {
                'rule': [{
                    'pipeline_id': pipeline_id,
                    'rule_id': rule_id,
                    'table_name': table_name,
                    'materialized': materialized,
                }]
            })
        case _:
            raise Exception(f"Invalid rule {rule}")



def rules_to_records(rules):
    pipeline_id = gen_random_id()
    records = {}
    for rule in rules:
        records = merge_records(records, rule_to_record(rule, pipeline_id))
    return records, pipeline_id



def column_def_to_records(table_name, column_name, column_def, pipeline_id):
    return {
        'schema_table_column': [{
            'pipeline_id': pipeline_id,
            'table_name': table_name,
            'column_name': column_name,
            'data_type': column_def['type'],
            'nullable': column_def.get('nullable', False),
        }]
    }

def archive_def_to_records(table_name, archive_def, pipeline_id):
    match archive_def:
        case {'from_file': filename}:
            return {
                'schema_table_archive_from_file': [{
                    'pipeline_id': pipeline_id,
                    'table_name': table_name,
                    'filename': filename,
                }]
            }
        case {'pg_url': pg_url, 'pq_query': pg_query}:
            return {
                'schema_table_archive_pg': [{
                    'pipeline_id': pipeline_id,
                    'table_name': table_name,
                    'pg_url': pg_url,
                    'pg_query': pg_query,
                }]
            }
        case _:
            raise Exception(f"Invalid archive_def {archive_def}")

def table_to_record(table_name, table_def, pipeline_id):
    records = {}
    for (column_name, columnd_def) in table_def.get('columns', {}).items():
        records = merge_records(records, column_def_to_records(table_name, column_name, columnd_def, pipeline_id))
    for archive_def in table_def.get('archive', []):
        records = merge_records(records, archive_def_to_records(table_name, archive_def, pipeline_id))
    return merge_records(records, {
        'schema_table': [{
            'pipeline_id': pipeline_id,
            'table_name': table_name,
            'materialized': table_def.get('materialized', False),
            'has_computed_id': table_def.get('has_computed_id', True),
            'has_tenant': table_def.get('has_tenant', True),
            'read_only': table_def.get('read_only', False),
        }]
    })

def schemas_to_records(tables, pipeline_id):
    records = {}
    for (table_name, table_def) in tables.items():
        records = merge_records(records, table_to_record(table_name, table_def, pipeline_id))
    return records