import os
import re
import functools

from lark import Lark, Tree, Token



def natural_num_generator():
    n = 1
    while True:
        yield n
        n += 1

def merge_records(records1, records2):
    result = {}
    # print(f"MERGE RECORDS {records1} {records2}")
    for key, rows in records1.items():
        if key in records2:
            result[key] = [*rows, *records2[key]]
        else:
            result[key] = rows
    for key, rows in records2.items():
        if key not in result:
            result[key] = rows
    return result



def records_from_val_args(rule_id, fncall_id, val_args, idgen):
    records = []
    for index, arg_expr in enumerate(val_args):
        match arg_expr:
            case Tree(data=Token(type='RULE', value='expr'), children=[expr]):
                expr_id = f'ex{next(idgen)}'
                expr_type, expr_records = records_from_expr(expr, rule_id, expr_id, idgen)
                records.append(merge_records(
                    expr_records,
                    {
                        'fn_val_arg': [{
                            'rule_id': rule_id,
                            'fncall_id': fncall_id,
                            'arg_index': index,
                            'expr_id': expr_id,
                            'expr_type': expr_type,

                            'start_line': arg_expr.meta.container_line,
                            'start_column': arg_expr.meta.container_column,
                            'end_line': arg_expr.meta.container_end_line,
                            'end_column': arg_expr.meta.container_end_column,
                        }]
                    }
                ))
            case _:
                raise Exception(f"Invalid arg expr {arg_expr}")
    return records

def records_from_kv_args(rule_id, fncall_id, kv_args, idgen):
    records = []
    for arg in kv_args:
        match arg:
            case Tree(data=Token(type='RULE', value='kv_arg'), children=[
                Token(type='IDENTIFIER', value=key),
                Tree(data=Token(type='RULE', value='expr'), children=[expr]),
            ]):
                expr_id = f'ex{next(idgen)}'
                expr_type, expr_records = records_from_expr(expr, rule_id, expr_id, idgen)
                records.append(merge_records(
                    expr_records,
                    {
                        'fn_kv_arg': [{
                            'rule_id': rule_id,
                            'fncall_id': fncall_id,
                            'key': key,
                            'expr_id': expr_id,
                            'expr_type': expr_type,

                            'start_line': arg.meta.container_line,
                            'start_column': arg.meta.container_column,
                            'end_line': arg.meta.container_end_line,
                            'end_column': arg.meta.container_end_column,
                        }]
                    }
                ))
            case _:
                raise Exception(f"Invalid arg expr {arg}")
    return records



def records_from_fncall_expr(aggr_expr, rule_id, expr_id, fn_name, fn_args, aggregated, idgen):
    # print(f"{fn_name} {fn_args}")
    fncall_id = f'fn{next(idgen)}'
    match fn_args:
        case None:
            return {
                'fncall_expr': [{
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'fn_name': fn_name,
                    'fncall_id': fncall_id,
                    'aggregated': aggregated,

                    'start_line': aggr_expr.meta.container_line,
                    'start_column': aggr_expr.meta.container_column,
                    'end_line': aggr_expr.meta.container_end_line,
                    'end_column': aggr_expr.meta.container_end_column,
                }]
            }
        case [Tree(data=Token(type='RULE', value='val_args'), children=val_args)]:
            return functools.reduce(
                merge_records,[
                    *records_from_val_args(rule_id, fncall_id, val_args, idgen),
                    {
                        'fncall_expr': [{
                            'rule_id': rule_id,
                            'expr_id': expr_id,
                            'fn_name': fn_name,
                            'fncall_id': fncall_id,
                            'aggregated': aggregated,

                            'start_line': aggr_expr.meta.container_line,
                            'start_column': aggr_expr.meta.container_column,
                            'end_line': aggr_expr.meta.container_end_line,
                            'end_column': aggr_expr.meta.container_end_column,
                        }]
                    }
                ]
            )
        case [
            Tree(data=Token(type='RULE', value='val_args'), children=val_args),
            Tree(data=Token(type='RULE', value='kv_args'), children=kv_args),
        ]:
            return functools.reduce(
                merge_records,[
                    *records_from_val_args(rule_id, fncall_id, val_args, idgen),
                    *records_from_kv_args(rule_id, fncall_id, kv_args, idgen),
                    {
                        'fncall_expr': [{
                            'rule_id': rule_id,
                            'expr_id': expr_id,
                            'fn_name': fn_name,
                            'fncall_id': fncall_id,
                            'aggregated': aggregated,

                            'start_line': aggr_expr.meta.container_line,
                            'start_column': aggr_expr.meta.container_column,
                            'end_line': aggr_expr.meta.container_end_line,
                            'end_column': aggr_expr.meta.container_end_column,
                        }]
                    }
                ]
            )
        case _:
            raise Exception(f"Invalid fn args {fn_args}")
    raise Exception("Not implemented")



def records_from_dict_arg(arg, rule_id, dict_id, idgen):
    match arg:
        case Tree(data=Token(type='RULE', value='kv_arg'), children=[
            Token(type='IDENTIFIER', value=key),
        ]):
            expr_id = f'ex{next(idgen)}'
            return {
                'var_expr': [{
                    'rule_id': rule_id, 'expr_id': expr_id, 'var_name': key,
                    'maybe_null_prefix': False,

                    'start_line': arg.children[0].line,
                    'start_column': arg.children[0].column,
                    'end_line': arg.children[0].end_line,
                    'end_column': arg.children[0].end_column,
                }],
                'dict_entry': [{
                    'rule_id': rule_id,
                    'dict_id': dict_id,
                    'key': key,
                    'expr_id': expr_id,
                    'expr_type': 'var_expr',

                    'start_line': arg.meta.container_line,
                    'start_column': arg.meta.container_column,
                    'end_line': arg.meta.container_end_line,
                    'end_column': arg.meta.container_end_column,
                }]
            }
        case Tree(data=Token(type='RULE', value='kv_arg'), children=[
            Token(type='IDENTIFIER', value=key),
            Tree(data=Token(type='RULE', value='expr'), children=children),
        ]):
            expr_id = f'ex{next(idgen)}'
            match children:
                case [expr]:
                    expr_type, expr_records = records_from_expr(expr, rule_id, expr_id, idgen)
                case [expr, Token(type='TYPE', value=assigned_type)]:
                    expr_type, expr_records = records_from_expr(
                        expr, rule_id, expr_id, idgen, assigned_type=assigned_type)
                case _:
                    raise Exception(f"Invalid arg expr {arg}")
            return merge_records(
                expr_records,
                {
                    'dict_entry': [{
                        'rule_id': rule_id,
                        'dict_id': dict_id,
                        'key': key,
                        'expr_id': expr_id,
                        'expr_type': expr_type,

                        'start_line': arg.meta.container_line,
                        'start_column': arg.meta.container_column,
                        'end_line': arg.meta.container_end_line,
                        'end_column': arg.meta.container_end_column,
                    }]
                }
            )
        case _:
            raise Exception(f"Invalid arg expr {arg}")



def records_from_array_element(element, index, rule_id, array_id, idgen):
    match element:
        case Tree(data=Token(type='RULE', value='array_element'), children=[
            Tree(data=Token(type='RULE', value='expr'), children=[expr]),
        ]):
            expr_id = f'ex{next(idgen)}'
            expr_type, expr_records = records_from_expr(expr, rule_id, expr_id, idgen)
            return merge_records(
                expr_records,
                {
                    'array_entry': [{
                        'rule_id': rule_id,
                        'array_id': array_id,
                        'index': index,
                        'expr_id': expr_id,
                        'expr_type': expr_type,

                        'start_line': element.meta.container_line,
                        'start_column': element.meta.container_column,
                        'end_line': element.meta.container_end_line,
                        'end_column': element.meta.container_end_column,
                    }]
                }
            )
        case Tree(data='asterisk_var', children=[
            Token(type='IDENTIFIER', value=var_name),
        ]):
            expr_id = f'ex{next(idgen)}'
            return {
                'var_expr': [{
                    'rule_id': rule_id, 'expr_id': expr_id, 'var_name': var_name,
                    'special_prefix': '*',
                    'maybe_null_prefix': False,

                    'start_line': element.children[0].line,
                    'start_column': element.children[0].column,
                    'end_line': element.children[0].end_line,
                    'end_column': element.children[0].end_column,
                }],
                'array_entry': [{
                    'rule_id': rule_id,
                    'array_id': array_id,
                    'index': index,
                    'expr_id': expr_id,
                    'expr_type': 'var_expr',

                    'start_line': element.meta.container_line,
                    'start_column': element.meta.container_column,
                    'end_line': element.meta.container_end_line,
                    'end_column': element.meta.container_end_column,
                }]
            }
        case _:
            raise Exception(f"Invalid array element {element}")



def records_from_expr(expr, rule_id, expr_id, idgen, assigned_type=None):
    match expr:
        case Token(type='NUMBER', value=value):
            return 'int_expr', {
                'int_expr': [{
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'value': int(value),

                    'start_line': expr.line,
                    'start_column': expr.column,
                    'end_line': expr.end_line,
                    'end_column': expr.end_column,
                }]
            }
        case Token(type='IDENTIFIER', value="NULL"):
            return 'null_expr', {
                'null_expr': [{
                    'rule_id': rule_id,
                    'expr_id': expr_id,

                    'start_line': expr.line,
                    'start_column': expr.column,
                    'end_line': expr.end_line,
                    'end_column': expr.end_column,
                }]
            }
        case Token(type='IDENTIFIER', value=var_name) if var_name in ["true", "false"]:
            return 'bool_expr', {
                'bool_expr': [{
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'value': (var_name == "true"),

                    'start_line': expr.line,
                    'start_column': expr.column,
                    'end_line': expr.end_line,
                    'end_column': expr.end_column,
                }]
            }
        case Token(type='IDENTIFIER', value=value):
            return 'var_expr', {
                'var_expr': [{
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'var_name': value,
                    'assigned_type': assigned_type,
                    'maybe_null_prefix': False,

                    'start_line': expr.line,
                    'start_column': expr.column,
                    'end_line': expr.end_line,
                    'end_column': expr.end_column,
                }]
            }
        case Token(type='ESCAPED_STRING', value=value):
            return 'str_expr', {
                'str_expr': [{
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'value': value[1:-1],

                    'start_line': expr.line,
                    'start_column': expr.column,
                    'end_line': expr.end_line,
                    'end_column': expr.end_column,
                }]
            }
        case Token(type='INTERPOLATED_STRING', value=value):
            template = re.split(r'(\{\{[a-zA-Z_][a-zA-Z0-9_]*\}\})', value[1:-1])
            return 'str_template_expr', {
                'str_template_expr': [{
                    'rule_id': rule_id,
                    'expr_id': expr_id,
                    'template': template,

                    'start_line': expr.line,
                    'start_column': expr.column,
                    'end_line': expr.end_line,
                    'end_column': expr.end_column,
                }]
            }
        case Tree(data=Token(type='RULE', value='binop_expr'), children=[
            Tree(data=Token(type='RULE', value='expr'), children=[left_expr]),
            Token(type=op_type, value=op),
            Tree(data=Token(type='RULE', value='expr'), children=[right_expr]),
        ]) if op_type in ['SPACED_BINOP', 'CMP_OP']:
            left_expr_id = f'ex{next(idgen)}'
            right_expr_id = f'ex{next(idgen)}'
            left_expr_type, left_expr_records = records_from_expr(left_expr, rule_id, left_expr_id, idgen)
            right_expr_type, right_expr_records = records_from_expr(right_expr, rule_id, right_expr_id, idgen)
            return 'binop_expr', functools.reduce(
                merge_records,
                [
                    left_expr_records,
                    right_expr_records,
                    {
                        'binop_expr': [{
                            'rule_id': rule_id,
                            'expr_id': expr_id,
                            'op': op,
                            'left_expr_id': left_expr_id,
                            'left_expr_type': left_expr_type,
                            'right_expr_id': right_expr_id,
                            'right_expr_type': right_expr_type,

                            'start_line': expr.meta.container_line,
                            'start_column': expr.meta.container_column,
                            'end_line': expr.meta.container_end_line,
                            'end_column': expr.meta.container_end_column,
                        }]
                    }
                ])

        case Tree(data=Token(type='RULE', value='aggregated_expr'), children=[
            Token(type='IDENTIFIER', value=fn_name),
            Tree(data=Token(type='RULE', value='fn_args'), children=fn_args),
        ]):
            return 'fncall_expr', records_from_fncall_expr(expr, rule_id, expr_id, fn_name, fn_args, True, idgen)
        case Tree(data=Token(type='RULE', value='aggregated_expr'), children=[
            Token(type='IDENTIFIER', value=fn_name),
        ]):
            return 'fncall_expr', records_from_fncall_expr(expr, rule_id, expr_id, fn_name, None, True, idgen)
        case Tree(data=Token(type='RULE', value='funcall_expr'), children=[
            Token(type='IDENTIFIER', value=fn_name),
            Tree(data=Token(type='RULE', value='fn_args'), children=fn_args),
        ]):
            return 'fncall_expr', records_from_fncall_expr(expr, rule_id, expr_id, fn_name, fn_args, False, idgen)

        case Tree(data=Token(type='RULE', value='dict_expr'), children=[
            Tree(data=Token(type='RULE', value='kv_args'), children=kv_args),
        ]):
            dict_id = f'dt{next(idgen)}'
            args_records = [records_from_dict_arg(da, rule_id, dict_id, idgen) for da in kv_args]
            return 'dict_expr', functools.reduce(merge_records, [
                *args_records,
                { 'dict_expr': [{
                    'rule_id': rule_id, 'dict_id': dict_id, 'expr_id': expr_id,

                    'start_line': expr.meta.container_line,
                    'start_column': expr.meta.container_column,
                    'end_line': expr.meta.container_end_line,
                    'end_column': expr.meta.container_end_column,
                }] },
            ])
        case Tree(data=Token(type='RULE', value='array_expr'), children=array_elements):
            array_id = f'ar{next(idgen)}'
            elements_records = [records_from_array_element(e, i+1, rule_id, array_id, idgen) for (i, e) in enumerate(array_elements)]
            return 'array_expr', functools.reduce(merge_records, [
                *elements_records,
                { 'array_expr': [{
                    'rule_id': rule_id, 'array_id': array_id, 'expr_id': expr_id,

                    'start_line': expr.meta.container_line,
                    'start_column': expr.meta.container_column,
                    'end_line': expr.meta.container_end_line,
                    'end_column': expr.meta.container_end_column,
                }] },
            ])
        case _:
            raise Exception(f"Invalid expr {expr}")



def records_from_fact_arg(fact_arg, rule_id, fact_id, idgen):
    expr_id = f'ex{next(idgen)}'
    match fact_arg:
        case Tree(data=Token(type='RULE', value='kv_arg'), children=[
            Token(type='IDENTIFIER', value=key),            
        ]):
            return {
                'fact_arg': [{
                    'rule_id': rule_id, 'fact_id': fact_id, 'key': key,
                    'expr_id': expr_id, 'expr_type': 'var_expr',

                    'start_line': fact_arg.meta.container_line,
                    'start_column': fact_arg.meta.container_column,
                    'end_line': fact_arg.meta.container_end_line,
                    'end_column': fact_arg.meta.container_end_column,
                }],
                'var_expr': [{
                    'rule_id': rule_id, 'expr_id': expr_id, 'var_name': key,
                    'maybe_null_prefix': False,

                    'start_line': fact_arg.children[0].line,
                    'start_column': fact_arg.children[0].column,
                    'end_line': fact_arg.children[0].end_line,
                    'end_column': fact_arg.children[0].end_column,
                }],
            }

        case Tree(data=Token(type='RULE', value='kv_arg'), children=[
            Token(type='IDENTIFIER', value=key),
            Tree(data=Token(type='RULE', value='expr'), children=[
                Token(type='MAYBE_NULL_PREFIX'),
                Token(type='IDENTIFIER', value=var_name),
            ]),
        ]):
            return {
                'fact_arg': [{
                    'rule_id': rule_id, 'fact_id': fact_id, 'key': key,
                    'expr_id': expr_id, 'expr_type': 'var_expr',

                    'start_line': fact_arg.meta.container_line,
                    'start_column': fact_arg.meta.container_column,
                    'end_line': fact_arg.meta.container_end_line,
                    'end_column': fact_arg.meta.container_end_column,
                }],
                'var_expr': [{
                    'rule_id': rule_id, 'expr_id': expr_id, 'var_name': var_name,
                    'maybe_null_prefix': True,

                    'start_line': fact_arg.children[0].line,
                    'start_column': fact_arg.children[0].column,
                    'end_line': fact_arg.children[0].end_line,
                    'end_column': fact_arg.children[0].end_column,
                }],
            }

        case Tree(data=Token(type='RULE', value='kv_arg'), children=[
            Token(type='IDENTIFIER', value=key),
            Tree(data=Token(type='RULE', value='expr'), children=[expr]),
        ]):
            expr_type, expr_records = records_from_expr(expr, rule_id, expr_id, idgen)
            return merge_records(
                expr_records,
                {
                    'fact_arg': [{
                        'rule_id': rule_id, 'fact_id': fact_id, 'key': key,
                        'expr_id': expr_id, 'expr_type': expr_type,

                        'start_line': fact_arg.meta.container_line,
                        'start_column': fact_arg.meta.container_column,
                        'end_line': fact_arg.meta.container_end_line,
                        'end_column': fact_arg.meta.container_end_column,
                    }]
                }
            )
        case _:
            raise Exception(f"Invalid fact arg {fact_arg}")



def records_from_body_stmt(index, stmt, rule_id, idgen):
    match stmt:
        case Tree(data=Token(type='RULE', value='fact'), children=[
            Token(type='IDENTIFIER', value=table_name),
            Tree(data=Token(type='RULE', value='kv_args'), children=fact_args),
        ]):
            # print(f"fact: {stmt}")
            fact_id = f'ft{next(idgen)}'
            args_records = [records_from_fact_arg(fa, rule_id, fact_id, idgen) for fa in fact_args]
            return functools.reduce(merge_records, [
                *args_records,
                { 'body_fact': [{
                    'rule_id': rule_id, 'fact_id': fact_id, 'index': index,
                    'table_name': table_name, 'negated': False,

                    'start_line': stmt.meta.container_line,
                    'start_column': stmt.meta.container_column,
                    'end_line': stmt.meta.container_end_line,
                    'end_column': stmt.meta.container_end_column,
                }] },
            ])
        case Tree(data=Token(type='RULE', value='negated_fact'), children=[
            Token(type='IDENTIFIER', value=table_name),
            Tree(data=Token(type='RULE', value='kv_args'), children=fact_args),
        ]):
            # print(f"fact: {stmt}")
            fact_id = f'ft{next(idgen)}'
            args_records = [records_from_fact_arg(fa, rule_id, fact_id, idgen) for fa in fact_args]
            return functools.reduce(merge_records, [
                *args_records,
                { 'body_fact': [{
                    'rule_id': rule_id, 'fact_id': fact_id, 'index': index,
                    'table_name': table_name, 'negated': True,

                    'start_line': stmt.meta.container_line,
                    'start_column': stmt.meta.container_column,
                    'end_line': stmt.meta.container_end_line,
                    'end_column': stmt.meta.container_end_column,
                }] },
            ])
        case Tree(data=Token(type='RULE', value='match_stmt'), children=[
            Tree(data=Token(type='RULE', value='expr'), children=[left_expr]),
            Tree(data=Token(type='RULE', value='expr'), children=[right_expr]),
        ]):
            match_id = f'mt{next(idgen)}'
            left_expr_id = f'ex{next(idgen)}'
            right_expr_id = f'ex{next(idgen)}'
            left_expr_type, left_expr_records = records_from_expr(
                left_expr, rule_id, left_expr_id, idgen)
            right_expr_type, right_expr_records = records_from_expr(
                right_expr, rule_id, right_expr_id, idgen)
            return functools.reduce(merge_records, [
                left_expr_records,
                right_expr_records,
                { 'body_match': [{
                    'rule_id': rule_id, 'match_id': match_id,
                    'left_expr_id': left_expr_id, 'left_expr_type': left_expr_type,
                    'right_expr_id': right_expr_id, 'right_expr_type': right_expr_type,

                    'start_line': stmt.meta.container_line,
                    'start_column': stmt.meta.container_column,
                    'end_line': stmt.meta.container_end_line,
                    'end_column': stmt.meta.container_end_column,
                }] },
            ])
        case Tree(data=Token(type='RULE', value='expr'), children=[expr]):
            # if it is not a fact and not match,
            # then it must be an expression, that must evaluate to bool
            expr_id = f'ex{next(idgen)}'
            expr_type, expr_records = records_from_expr(expr, rule_id, expr_id, idgen)
            return functools.reduce(merge_records, [
                expr_records,
                {
                    'body_expr': [{
                        'rule_id': rule_id, 'expr_id': expr_id, 'expr_type': expr_type,

                        'start_line': stmt.meta.container_line,
                        'start_column': stmt.meta.container_column,
                        'end_line': stmt.meta.container_end_line,
                        'end_column': stmt.meta.container_end_column,
                    }]
                }
            ])
        case _:
            raise Exception(f"Invalid body stmt {stmt}")    



def records_from_rule_param(rule_param, rule_id, idgen):
    expr_id = f'ex{next(idgen)}'
    match rule_param:
        case Tree(data=Token(type='RULE', value='kv_arg'), children=[
            Token(type='IDENTIFIER', value=key),
            Tree(data=Token(type='RULE', value='expr'), children=[expr]),
        ]):
            expr_type, expr_records = records_from_expr(expr, rule_id, expr_id, idgen)
            return merge_records(
                expr_records,
                { 'rule_param': [{
                    'rule_id': rule_id, 'key': key, 'expr_id': expr_id, 'expr_type': expr_type,
                
                    'start_line': rule_param.meta.container_line,
                    'start_column': rule_param.meta.container_column,
                    'end_line': rule_param.meta.container_end_line,
                    'end_column': rule_param.meta.container_end_column,
                }] },
            )
        case Tree(data=Token(type='RULE', value='kv_arg'), children=[
            Token(type='IDENTIFIER', value=key),
        ]):
            return {
                'rule_param': [{ 
                    'rule_id': rule_id, 'key': key, 'expr_id': expr_id, 'expr_type': 'var_expr',

                    'start_line': rule_param.meta.container_line,
                    'start_column': rule_param.meta.container_column,
                    'end_line': rule_param.meta.container_end_line,
                    'end_column': rule_param.meta.container_end_column,
                }],
                'var_expr': [{
                    'rule_id': rule_id, 'expr_id': expr_id, 'var_name': key,
                    'maybe_null_prefix': False,
                
                    'start_line': rule_param.children[0].line,
                    'start_column': rule_param.children[0].column,
                    'end_line': rule_param.children[0].end_line,
                    'end_column': rule_param.children[0].end_column,
                }],
            }
        case _:
            raise Exception(f"Invalid rule param {rule_param}")



def records_from_rule_decl(rule_decl, original_source_path, table_name, rule_params, body_stmts, idgen):
    rule_id = f'ru{next(idgen)}'
    param_records = [records_from_rule_param(rp, rule_id, idgen) for rp in rule_params]
    # print(f"params_recorsd {param_records}")
    body_stmts_records = [records_from_body_stmt(i+1, bs, rule_id, idgen) for (i, bs) in enumerate(body_stmts)]
    return functools.reduce(merge_records, [
        *param_records,
        *body_stmts_records,
        { 'rule': [{
            'rule_id': rule_id, 'table_name': table_name,

            'source_path': original_source_path,
            'start_line': rule_decl.meta.container_line,
            'start_column': rule_decl.meta.container_column,
            'end_line': rule_decl.meta.container_end_line,
            'end_column': rule_decl.meta.container_end_column,
        }] },
    ])



def records_from_toplevel_decls(toplevel_decl, original_source_path, idgen):
    # print(toplevel_decl)
    match toplevel_decl:
        case Tree(data=Token(type='RULE', value='rule'), children=[
            Token(type='IDENTIFIER', value=table_name),
            Tree(data=Token(type='RULE', value='kv_args'), children=rule_params),
        ]):
            return records_from_rule_decl(
                toplevel_decl, original_source_path, table_name, rule_params, [], idgen)
        case Tree(data=Token(type='RULE', value='rule'), children=[
            Token(type='IDENTIFIER', value=table_name),
            Tree(data=Token(type='RULE', value='kv_args'), children=rule_params),
            Tree(data=Token(type='RULE', value='body_stmt'), children=[body_stmt]),
        ]):
            return records_from_rule_decl(
                toplevel_decl, original_source_path, table_name, rule_params, [body_stmt], idgen)
        case Tree(data=Token(type='RULE', value='rule'), children=[
            Token(type='IDENTIFIER', value=table_name),
            Tree(data=Token(type='RULE', value='kv_args'), children=rule_params),
            Tree(data=Token(type='RULE', value='multiline_body'), children=children),
        ]):
            body_stmts = []
            for child in children:
                match child:
                    case Tree(data=Token(type='RULE', value='body_stmt'), children=[body_stmt]):
                        body_stmts.append(body_stmt)
                    case _:
                        raise Exception(f"Invalid body stmt {child}")
            return records_from_rule_decl(
                toplevel_decl, original_source_path, table_name, rule_params, body_stmts, idgen)
        case _:
            raise Exception(f"Invalid toplevel decl {toplevel_decl}")



def records_from_tree(tree, original_source_path, idgen):
    match tree:
        case Tree(data=Token(type='RULE', value='start'), children=toplevel_decls):
            return functools.reduce(
                merge_records,
                [records_from_toplevel_decls(d, original_source_path, idgen) for d in toplevel_decls],
                {})
        case _:
            raise Exception(f"Invalid tree {tree}")



def records_from_schema(schema):
    schema_table = []
    schema_table_column = []
    for (table_name, table_def) in schema.get('tables', {}).items():
        for (column_name, column_def) in table_def.get('columns', {}).items():
            schema_table_column.append({
                'table_name': table_name,
                'column_name': column_name,
                'column_type': column_def.get('type'),
                'nullable': column_def.get('nullable', False),
            })
        schema_table.append({
            'table_name': table_name,
            'materialized': table_def.get('materialized', False),
        })
    return {
        'schema_table': schema_table,
        'schema_table_column': schema_table_column,
    }



def parse(text, original_path, schema=None, idgen=None):
    scripts_dir = os.path.abspath(os.path.dirname(__file__))
    grammar_text = open(f'{scripts_dir}/../../grammar.lark', 'r').read()
    # propagate token positions: line, column, end_line, end_col.
    # https://github.com/lark-parser/lark/issues/12#issuecomment-304404835
    parser = Lark(grammar_text, parser="earley", propagate_positions=True)
    # for simplicity of grammar, always insert new lines in the beginnging of the file
    # and in the end
    # for tok in parser.lex("\n" + text + "\n"):
    #     print(f"TOKEN: {repr(tok)}")

    tree = parser.parse("\n" + text + "\n")
    # print(f'\n\ntree:\n{tree}\n\n')
    # print(f'\n\ntree:\n{tree.pretty()}\n\n')
    if not idgen:
        idgen = natural_num_generator()
    if schema:
        return merge_records(
            records_from_tree(tree, original_path, idgen),
            records_from_schema(schema))

    return records_from_tree(tree, original_path, idgen)
