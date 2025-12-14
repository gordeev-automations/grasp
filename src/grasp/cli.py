import argparse
import pathlib
import asyncio
import time

import json5
import aiohttp

from grasp.scripts.ensure_transpiler_ready import ensure_transpiler_pipeline_is_ready
from grasp.util import start_transaction, commit_transaction, insert_records, wait_till_input_tokens_processed, adhoc_query
import grasp.parser as parser



arg_parser = argparse.ArgumentParser(
    prog='Grasp',
    description='Transpiler from Grasp to Feldera SQL')

arg_parser.add_argument(
    '--feldera-url', type=str, default='http://localhost:8080', help='Feldera URL')
arg_parser.add_argument(
    '--transpiler-pipeline-name', type=str, default='grasp_transpiler',
    help='Name of the pipeline responsible for the transpiler')
arg_parser.add_argument(
    'input', nargs='+', type=pathlib.Path,
    help='Input file(s) *.grasp and *.schema.json5')



def natural_num_generator():
    n = 1
    while True:
        yield n
        n += 1

def add_fields_to_records(records, fields):
    def mix_in_fields(rows):
        return [{**r, **fields} for r in rows]
    return dict([(table_name, mix_in_fields(rows)) for (table_name, rows) in records.items()])

async def grasp_main():
    args = arg_parser.parse_args()

    grasp_source_paths = [p for p in args.input if p.suffix == '.grasp']
    schema_paths = [p for p in args.input if (p.name[-13:] == '.schema.json5')]
    for p in args.input:
        if p not in grasp_source_paths and p not in schema_paths:
            print(f'Unexpected input, neither *.grasp nor *.schema.json5 prefix: {p}')
            exit(1)

    pipeline_id = str(time.time())
    idgen = natural_num_generator()

    async with aiohttp.ClientSession(args.feldera_url, timeout=aiohttp.ClientTimeout(sock_read=0,total=0)) as session:
        await ensure_transpiler_pipeline_is_ready(session, args.transpiler_pipeline_name)

        queued_tokens = set()
        try:
            await start_transaction(session, args.transpiler_pipeline_name)
            for schema_path in schema_paths:
                schema = json5.loads(open(schema_path, 'r').read())
                records0 = parser.records_from_schema(schema)
                records = add_fields_to_records(records0, {'pipeline_id': pipeline_id})
                tokens = await insert_records(session, args.transpiler_pipeline_name, records)
                queued_tokens = queued_tokens.union(tokens)

            for source_path in grasp_source_paths:
                    records0 = parser.parse(open(source_path, 'r').read(), str(source_path), idgen=idgen)
                    records = add_fields_to_records(records0, {'pipeline_id': pipeline_id})
                    tokens = await insert_records(session, args.transpiler_pipeline_name, records)
                    queued_tokens = queued_tokens.union(tokens)
        finally:
            await commit_transaction(session, args.transpiler_pipeline_name)

        # print(f"Queued: {queued_tokens}")

        # all_tokens = set().union(*queued_tokens.values())
        await wait_till_input_tokens_processed(
            session, args.transpiler_pipeline_name, queued_tokens)
        sql = f'SELECT sql_lines FROM full_pipeline_sql WHERE pipeline_id = \'{pipeline_id}\''
        resp = await adhoc_query(session, args.transpiler_pipeline_name, sql)
        match resp:
            case {'sql_lines': sql_lines}:
                print('\n'.join(sql_lines))
            case _:
                raise Exception(f"Unexpected response {resp}")
                # exit(1)

        # paths_without_errors = (set(testcases_paths) - set(with_errors.keys())) & set(pipeline_ids.keys())
        # for testcase_path in paths_without_errors:
        #     dest_path = testcase_dest_path(testcase_path, cache_dir)
        #     pipeline_id = pipeline_ids[testcase_path]
        #     await write_output_sql(session, pipeline_name, pipeline_id, dest_path)

    # if with_errors:
    #     exit(1)



def main():
    asyncio.run(grasp_main(), debug=True)