import os
import sys
import hashlib
import asyncio

import aiohttp
import json5

import grasp.parser as parser
from grasp.scripts.util import testcase_key, insert_records, file_hash, need_to_transpile_testcase, adhoc_query, testcase_dest_path, fetch_ingest_status



async def fetch_output_sql_lines(session, pipeline_name, pipeline_id):
    sql = f"SELECT sql_lines FROM full_pipeline_sql WHERE pipeline_id = '{pipeline_id}'"
    result = await adhoc_query(session, pipeline_name, sql)
    assert result
    return result['sql_lines']

async def enqueue_transpilation(testcase_path, pipeline_name, session):
    records0 = parser.parse(open(testcase_path, 'r').read(), testcase_path)
    pipeline_id = f'{testcase_key(testcase_path)}:{file_hash(testcase_path)}'
    records = {
        'table_name_prefix': [{
            # all tables in every testcase are prefixed with the testcase name.
            # This way we can transpile each case independently,
            # and then lump them all together, to run all tests in parallel.
            'pipeline_id': pipeline_id,
            'prefix': testcase_key(testcase_path),
        }]
    }
    for table_name, rows in records0.items():
        records[table_name] = [{**r, 'pipeline_id': pipeline_id} for r in rows]
    tokens = await insert_records(session, pipeline_name, records)
    return (pipeline_id, tokens)

async def report_errors_if_any(session, pipeline_name, pipeline_id, testcase_path):
    sql = f"SELECT error_type FROM \"error\" WHERE pipeline_id = '{pipeline_id}'"
    result = await adhoc_query(session, pipeline_name, sql)
    if result:
        print(f"Error in {testcase_path}: {', '.join([x['error_type'] for x in result])}")
        return True
    return False 

async def write_output_sql(session, pipeline_name, pipeline_id, dest_path):
    sql_lines = await fetch_output_sql_lines(session, pipeline_name, pipeline_id)
    # print(f"SQL LINES: {sql_lines}")
    with open(dest_path, 'w') as f:
        for line in sql_lines:
            f.write(line + '\n')



async def main(testcases_paths):
    feldera_url = 'http://localhost:8080'
    pipeline_name = 'transpiler'

    curr_dir = os.path.abspath(os.path.dirname(__file__))
    cache_dir = f'{curr_dir}/../test/.grasp_cache'
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    pipeline_ids = {}
    queued_tokens = {}
    with_errors = {}
    async with aiohttp.ClientSession(feldera_url, timeout=aiohttp.ClientTimeout(sock_read=0,total=0)) as session:
        # await start_transaction(session, pipeline_name)
        for testcase_path in testcases_paths:
            if need_to_transpile_testcase(testcase_path, cache_dir):
                # insert all inputs at once, so it would transpile in parallel
                (pipeline_id, tokens) = await enqueue_transpilation(testcase_path, pipeline_name, session)
                queued_tokens[testcase_path] = tokens
                pipeline_ids[testcase_path] = pipeline_id
        # await commit_transaction(session, pipeline_name)

        # print(f"Queued: {queued}")

        # while queued_tokens or fin_tokens:
        while queued_tokens:
            for testcase_path, tokens in {**queued_tokens}.items():
                pipeline_id = pipeline_ids[testcase_path]
                for token in list(tokens):
                    status = await fetch_ingest_status(session, pipeline_name, token)
                    match status:
                        case {'status': 'inprogress'}:
                            pass
                        case {'status': 'complete'}:
                            # print(f"Insert completed: {token}")
                            tokens.remove(token)
                        case _:
                            raise Exception(f"Unknown ingest status: {status}")
                
                if not tokens:
                    del queued_tokens[testcase_path]

            await asyncio.sleep(1)

        paths_without_errors = (set(testcases_paths) - set(with_errors.keys())) & set(pipeline_ids.keys())
        for testcase_path in paths_without_errors:
            dest_path = testcase_dest_path(testcase_path, cache_dir)
            pipeline_id = pipeline_ids[testcase_path]
            await write_output_sql(session, pipeline_name, pipeline_id, dest_path)

    if with_errors:
        exit(1)



if __name__ == '__main__':
    asyncio.run(main(sys.argv[1:]), debug=True)
