import os
import sys
import asyncio

import json5
import json
import aiohttp

from grasp.util import testcase_dest_path, recompile_pipeline, do_need_to_recompile_pipeline, wait_till_pipeline_compiled, ensure_pipeline_started, testcase_expected_records_path, adhoc_query, testcase_key, testcase_table_inputs_paths, insert_records, wait_till_input_tokens_processed



def value_to_sql(v):
    if isinstance(v, str):
        return f"'{v}'"
    else:
        return str(v)

def expected_record_check_sql(key, table_name, r):
    return f'SELECT true AS passed FROM "{key}:{table_name}" WHERE {" AND ".join(f"\"{k}\" = {value_to_sql(v)}" for k, v in r.items())}'

async def check_testcase_results(session, pipeline_name, testcase_path):
    records_path = testcase_expected_records_path(testcase_path)
    expected_records = json5.loads(open(records_path, 'r').read())
    key = testcase_key(testcase_path)
    at_least_one_failed = False
    for table_name, records in expected_records.items():
        for r in records:
            sql = expected_record_check_sql(key, table_name, r)
            resp = await adhoc_query(session, pipeline_name, sql)
            print(f"Checking record: {table_name}({r}), response: {resp}")
            if not resp: # resp.get('passed', False):
                print(f"❌ Missing record: {table_name}({r})")
                at_least_one_failed = True
    return (not at_least_one_failed)

async def insert_testcases_input_data(session, pipeline_name, testcases_paths):
    records_to_insert = {}
    for testcase_path in testcases_paths:
        key = testcase_key(testcase_path)
        for table_name, input_path in testcase_table_inputs_paths(testcase_path).items():
            records_to_insert[f"{key}:{table_name}"] = []
            with open(input_path, 'r') as f:
                for line in f:
                    records_to_insert[f"{key}:{table_name}"].append(json.loads(line))
    return await insert_records(session, pipeline_name, records_to_insert)

async def main(testcases_paths):
    feldera_url = 'http://localhost:8080'
    pipeline_name = 'grasp_testsuite'
    curr_dir = os.path.abspath(os.path.dirname(__file__))
    cache_dir = f'{curr_dir}/../test/.grasp_cache'

    # prefer deterministic order
    testcases_paths.sort()
    
    testsuite_sql = ''
    udf_rs_path = f'{curr_dir}/../transpiler/udf.rs'
    udf_sql_preface_path = f'{curr_dir}/../transpiler/001_udf.sql'
    testsuite_sql += open(udf_sql_preface_path, 'r').read()

    for testcase_path in testcases_paths:
        dest_path = testcase_dest_path(testcase_path, cache_dir)
        if not os.path.exists(dest_path):
            raise Exception(f"{dest_path} does not exist")
        
        with open(dest_path, 'r') as f:
            testsuite_sql += f.read()

    async with aiohttp.ClientSession(feldera_url, timeout=aiohttp.ClientTimeout(sock_read=0,total=0)) as session:
        udf_rs = open(udf_rs_path, 'r').read()
        if (await do_need_to_recompile_pipeline(session, pipeline_name, testsuite_sql, udf_rs)):
            await recompile_pipeline(session, pipeline_name, testsuite_sql, udf_rs)
        await wait_till_pipeline_compiled(session, pipeline_name)
        await ensure_pipeline_started(session, pipeline_name)
        tokens = await insert_testcases_input_data(session, pipeline_name, testcases_paths)
        await wait_till_input_tokens_processed(session, pipeline_name, tokens)
        await asyncio.sleep(5)

        at_least_one_failed = False
        for testcase_path in testcases_paths:
            at_least_one_failed = at_least_one_failed or not (await check_testcase_results(session, pipeline_name, testcase_path))

        if not at_least_one_failed:
            print("✅ All tests passed!")



if __name__ == '__main__':
    asyncio.run(main(sys.argv[1:]), debug=True)