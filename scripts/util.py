import os
import hashlib
import asyncio



async def fetch_pipeline_status(session, pipeline_name):
    url = f'/v0/pipelines/{pipeline_name}'
    async with session.get(url, params={'selector': 'status'}) as resp:
        return await resp.json()

async def fetch_pipeline_state(session, pipeline_name):
    url = f'/v0/pipelines/{pipeline_name}'
    async with session.get(url) as resp:
        return await resp.json()

async def do_need_to_recompile_pipeline(session, pipeline_name, curr_transpiler_sql, curr_udf_rs):
    url = f'/v0/pipelines/{pipeline_name}'
    async with session.get(url) as resp:
        json_resp = await resp.json()
        match json_resp:
            case {'error_code': 'UnknownPipelineName'}:
                print(f"Pipeline {pipeline_name} does not exist")
                return True
            case {'program_code': program_code, 'udf_rust': udf_rs}:
                if program_code != curr_transpiler_sql or udf_rs != curr_udf_rs:
                    return True
    return False

async def recompile_pipeline(session, pipeline_name, pipeline_sql, udf_rs):
    status = await fetch_pipeline_status(session, pipeline_name)
    has_prev_version = not (status.get('error_code', None) == 'UnknownPipelineName')
    if has_prev_version:
        if status['deployment_status'] == 'Running':
            async with session.post(f'/v0/pipelines/{pipeline_name}/stop', params={'force': 'true'}) as resp:
                if resp.status not in [200, 202]:
                    body = await resp.text()
                    raise Exception(f"Unexpected response {resp.status}: {body}")

        while True:
            status = await fetch_pipeline_status(session, pipeline_name)
            if status['deployment_status'] == 'Stopped':
                break
            await asyncio.sleep(1)
        
        async with session.post(f'/v0/pipelines/{pipeline_name}/clear') as resp:
            if resp.status not in [200, 202]:
                body = await resp.text()
                raise Exception(f"Unexpected response {resp.status}: {body}")

        while True:
            status = await fetch_pipeline_status(session, pipeline_name)
            if status['storage_status'] == 'Cleared':
                break
            await asyncio.sleep(1)

    url = f'/v0/pipelines/{pipeline_name}'
    data = {
        'program_code': pipeline_sql,
        'name': pipeline_name,
        'udf_rust': udf_rs,
    }
    async with session.put(url, json=data) as resp:
        if resp.status not   in [200, 201]:
            body = await resp.text()
            raise Exception(f"Unexpected response {resp.status}: {body}")

async def wait_till_pipeline_compiled(session, pipeline_name):
    while True:
        status = await fetch_pipeline_status(session, pipeline_name)
        # print(f"Status: {status}")
        match status['program_status']:
            case 'Success':
                break
            case 'Pending' | 'CompilingSql' | 'SqlCompiled' | 'CompilingRust':
                pass
            case 'SqlError' | 'RustError' | 'SystemError':
                data = await fetch_pipeline_state(session, pipeline_name)
                print(data['program_error'])
                raise Exception("Pipeline failed to compile")
            case program_status:
                raise Exception(f"Unknown transpiler status: {program_status}")
        await asyncio.sleep(1)

async def ensure_pipeline_started(session, pipeline_name):
    url = f'/v0/pipelines/{pipeline_name}/start'
    async with session.post(url) as resp:
        if resp.status not in [200, 201, 202]:
            body = await resp.text()
            raise Exception(f"Unexpected response {resp.status}: {body}")
    while True:
        status = await fetch_pipeline_status(session, pipeline_name)
        if status['deployment_status'] == 'Running':
            break
        await asyncio.sleep(1)

async def start_transaction(session, pipeline_name):
    url = f'/v0/pipelines/{pipeline_name}/start_transaction'
    async with session.post(url) as resp:
        if resp.status not in [200, 201]:
            body = await resp.text()
            raise Exception(f"Unexpected response {resp.status}: {body}")

async def fetch_pipeline_stats(session, pipeline_name):
    url = f'/v0/pipelines/{pipeline_name}/stats'
    async with session.get(url) as resp:
        return await resp.json()

async def commit_transaction(session, pipeline_name):
    url = f'/v0/pipelines/{pipeline_name}/commit_transaction'
    async with session.post(url) as resp:
        if resp.status not in [200, 201]:
            body = await resp.text()
            raise Exception(f"Unexpected response {resp.status}: {body}")

    while True:
        stats = await fetch_pipeline_stats(session, pipeline_name)
        match stats:
            case {'global_metrics': {'transaction_status': 'NoTransaction'}}:
                return
            case {'global_metrics': {'transaction_status': 'TransactionInProgress'}}:
                pass
            case {'global_metrics': {'transaction_status': 'CommitInProgress'}}:
                pass
            case _:
                raise Exception(f"Unexpected stats: {stats}")
        await asyncio.sleep(1)

async def insert_records(session, pipeline_name, records):
    insert_tokens = set()

    for table_name, rows in records.items():
        url = f'/v0/pipelines/{pipeline_name}/ingress/{table_name}'
        params = {'update_format': 'raw', 'array': 'true', 'format': 'json'}

        async with session.post(url, params=params, json=rows) as resp:
            if resp.status not in [200, 201]:
                body = await resp.text()
                raise Exception(f"Unexpected response {resp.status}: {body}")
            json_resp = await resp.json()
            insert_tokens.add(json_resp['token'])
            print(f"Inserted {len(rows)} records into {table_name}: {json_resp}")

    return insert_tokens

async def fetch_ingest_status(session, pipeline_name, token):
    url = f'/v0/pipelines/{pipeline_name}/completion_status'
    async with session.get(url, params={'token': token}) as resp:
        if resp.status in [200, 201, 202]:
            return await resp.json()
        raise Exception(f"Unexpected response {resp.status}: {await resp.text()}")

async def adhoc_query(session, pipeline_name, sql):
    url = f'/v0/pipelines/{pipeline_name}/query'
    async with session.get(url, params={'sql': sql, 'format': 'json', 'array': 'true'}) as resp:
        if resp.status in [200, 201, 202]:
            return await resp.json()
        raise Exception(f"Unexpected response {resp.status}: {await resp.text()}\nSQL: {sql}")

def file_hash(path):
    return hashlib.sha256(open(path, 'rb').read()).hexdigest()[:10]

def testcase_key(path):
    filename = os.path.basename(path)
    assert filename[-11:] == '.test.grasp'
    return filename[:-11]

def testcase_dest_path(testcase_path, cache_dir):
    return f'{cache_dir}/{testcase_key(testcase_path)}.{file_hash(testcase_path)}.sql'

def testcase_expected_records_path(testcase_path):
    dirpath = os.path.dirname(testcase_path)
    return f'{dirpath}/{testcase_key(testcase_path)}.expected.json5'

def need_to_transpile_testcase(testcase_path, cache_dir):
    return not os.path.exists(testcase_dest_path(testcase_path, cache_dir))
