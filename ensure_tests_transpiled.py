import aiohttp
import asyncio
import json5



async def insert_records(session, pipeline_name, records):
    insert_tokens = set()

    url = f'/v0/pipelines/{pipeline_name}/start_transaction'
    async with session.post(url) as resp:
        if resp.status not in [200, 201]:
            body = await resp.text()
            raise Exception(f"Unexpected response {resp.status}: {body}")

    for table_name, rows in records.items():
        url = f'/v0/pipelines/{pipeline_name}/ingress/{table_name}'
        params = {'update_format': 'raw', 'array': 'true', 'format': 'json'}
        async with session.post(url, params=params, json=rows) as resp:
            if resp.status not in [200, 201]:
                body = await resp.text()
                raise Exception(f"Unexpected response {resp.status}: {body}")
            json_resp = await resp.json()
            insert_tokens.add(json_resp['token'])
            # print(f"Inserted records to table {table_name}: {json_resp}")

    url = f'/v0/pipelines/{pipeline_name}/commit_transaction'
    async with session.post(url) as resp:
        if resp.status not in [200, 201]:
            body = await resp.text()
            raise Exception(f"Unexpected response {resp.status}: {body}")
    return insert_tokens

async def fetch_ingest_status(session, pipeline_name, token):
    url = f'/v0/pipelines/{pipeline_name}/completion_status'
    async with session.get(url, params={'token': token}) as resp:
        return await resp.json()

async def wait_till_complete(session, pipeline_name, tokens):
    while tokens:
        for token in list(tokens):
            status = await fetch_ingest_status(session, pipeline_name, token)
            match status:
                case {'status': 'inprogress'}:
                    pass
                case {'status': 'complete'}:
                    tokens.remove(token)
                case _:
                    raise Exception(f"Unknown ingest status: {status}")
        await asyncio.sleep(1)

async def adhoc_query(session, pipeline_name, sql):
    url = f'/v0/pipelines/{pipeline_name}/query'
    async with session.get(url, params={'sql': sql, 'format': 'json', 'array': 'true'}) as resp:
        return await resp.json()

async def ensure_absence_of_errors(session, pipeline_name, pipeline_id):
    sql = f"SELECT * FROM \"error\" WHERE pipeline_id = '{pipeline_id}'"
    result = await adhoc_query(session, pipeline_name, sql)
    if result:
        raise Exception(f"Pipeline {pipeline_name} has errors: {result}")

async def fetch_output_sql(session, pipeline_name, pipeline_id):
    sql = f"SELECT sql_lines FROM full_pipeline_sql WHERE pipeline_id = '{pipeline_id}'"
    result = await adhoc_query(session, pipeline_name, sql)
    # print(f"REUSLT: {result}")
    # assert len(result) == 1
    return result['sql_lines']

async def main():
    feldera_url = 'http://localhost:8080'
    pipeline_name = 'transpiler'


    app_schema = json5.load(open(app_schema_path, 'r'))
    rules = get_rules()
    tables2 = app_schema['tables']

    async with aiohttp.ClientSession(feldera_url, timeout=aiohttp.ClientTimeout(sock_read=0,total=0)) as session:
        # await ensure_transpiler_pipeline_is_ready(session, pipeline_name)
        records1, pipeline_id = rules_to_records(rules)
        records2 = schemas_to_records(tables2, pipeline_id)
        records = {**records2, **records1}
        tokens = await insert_records(session, pipeline_name, records)
        await wait_till_complete(session, pipeline_name, tokens)

        await ensure_absence_of_errors(session, pipeline_name, pipeline_id)

        output_lines = await fetch_output_sql(session, pipeline_name, pipeline_id)
        print("\n".join(output_lines))



if __name__ == '__main__':
    asyncio.run(main(), debug=True)
