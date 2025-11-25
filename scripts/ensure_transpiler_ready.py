import os
import asyncio
import aiohttp

from grasp.scripts.util import fetch_pipeline_status, recompile_pipeline, do_need_to_recompile_pipeline, wait_till_pipeline_compiled, ensure_pipeline_started





def read_transpiler_sql():
    curr_dir = os.path.abspath(os.path.dirname(__file__))
    # select all *.sql files from transpiler/ directory
    # sort by name. Read in order, concatenate content and return
    sql_files = [f for f in os.listdir(f'{curr_dir}/../transpiler') if f.endswith('.sql')]
    sql_files.sort()
    sql_files = [open(f'{curr_dir}/../transpiler/{f}', 'r').read() for f in sql_files]
    return '\n'.join(sql_files)

def read_transpiler_udf_rs():
    curr_dir = os.path.abspath(os.path.dirname(__file__))
    return open(f'{curr_dir}/../transpiler/udf.rs', 'r').read()

async def ensure_transpiler_pipeline_is_ready(session, pipeline_name):
    # curr_dir = os.path.abspath(os.path.dirname(__file__))
    transpiler_sql = read_transpiler_sql()
    udf_rs = read_transpiler_udf_rs()
    # retrieve current version of program_code for transpiler pipeline
    # is it is not the same as on on the disc, recompile it
    if (await do_need_to_recompile_pipeline(session, pipeline_name, transpiler_sql, udf_rs)):
        await recompile_pipeline(session, pipeline_name, transpiler_sql, udf_rs)
    print("Waiting for transpiler to be ready")
    await wait_till_pipeline_compiled(session, pipeline_name)
    await ensure_pipeline_started(session, pipeline_name)



async def main():
    feldera_url = 'http://localhost:8080'
    pipeline_name = 'transpiler'

    async with aiohttp.ClientSession(feldera_url, timeout=aiohttp.ClientTimeout(sock_read=0,total=0)) as session:
        await ensure_transpiler_pipeline_is_ready(session, pipeline_name)



if __name__ == '__main__':
    asyncio.run(main(), debug=True)
