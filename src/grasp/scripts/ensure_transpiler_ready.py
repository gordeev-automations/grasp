import os
import sys

import asyncio
import aiohttp

from grasp.util import recompile_pipeline, do_need_to_recompile_pipeline, wait_till_pipeline_compiled, ensure_pipeline_started, read_transpiler_sql, read_transpiler_udf_rs



async def ensure_transpiler_pipeline_is_ready(session, pipeline_name):
    # curr_dir = os.path.abspath(os.path.dirname(__file__))
    transpiler_sql = read_transpiler_sql()
    udf_rs = read_transpiler_udf_rs()
    # retrieve current version of program_code for transpiler pipeline
    # is it is not the same as on on the disc, recompile it
    if (await do_need_to_recompile_pipeline(session, pipeline_name, transpiler_sql, udf_rs)):
        await recompile_pipeline(session, pipeline_name, transpiler_sql, udf_rs)
    print("Waiting for transpiler to be ready", file=sys.stderr)
    await wait_till_pipeline_compiled(session, pipeline_name)
    await ensure_pipeline_started(session, pipeline_name)



async def main():
    feldera_url = 'http://localhost:8080'
    pipeline_name = 'grasp_transpiler'

    async with aiohttp.ClientSession(feldera_url, timeout=aiohttp.ClientTimeout(sock_read=0,total=0)) as session:
        await ensure_transpiler_pipeline_is_ready(session, pipeline_name)



if __name__ == '__main__':
    asyncio.run(main(), debug=True)
