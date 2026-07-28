import os

import pytest_asyncio

from jobq import JobQueue, create_pool, create_schema

DSN = os.environ.get("JOBQ_DSN", "postgres://postgres:postgres@127.0.0.1:5432/jobq")


@pytest_asyncio.fixture
async def pool():
    p = await create_pool(DSN)
    await create_schema(p)
    yield p
    await p.close()


@pytest_asyncio.fixture
async def q(pool):
    # Start every test from an empty table.
    await pool.execute("TRUNCATE jobs RESTART IDENTITY")
    return JobQueue(pool)
