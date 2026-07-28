"""Database plumbing: pool creation, JSON codec, schema bootstrap."""
from __future__ import annotations

import json
import pathlib

import asyncpg

_SCHEMA_PATH = pathlib.Path(__file__).with_name("schema.sql")


async def _init_connection(conn: asyncpg.Connection) -> None:
    # Transparently encode/decode jsonb <-> Python objects so callers pass and
    # receive plain dicts/lists instead of JSON strings.
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def create_pool(dsn: str, *, min_size: int = 1, max_size: int = 10) -> asyncpg.Pool:
    """Create an asyncpg pool wired up with the JSON codec."""
    return await asyncpg.create_pool(
        dsn, init=_init_connection, min_size=min_size, max_size=max_size
    )


async def create_schema(pool: asyncpg.Pool) -> None:
    """Apply schema.sql (idempotent, safe to call on every startup)."""
    sql = _SCHEMA_PATH.read_text()
    async with pool.acquire() as conn:
        await conn.execute(sql)
