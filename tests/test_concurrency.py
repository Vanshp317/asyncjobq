import asyncio
import collections

import pytest

from jobq import JobQueue


async def test_concurrent_claimers_never_double_claim(q: JobQueue):
    """The core guarantee: with many workers hammering claim() at once, every
    job is handed to exactly one worker. This is what FOR UPDATE SKIP LOCKED
    buys us."""
    N_JOBS = 200
    N_WORKERS = 16

    for i in range(N_JOBS):
        await q.enqueue("work", {"i": i})

    claimed_by: dict[int, str] = {}
    lock = asyncio.Lock()

    async def worker(wid: str):
        while True:
            jobs = await q.claim("default", 5, wid, lease_seconds=30)
            if not jobs:
                # Could be transient contention; double-check nothing's left.
                remaining = await q.stats()
                if remaining["queued"] == 0:
                    return
                continue
            async with lock:
                for j in jobs:
                    # If this id was already claimed, the guarantee is violated.
                    assert j.id not in claimed_by, f"job {j.id} double-claimed"
                    claimed_by[j.id] = wid

    await asyncio.gather(*(worker(f"w{n}") for n in range(N_WORKERS)))

    assert len(claimed_by) == N_JOBS
    # Sanity: every job ended up running, none left queued.
    stats = await q.stats()
    assert stats["running"] == N_JOBS
    assert stats["queued"] == 0


async def test_batch_claim_distributes_across_workers(q: JobQueue):
    for i in range(50):
        await q.enqueue("work", {"i": i})

    a = await q.claim("default", 25, "wa", 30)
    b = await q.claim("default", 25, "wb", 30)

    ids_a = {j.id for j in a}
    ids_b = {j.id for j in b}
    assert len(a) == 25 and len(b) == 25
    assert ids_a.isdisjoint(ids_b)          # no overlap
    assert len(ids_a | ids_b) == 50         # full coverage
