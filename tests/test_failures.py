import asyncio

import pytest

from jobq import JobQueue, Worker


async def test_expired_lease_is_recovered(q: JobQueue):
    """Simulate a worker crash: it claims a job with a tiny lease and never
    finishes. The reaper must return the job to 'queued' so another worker can
    pick it up."""
    await q.enqueue("work")
    c = (await q.claim("default", 1, "dead-worker", lease_seconds=0.2))[0]
    assert c.status == "running"

    # Before expiry, nobody else can claim it.
    assert await q.claim("default", 5, "w2", 30) == []

    await asyncio.sleep(0.3)  # lease expires
    recovered = await q.recover_expired_leases()
    assert recovered == 1

    # Now a healthy worker can claim it; note attempts keeps climbing.
    again = await q.claim("default", 1, "w2", 30)
    assert len(again) == 1
    assert again[0].attempts == 2


async def test_stale_worker_cannot_complete_after_reclaim(q: JobQueue):
    """Fencing: once a job is reaped and reclaimed by w2, the original w1 must
    not be able to mark it succeeded."""
    await q.enqueue("work")
    c1 = (await q.claim("default", 1, "w1", lease_seconds=0.2))[0]
    await asyncio.sleep(0.3)
    await q.recover_expired_leases()
    c2 = (await q.claim("default", 1, "w2", lease_seconds=30))[0]
    assert c2.id == c1.id

    # w1 comes back from the dead and tries to complete, but must be rejected.
    stolen = await q.complete(c1.id, "w1", {"oops": True})
    assert stolen is False

    # w2 is the legitimate owner and can complete.
    ok = await q.complete(c2.id, "w2", {"ok": True})
    assert ok is True


async def test_heartbeat_keeps_job_alive(q: JobQueue):
    await q.enqueue("work")
    c = (await q.claim("default", 1, "w1", lease_seconds=0.3))[0]

    # Renew before expiry; lease should still belong to w1.
    await asyncio.sleep(0.15)
    assert await q.renew_lease(c.id, "w1", lease_seconds=0.3) is True

    await asyncio.sleep(0.15)
    # Without the renew above this would have expired; instead it's still live.
    assert await q.recover_expired_leases() == 0


async def test_renew_lease_fails_for_non_owner(q: JobQueue):
    await q.enqueue("work")
    c = (await q.claim("default", 1, "w1", 30))[0]
    assert await q.renew_lease(c.id, "someone-else", 30) is False


async def test_worker_end_to_end(q: JobQueue):
    """Drive a real Worker: enqueue work, run handlers, confirm success and that
    an always-failing task lands in the dead-letter state."""
    processed: list[int] = []

    worker = Worker(q, concurrency=4, poll_interval=0.05, lease_seconds=30, worker_id="e2e")

    @worker.handler("square")
    async def square(job):
        await asyncio.sleep(0)  # yield, simulate async I/O
        n = job.payload["n"]
        processed.append(n)
        return {"squared": n * n}

    @worker.handler("always_fail")
    async def always_fail(job):
        raise RuntimeError("nope")

    for i in range(10):
        await q.enqueue("square", {"n": i})
    await q.enqueue("always_fail", max_attempts=1)

    # Run the worker until everything is terminal, then stop it.
    async def stopper():
        for _ in range(200):  # up to ~10s
            await asyncio.sleep(0.05)
            s = await q.stats()
            if s["queued"] == 0 and s["running"] == 0:
                worker.stop()
                return
        worker.stop()

    await asyncio.gather(worker.run(), stopper())

    stats = await q.stats()
    assert stats["succeeded"] == 10
    assert stats["dead"] == 1
    assert sorted(processed) == list(range(10))
