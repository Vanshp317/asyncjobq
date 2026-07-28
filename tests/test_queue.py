import asyncio

import pytest

from jobq import JobQueue


async def test_enqueue_claim_complete(q: JobQueue):
    job = await q.enqueue("greet", {"name": "ada"})
    assert job.status == "queued"
    assert job.attempts == 0

    claimed = await q.claim("default", 10, "w1", lease_seconds=30)
    assert len(claimed) == 1
    c = claimed[0]
    assert c.id == job.id
    assert c.status == "running"
    assert c.attempts == 1            # incremented at claim time
    assert c.locked_by == "w1"
    assert c.payload == {"name": "ada"}

    ok = await q.complete(c.id, "w1", {"greeting": "hello ada"})
    assert ok is True

    fetched = await q.get(job.id)
    assert fetched.status == "succeeded"
    assert fetched.result == {"greeting": "hello ada"}
    assert fetched.locked_by is None


async def test_claim_returns_nothing_when_empty(q: JobQueue):
    assert await q.claim("default", 5, "w1", 30) == []


async def test_idempotency_key_dedupes(q: JobQueue):
    a = await q.enqueue("charge", {"cents": 500}, idempotency_key="order-42")
    b = await q.enqueue("charge", {"cents": 999}, idempotency_key="order-42")
    # Same key -> same job; the second enqueue is a no-op (original payload kept).
    assert a.id == b.id
    assert b.payload == {"cents": 500}

    stats = await q.stats()
    assert stats["queued"] == 1


async def test_delay_schedules_into_future(q: JobQueue):
    await q.enqueue("later", delay=60)
    # Not yet due, so a claim returns nothing.
    assert await q.claim("default", 5, "w1", 30) == []


async def test_separate_queues_are_isolated(q: JobQueue):
    await q.enqueue("t", queue="emails")
    await q.enqueue("t", queue="reports")
    claimed = await q.claim("emails", 10, "w1", 30)
    assert len(claimed) == 1
    assert claimed[0].queue == "emails"


async def test_retry_then_dead_letter(q: JobQueue):
    job = await q.enqueue("flaky", max_attempts=2)

    # Attempt 1: claim, fail with zero backoff so it's immediately due again.
    c = (await q.claim("default", 1, "w1", 30))[0]
    assert c.attempts == 1
    status = await q.fail(c.id, "w1", "boom", backoff=0)
    assert status == "queued"

    # Attempt 2: claim again, fail -> attempts now == max_attempts -> dead.
    c = (await q.claim("default", 1, "w1", 30))[0]
    assert c.attempts == 2
    status = await q.fail(c.id, "w1", "boom again", backoff=0)
    assert status == "dead"

    dead = await q.get(job.id)
    assert dead.status == "dead"
    assert dead.last_error == "boom again"

    # Dead jobs are not claimable...
    assert await q.claim("default", 5, "w2", 30) == []
    # ...until operationally requeued.
    n = await q.requeue_dead()
    assert n == 1
    again = await q.get(job.id)
    assert again.status == "queued"
    assert again.attempts == 0


async def test_backoff_failure_not_immediately_claimable(q: JobQueue):
    await q.enqueue("flaky", max_attempts=5)
    c = (await q.claim("default", 1, "w1", 30))[0]
    await q.fail(c.id, "w1", "err", backoff=60)  # retry scheduled 60s out
    assert await q.claim("default", 5, "w1", 30) == []
