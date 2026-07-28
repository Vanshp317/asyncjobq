"""End-to-end demo.

Run a local Postgres (see docker-compose.yml), then:
    JOBQ_DSN=postgres://postgres:postgres@127.0.0.1:5432/jobq python -m examples.demo
"""
import asyncio
import logging
import os
import random

from jobq import JobQueue, Worker, create_pool, create_schema

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
DSN = os.environ.get("JOBQ_DSN", "postgres://postgres:postgres@127.0.0.1:5432/jobq")


async def main():
    pool = await create_pool(DSN)
    await create_schema(pool)
    await pool.execute("TRUNCATE jobs RESTART IDENTITY")
    q = JobQueue(pool)

    worker = Worker(q, concurrency=4, poll_interval=0.1, lease_seconds=15)

    @worker.handler("send_email")
    async def send_email(job):
        await asyncio.sleep(random.uniform(0.05, 0.2))  # pretend to do I/O
        if random.random() < 0.3:                       # 30% transient failure
            raise RuntimeError("smtp timeout")
        return {"sent_to": job.payload["to"]}

    # Producer: enqueue 20 emails, with idempotency so re-runs don't duplicate.
    for i in range(20):
        await q.enqueue(
            "send_email",
            {"to": f"user{i}@example.com"},
            max_attempts=5,
            idempotency_key=f"welcome-{i}",
        )

    # Run the worker until the queue is fully drained.
    async def until_done():
        while True:
            await asyncio.sleep(0.2)
            s = await q.stats()
            if s["queued"] == 0 and s["running"] == 0:
                worker.stop()
                return

    await asyncio.gather(worker.run(), until_done())

    print("\nfinal stats:", await q.stats())
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
