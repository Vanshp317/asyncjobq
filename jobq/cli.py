"""Command-line interface for asyncjobq.

This makes the queue usable as a real tool: run long-lived workers in some
terminals, push jobs in from another, and watch them get processed.

    jobq init                         # create the schema
    jobq worker --concurrency 4       # run a worker (Ctrl-C to drain & stop)
    jobq enqueue flaky --count 50     # push 50 demo jobs
    jobq watch                        # live queue stats
    jobq stats                        # one-shot stats
    jobq requeue-dead                 # revive dead-lettered jobs

The worker ships with a few DEMO task handlers (echo / sleep / flaky) so you can
play with it out of the box. In a real app you'd register your own handlers —
see register_demo_handlers() for the pattern.

DSN comes from --dsn or the JOBQ_DSN env var.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys

from . import JobQueue, Worker, create_pool, create_schema

DEFAULT_DSN = os.environ.get("JOBQ_DSN", "postgres://postgres:postgres@127.0.0.1:5432/jobq")


def register_demo_handlers(worker: Worker) -> None:
    """Sample handlers so the CLI worker does something interesting.
    Replace these with your own @worker.handler('task') functions for real use.
    """

    @worker.handler("echo")
    async def echo(job):
        return {"echo": job.payload}

    @worker.handler("sleep")
    async def sleep_task(job):
        secs = float(job.payload.get("seconds", 1.0))
        await asyncio.sleep(secs)
        return {"slept": secs}

    @worker.handler("flaky")
    async def flaky(job):
        await asyncio.sleep(random.uniform(0.05, 0.3))
        if random.random() < float(job.payload.get("fail_rate", 0.4)):
            raise RuntimeError("transient failure")
        return {"ok": True}


# --------------------------------------------------------------------------- #
# commands
# --------------------------------------------------------------------------- #
async def cmd_init(args):
    pool = await create_pool(args.dsn)
    await create_schema(pool)
    await pool.close()
    print("schema created")


async def cmd_enqueue(args):
    pool = await create_pool(args.dsn)
    q = JobQueue(pool)
    payload = json.loads(args.payload) if args.payload else {}
    first_id = None
    for i in range(args.count):
        key = None
        if args.key:
            key = args.key if args.count == 1 else f"{args.key}-{i}"
        job = await q.enqueue(
            args.task, payload, queue=args.queue,
            max_attempts=args.max_attempts, delay=args.delay, idempotency_key=key,
        )
        first_id = first_id or job.id
    await pool.close()
    if args.count == 1:
        print(f"enqueued job {first_id} ({args.task}) on queue '{args.queue}'")
    else:
        print(f"enqueued {args.count} '{args.task}' jobs on queue '{args.queue}'")


async def cmd_worker(args):
    pool = await create_pool(args.dsn, max_size=max(10, args.concurrency + 2))
    q = JobQueue(pool)
    worker = Worker(
        q, queue_name=args.queue, concurrency=args.concurrency,
        poll_interval=args.poll_interval, lease_seconds=args.lease,
    )
    register_demo_handlers(worker)
    worker.install_signal_handlers()
    print(f"worker {worker.worker_id} on queue '{args.queue}' "
          f"(concurrency={args.concurrency}). Ctrl-C to stop.")
    try:
        await worker.run()
    finally:
        await pool.close()


def _format_stats(queue: str, s: dict) -> str:
    total = sum(s.values())
    return (f"queue={queue}  total={total}  "
            f"queued={s['queued']}  running={s['running']}  "
            f"succeeded={s['succeeded']}  dead={s['dead']}")


async def cmd_stats(args):
    pool = await create_pool(args.dsn)
    q = JobQueue(pool)
    print(_format_stats(args.queue, await q.stats(args.queue)))
    await pool.close()


async def cmd_watch(args):
    pool = await create_pool(args.dsn)
    q = JobQueue(pool)
    try:
        while True:
            s = await q.stats(args.queue)
            # \r + clear-to-end-of-line: refresh one line in place
            sys.stdout.write("\r\033[K" + _format_stats(args.queue, s))
            sys.stdout.flush()
            await asyncio.sleep(args.interval)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print()
    finally:
        await pool.close()


async def cmd_requeue_dead(args):
    pool = await create_pool(args.dsn)
    q = JobQueue(pool)
    n = await q.requeue_dead(args.queue)
    await pool.close()
    print(f"requeued {n} dead job(s) on queue '{args.queue}'")


# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="jobq", description="async job queue CLI")
    p.add_argument("--dsn", default=DEFAULT_DSN, help="Postgres DSN (or set JOBQ_DSN)")
    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("init", help="create the database schema")
    sp.set_defaults(func=cmd_init)

    sp = sub.add_parser("enqueue", help="enqueue one or more jobs")
    sp.add_argument("task", help="task name (e.g. echo, sleep, flaky)")
    sp.add_argument("--payload", help="JSON payload, e.g. '{\"seconds\": 2}'")
    sp.add_argument("--queue", default="default")
    sp.add_argument("--count", type=int, default=1)
    sp.add_argument("--max-attempts", type=int, default=5, dest="max_attempts")
    sp.add_argument("--delay", type=float, default=0.0, help="seconds to defer")
    sp.add_argument("--key", help="idempotency key (suffixed with index when --count>1)")
    sp.set_defaults(func=cmd_enqueue)

    sp = sub.add_parser("worker", help="run a worker process")
    sp.add_argument("--queue", default="default")
    sp.add_argument("--concurrency", type=int, default=4)
    sp.add_argument("--lease", type=float, default=30.0, help="lease seconds")
    sp.add_argument("--poll-interval", type=float, default=0.5, dest="poll_interval")
    sp.set_defaults(func=cmd_worker)

    sp = sub.add_parser("stats", help="print queue stats once")
    sp.add_argument("--queue", default="default")
    sp.set_defaults(func=cmd_stats)

    sp = sub.add_parser("watch", help="live-updating queue stats")
    sp.add_argument("--queue", default="default")
    sp.add_argument("--interval", type=float, default=0.5)
    sp.set_defaults(func=cmd_watch)

    sp = sub.add_parser("requeue-dead", help="move dead jobs back to queued")
    sp.add_argument("--queue", default="default")
    sp.set_defaults(func=cmd_requeue_dead)

    return p


def main(argv=None) -> None:
    args = build_parser().parse_args(argv)
    try:
        asyncio.run(args.func(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
