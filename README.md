# asyncjobq

A durable asynchronous job queue built on PostgreSQL with `asyncio`. Workers
pull jobs, run handlers concurrently, and the system survives crashes,
duplicate submissions, and slow/poison jobs. No broker, no extra
infrastructure — just Postgres.

This is a from-scratch implementation of the same primitives you find in SQS,
Sidekiq, or Celery, written to make the hard parts explicit rather than hidden
behind a library.

## Features

- **Atomic claiming** via `SELECT ... FOR UPDATE SKIP LOCKED` — concurrent
  workers never claim the same job, with no application-level locks.
- **Bounded concurrency** per worker (asyncio, a fixed number of in-flight jobs).
- **Submission idempotency** — an optional idempotency key dedupes enqueues, so
  a producer that retries never creates duplicate work.
- **At-least-once delivery with retries** — exponential backoff with full jitter,
  configurable `max_attempts`.
- **Leases / visibility timeout** — a claimed job is leased for a fixed window;
  a reaper returns expired leases to the queue, so a crashed worker's job is
  retried elsewhere.
- **Lease heartbeats** — long-running jobs renew their lease so the reaper
  doesn't steal them mid-flight.
- **Fencing** — a worker that lost its lease can't complete or clobber the job's
  new owner.
- **Dead-letter state** — jobs that exhaust their retries are parked, and can be
  inspected and requeued operationally.
- **Scheduled / delayed jobs** — `delay=` enqueues a job to run in the future.
- **Named queues** — isolate workloads (`emails`, `reports`, ...).
- **Graceful shutdown** — on SIGINT/SIGTERM the worker stops claiming and drains
  in-flight jobs before exiting.

## Using it (CLI)

After `pip install -e .` you get a `jobq` command. A job queue is a long-lived
system, so the natural way to use it is to run workers and push jobs at them.
Open a few terminals (all with `JOBQ_DSN` set, or pass `--dsn`):

```bash
jobq init                                  # create the schema (once)

# Terminal A — a worker
jobq worker --concurrency 4

# Terminal B — a second worker (watch them split the load)
jobq worker --concurrency 4

# Terminal C — push work in and watch it drain
jobq enqueue flaky --count 50 --payload '{"fail_rate":0.5}'
jobq watch                                 # live: queued / running / succeeded / dead
```

Things worth trying, because they're the demos that land in interviews:

- **Concurrency:** run two workers, enqueue 50 jobs, and watch them divide the
  work with no duplicates.
- **Crash recovery:** while a worker is mid-job, kill it hard (`kill -9` / close
  the terminal). Within a lease window the reaper returns its job to `queued`
  and the other worker finishes it.
- **Dead-lettering:** `jobq enqueue flaky --count 10 --payload '{"fail_rate":1}' --max-attempts 2`,
  let it run, then `jobq stats` shows them `dead`; `jobq requeue-dead` revives them.

The worker ships with demo handlers (`echo`, `sleep`, `flaky`) so it works out of
the box. For a real app you register your own — see `register_demo_handlers()`
in `jobq/cli.py` for the pattern, or use the library directly (below).

## Quick start (tests)

```bash
# 1. Start Postgres
docker compose up -d

# 2. Install
pip install -e ".[test]"

# 3. Run the tests (spins through concurrency, crash recovery, fencing, ...)
JOBQ_DSN=postgres://postgres:postgres@127.0.0.1:5432/jobq pytest -q

# 4. Run the demo
JOBQ_DSN=postgres://postgres:postgres@127.0.0.1:5432/jobq python -m examples.demo
```

## Usage

```python
from jobq import JobQueue, Worker, create_pool, create_schema

pool = await create_pool("postgres://postgres:postgres@localhost:5432/jobq")
await create_schema(pool)
q = JobQueue(pool)

# Producer
await q.enqueue("send_email", {"to": "ada@example.com"},
                max_attempts=5, idempotency_key="welcome-ada")

# Consumer
worker = Worker(q, concurrency=8, lease_seconds=30)

@worker.handler("send_email")
async def send_email(job):
    await smtp.send(job.payload["to"])
    return {"sent": True}

worker.install_signal_handlers()
await worker.run()
```

## Architecture

One table, `jobs`, holds everything. A job's lifecycle is encoded in `status`:

```
                 claim (FOR UPDATE SKIP LOCKED)
   queued  ───────────────────────────────────────►  running
     ▲                                                   │
     │  fail + attempts < max  (retry, backoff run_at)    │ complete
     │  OR reaper recovers expired lease                  ▼
     └───────────────────────◄──────────────         succeeded
                                                          
   fail + attempts >= max  ──►  dead  ──(requeue_dead)──► queued
```

- `jobq/queue.py` owns **all** the SQL — the consistency-critical code lives in
  one auditable place.
- `jobq/worker.py` orchestrates: claim loop, concurrency limit, heartbeats,
  reaper, graceful drain. It contains no SQL.
- Three partial indexes keep the hot paths fast: claiming, lease recovery, and
  idempotency lookups.

## Interview talking points

Each design decision maps to a concept worth being able to defend out loud.

**"How do you stop two workers running the same job?"**
`FOR UPDATE SKIP LOCKED` inside the claim query. Each worker's transaction locks
a disjoint set of rows and skips rows others hold, so claiming is atomic without
a separate lock service. `tests/test_concurrency.py` runs 16 workers against 200
jobs and asserts every job is claimed exactly once.

**"What happens when a worker crashes mid-job?"**
Jobs are *leased*, not removed. A claim sets `lease_expires_at`; a reaper sweep
returns any `running` job whose lease expired back to `queued` (the same idea as
an SQS visibility timeout). `attempts` is incremented at *claim* time, not
failure time, so a crash still spends an attempt and crash-loops are bounded by
`max_attempts`.

**"So a job can run more than once. Isn't that a bug?"**
This is at-least-once delivery, and it's a deliberate tradeoff: exactly-once is
impossible across external side effects. Two defenses: (1) submission
idempotency keys dedupe producers; (2) handlers receive the job and can dedupe
their own side effects using the job id / idempotency key. The lease + heartbeat
narrows the duplicate-execution window; fencing (the `locked_by` guard on
`complete`) ensures a revived stale worker can't overwrite the new owner's
result.

**"How do retries not hammer a failing dependency?"**
Exponential backoff with full jitter (`backoff.py`) spreads retry times out, and
a failed job's `run_at` is pushed into the future so it isn't immediately
re-claimed.

**"Why Postgres instead of Redis/SQS?"**
Durability and transactions for free, one fewer piece of infra, and the claim
pattern is a well-trodden production approach. The storage layer is isolated in
`JobQueue`, so swapping in Redis would mean reimplementing one class.

## Tradeoffs & honest limitations

- **Polling, not push.** Workers poll on an interval. `LISTEN/NOTIFY` would cut
  latency — a natural next step.
- **At-least-once only.** See above; true exactly-once isn't offered (or
  achievable for arbitrary side effects).
- **Single-table contention.** Fine to tens of thousands of jobs/sec on one
  Postgres; beyond that you'd partition by queue or shard.

## Possible extensions (good "phase 2" resume bullets)

- `LISTEN/NOTIFY` to replace polling.
- Per-tenant **rate limiting** (token bucket) on claim.
- Priorities and fairness across queues.
- A small CLI / web dashboard reading `stats()`.
- Prometheus metrics (claim latency, queue depth, retry rate).
- Cancel-on-lost-lease (abort the handler when a heartbeat fails).
```
