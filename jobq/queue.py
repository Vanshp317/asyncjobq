"""JobQueue: all the durable queue operations.

This class owns the SQL. Workers (see worker.py) orchestrate calls to it but
contain no SQL themselves, which keeps the consistency-critical logic in one
auditable place.
"""
from __future__ import annotations

import uuid
from typing import Any, Optional

import asyncpg

from .models import Job, JobStatus


class JobQueue:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    # ------------------------------------------------------------------ #
    # Producer side
    # ------------------------------------------------------------------ #
    async def enqueue(
        self,
        task: str,
        payload: Optional[dict[str, Any]] = None,
        *,
        queue: str = "default",
        max_attempts: int = 5,
        delay: float = 0.0,
        idempotency_key: Optional[str] = None,
    ) -> Job:
        """Insert a job. If `idempotency_key` is given and a job with that key
        already exists, no new row is created and the existing job is returned —
        so a producer that retries enqueue (e.g. after a network blip) never
        creates duplicates.
        """
        payload = payload or {}
        if idempotency_key is not None:
            row = await self.pool.fetchrow(
                """
                INSERT INTO jobs (queue, task, payload, max_attempts, run_at, idempotency_key)
                VALUES ($1, $2, $3, $4, now() + ($5 * interval '1 second'), $6)
                ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
                DO NOTHING
                RETURNING *
                """,
                queue, task, payload, max_attempts, delay, idempotency_key,
            )
            if row is None:  # conflict: the job already exists, fetch it
                row = await self.pool.fetchrow(
                    "SELECT * FROM jobs WHERE idempotency_key = $1", idempotency_key
                )
            return Job.from_record(row)

        row = await self.pool.fetchrow(
            """
            INSERT INTO jobs (queue, task, payload, max_attempts, run_at)
            VALUES ($1, $2, $3, $4, now() + ($5 * interval '1 second'))
            RETURNING *
            """,
            queue, task, payload, max_attempts, delay,
        )
        return Job.from_record(row)

    # ------------------------------------------------------------------ #
    # Consumer side
    # ------------------------------------------------------------------ #
    async def claim(
        self, queue: str, limit: int, worker_id: str, lease_seconds: float
    ) -> list[Job]:
        """Atomically lease up to `limit` due jobs for `worker_id`.

        FOR UPDATE SKIP LOCKED is the heart of the queue: concurrent workers
        running this exact statement will each lock a *disjoint* set of rows
        and skip rows another transaction already holds, so no two workers ever
        claim the same job — without any application-level locking.

        attempts is incremented here (at claim time, not failure time) so that a
        worker which crashes mid-job still "spends" an attempt, bounding crash
        loops by max_attempts.
        """
        if limit <= 0:
            return []
        rows = await self.pool.fetch(
            """
            WITH claimed AS (
                SELECT id FROM jobs
                WHERE queue = $1 AND status = 'queued' AND run_at <= now()
                ORDER BY run_at, id
                FOR UPDATE SKIP LOCKED
                LIMIT $2
            )
            UPDATE jobs j
            SET status = 'running',
                locked_by = $3,
                locked_at = now(),
                lease_expires_at = now() + ($4 * interval '1 second'),
                attempts = attempts + 1,
                updated_at = now()
            FROM claimed
            WHERE j.id = claimed.id
            RETURNING j.*
            """,
            queue, limit, worker_id, lease_seconds,
        )
        return [Job.from_record(r) for r in rows]

    async def renew_lease(
        self, job_id: int, worker_id: str, lease_seconds: float
    ) -> bool:
        """Extend the lease on a job this worker still owns (heartbeat).

        Returns False if the worker no longer owns the job (it was reaped and
        possibly reclaimed elsewhere) — the caller should stop work to avoid
        duplicating effort.
        """
        result = await self.pool.fetchval(
            """
            UPDATE jobs
            SET lease_expires_at = now() + ($3 * interval '1 second'),
                updated_at = now()
            WHERE id = $1 AND locked_by = $2 AND status = 'running'
            RETURNING id
            """,
            job_id, worker_id, lease_seconds,
        )
        return result is not None

    async def complete(
        self, job_id: int, worker_id: str, result: Any = None
    ) -> bool:
        """Mark a job succeeded. The `locked_by = worker_id` guard is a fencing
        check: a worker whose lease was stolen cannot clobber the job's new
        owner. Returns True only if this worker still legitimately owned it.
        """
        updated = await self.pool.fetchval(
            """
            UPDATE jobs
            SET status = 'succeeded', result = $3,
                locked_by = NULL, locked_at = NULL, lease_expires_at = NULL,
                last_error = NULL, updated_at = now()
            WHERE id = $1 AND locked_by = $2 AND status = 'running'
            RETURNING id
            """,
            job_id, worker_id, result,
        )
        return updated is not None

    async def fail(
        self, job_id: int, worker_id: str, error: str, backoff: float
    ) -> Optional[str]:
        """Record a failed attempt. If attempts remain, requeue with a future
        run_at (the backoff delay); otherwise move to the dead-letter state.

        Returns the resulting status ('queued' or 'dead'), or None if this
        worker no longer owned the job.
        """
        return await self.pool.fetchval(
            """
            UPDATE jobs
            SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
                run_at = CASE WHEN attempts >= max_attempts
                              THEN run_at
                              ELSE now() + ($4 * interval '1 second') END,
                last_error = $3,
                locked_by = NULL, locked_at = NULL, lease_expires_at = NULL,
                updated_at = now()
            WHERE id = $1 AND locked_by = $2 AND status = 'running'
            RETURNING status
            """,
            job_id, worker_id, error, backoff,
        )

    async def recover_expired_leases(self) -> int:
        """Reaper: return running jobs whose lease has expired back to 'queued'.

        This is how the system tolerates worker crashes — the same idea as an
        SQS visibility timeout. Returns the number of jobs recovered.
        """
        rows = await self.pool.fetch(
            """
            UPDATE jobs
            SET status = 'queued',
                locked_by = NULL, locked_at = NULL, lease_expires_at = NULL,
                updated_at = now()
            WHERE status = 'running' AND lease_expires_at < now()
            RETURNING id
            """
        )
        return len(rows)

    # ------------------------------------------------------------------ #
    # Introspection / ops
    # ------------------------------------------------------------------ #
    async def get(self, job_id: int) -> Optional[Job]:
        row = await self.pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
        return Job.from_record(row) if row else None

    async def stats(self, queue: str = "default") -> dict[str, int]:
        rows = await self.pool.fetch(
            "SELECT status, count(*) AS n FROM jobs WHERE queue = $1 GROUP BY status",
            queue,
        )
        counts = {s.value: 0 for s in JobStatus}
        for r in rows:
            counts[r["status"]] = r["n"]
        return counts

    async def requeue_dead(self, queue: str = "default") -> int:
        """Operational helper: move dead-letter jobs back to queued and reset
        their attempt counter so they get a fresh set of retries."""
        rows = await self.pool.fetch(
            """
            UPDATE jobs
            SET status = 'queued', attempts = 0, run_at = now(),
                last_error = NULL, updated_at = now()
            WHERE queue = $1 AND status = 'dead'
            RETURNING id
            """,
            queue,
        )
        return len(rows)

    @staticmethod
    def new_worker_id(prefix: str = "worker") -> str:
        return f"{prefix}-{uuid.uuid4().hex[:8]}"
