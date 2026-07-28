"""Worker: pulls jobs from a JobQueue and runs registered handlers.

Handlers are async callables that take a Job and may return a JSON-serializable
result. Raising any exception marks the attempt failed (and triggers retry or
dead-lettering).
"""
from __future__ import annotations

import asyncio
import logging
import signal
from typing import Awaitable, Callable

from .backoff import compute_backoff
from .models import Job
from .queue import JobQueue

log = logging.getLogger("jobq.worker")

Handler = Callable[[Job], Awaitable[object]]


class Worker:
    def __init__(
        self,
        queue: JobQueue,
        *,
        queue_name: str = "default",
        concurrency: int = 4,
        poll_interval: float = 1.0,
        lease_seconds: float = 30.0,
        heartbeat_interval: float = 10.0,
        reap_interval: float = 5.0,
        worker_id: str | None = None,
        backoff_base: float = 0.5,
        backoff_cap: float = 300.0,
    ):
        self.queue = queue
        self.queue_name = queue_name
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self.lease_seconds = lease_seconds
        self.heartbeat_interval = heartbeat_interval
        self.reap_interval = reap_interval
        self.worker_id = worker_id or JobQueue.new_worker_id()
        self.backoff_base = backoff_base
        self.backoff_cap = backoff_cap

        self._handlers: dict[str, Handler] = {}
        self._running: set[asyncio.Task] = set()
        self._stop = asyncio.Event()

    # ------------------------------------------------------------------ #
    def register(self, task: str, handler: Handler) -> None:
        self._handlers[task] = handler

    def handler(self, task: str) -> Callable[[Handler], Handler]:
        """Decorator form: @worker.handler('send_email')."""
        def deco(fn: Handler) -> Handler:
            self.register(task, fn)
            return fn
        return deco

    def stop(self) -> None:
        self._stop.set()

    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stop)
            except NotImplementedError:  # e.g. Windows
                pass

    # ------------------------------------------------------------------ #
    async def run(self) -> None:
        """Main loop. Claims work up to the concurrency limit, runs it, and on
        stop() drains in-flight jobs before returning (graceful shutdown)."""
        loop = asyncio.get_running_loop()
        last_reap = loop.time()
        log.info("worker %s starting (concurrency=%d)", self.worker_id, self.concurrency)

        while not self._stop.is_set():
            now = loop.time()
            if now - last_reap >= self.reap_interval:
                recovered = await self.queue.recover_expired_leases()
                if recovered:
                    log.info("reaper recovered %d expired job(s)", recovered)
                last_reap = now

            free = self.concurrency - len(self._running)
            claimed: list[Job] = []
            if free > 0:
                claimed = await self.queue.claim(
                    self.queue_name, free, self.worker_id, self.lease_seconds
                )

            for job in claimed:
                task = asyncio.create_task(self._execute(job))
                self._running.add(task)
                task.add_done_callback(self._running.discard)

            if not claimed:
                # Nothing to do, so sleep but wake immediately on stop().
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self.poll_interval)
                except asyncio.TimeoutError:
                    pass

        # Drain: let in-flight jobs finish so we never abandon committed work.
        if self._running:
            log.info("draining %d in-flight job(s)", len(self._running))
            await asyncio.gather(*self._running, return_exceptions=True)
        log.info("worker %s stopped", self.worker_id)

    # ------------------------------------------------------------------ #
    async def _execute(self, job: Job) -> None:
        handler = self._handlers.get(job.task)
        if handler is None:
            backoff = compute_backoff(job.attempts, base=self.backoff_base, cap=self.backoff_cap)
            await self.queue.fail(
                job.id, self.worker_id, f"no handler registered for task {job.task!r}", backoff
            )
            log.warning("no handler for task %r (job %d)", job.task, job.id)
            return

        stop_hb = asyncio.Event()
        hb = asyncio.create_task(self._heartbeat(job, stop_hb))
        try:
            result = await handler(job)
            ok = await self.queue.complete(job.id, self.worker_id, result)
            if not ok:
                log.warning("job %d completed but lease was lost; result discarded", job.id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - any handler error is a failed attempt
            backoff = compute_backoff(job.attempts, base=self.backoff_base, cap=self.backoff_cap)
            status = await self.queue.fail(job.id, self.worker_id, repr(exc), backoff)
            if status == "dead":
                log.error("job %d dead-lettered after %d attempts: %r", job.id, job.attempts, exc)
            else:
                log.info("job %d failed (attempt %d), retrying in ~%.1fs: %r",
                         job.id, job.attempts, backoff, exc)
        finally:
            stop_hb.set()
            await hb

    async def _heartbeat(self, job: Job, stop: asyncio.Event) -> None:
        """Periodically extend the lease so the reaper doesn't reclaim a job
        that's still legitimately being worked on."""
        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.heartbeat_interval)
                return  # handler finished
            except asyncio.TimeoutError:
                alive = await self.queue.renew_lease(job.id, self.worker_id, self.lease_seconds)
                if not alive:
                    log.warning("lost lease on job %d during heartbeat", job.id)
                    return
