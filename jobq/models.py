"""Domain model: Job and JobStatus."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    DEAD = "dead"


_FIELDS = (
    "id", "queue", "task", "payload", "status", "idempotency_key",
    "attempts", "max_attempts", "run_at", "locked_by", "locked_at",
    "lease_expires_at", "last_error", "result", "created_at", "updated_at",
)


@dataclass(slots=True)
class Job:
    id: int
    queue: str
    task: str
    payload: dict[str, Any]
    status: str
    idempotency_key: Optional[str]
    attempts: int
    max_attempts: int
    run_at: datetime
    locked_by: Optional[str]
    locked_at: Optional[datetime]
    lease_expires_at: Optional[datetime]
    last_error: Optional[str]
    result: Any
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_record(cls, record) -> "Job":
        """Build a Job from an asyncpg Record (or any mapping)."""
        return cls(**{name: record[name] for name in _FIELDS})
