"""Exceptions raised by asyncjobq."""


class JobQueueError(Exception):
    """Base class for all asyncjobq errors."""


class UnknownTaskError(JobQueueError):
    """Raised when a claimed job names a task with no registered handler."""


class LeaseLostError(JobQueueError):
    """Raised/handled internally when a worker can no longer prove ownership
    of a job (its lease was reaped and the job reclaimed elsewhere)."""
