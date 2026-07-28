"""Retry backoff computation."""
from __future__ import annotations

import random


def compute_backoff(
    attempts: int,
    *,
    base: float = 0.5,
    factor: float = 2.0,
    cap: float = 300.0,
    jitter: bool = True,
) -> float:
    """Return the delay in seconds before a job's next attempt.

    Exponential growth (base * factor**(attempts-1)) capped at `cap`, with
    optional "full jitter" (AWS-style) to avoid thundering-herd retries where
    many failed jobs all wake up at the same instant.

    `attempts` is the number of attempts already made (>= 1).
    """
    if attempts < 1:
        attempts = 1
    raw = base * (factor ** (attempts - 1))
    delay = min(raw, cap)
    if jitter:
        # full jitter: sample uniformly in [0, delay]
        delay = random.uniform(0.0, delay)
    return delay
