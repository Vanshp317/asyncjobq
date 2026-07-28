-- asyncjobq schema
-- A single table models the whole queue. Job lifecycle is encoded in `status`:
--   queued    -> ready (or scheduled, via run_at) to be claimed
--   running   -> leased by a worker; reclaimable once lease_expires_at passes
--   succeeded -> terminal success
--   dead      -> terminal failure (exhausted retries) == dead-letter

CREATE TABLE IF NOT EXISTS jobs (
    id               BIGSERIAL   PRIMARY KEY,
    queue            TEXT        NOT NULL DEFAULT 'default',
    task             TEXT        NOT NULL,
    payload          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status           TEXT        NOT NULL DEFAULT 'queued'
                         CHECK (status IN ('queued', 'running', 'succeeded', 'dead')),
    idempotency_key  TEXT,
    attempts         INT         NOT NULL DEFAULT 0,
    max_attempts     INT         NOT NULL DEFAULT 5,
    run_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by        TEXT,
    locked_at        TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    last_error       TEXT,
    result           JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The hot path: claiming work. A partial index over only queued, due jobs
-- keeps the index tiny and the ORDER BY cheap regardless of table size.
CREATE INDEX IF NOT EXISTS idx_jobs_claim
    ON jobs (queue, run_at, id)
    WHERE status = 'queued';

-- The reaper path: find running jobs whose lease has expired (dead workers).
CREATE INDEX IF NOT EXISTS idx_jobs_lease
    ON jobs (lease_expires_at)
    WHERE status = 'running';

-- Submission-side idempotency: a key, if supplied, identifies one job forever.
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency
    ON jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
