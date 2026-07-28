# GETTING STARTED

Follow these steps top to bottom. Commands are for macOS (your setup).

## 0. Prerequisites (install these once, if you don't have them)

- **Docker Desktop** — runs the Postgres database. Download from
  https://www.docker.com/products/docker-desktop/ , install it, and **open the
  app** so it's running (whale icon in the menu bar). Verify in a terminal:
  ```
  docker --version
  ```
- **Python 3.11+** — you already have Anaconda's Python. Verify:
  ```
  python --version
  ```

## 1. Get the code

Download `asyncjobq.zip`, then in Terminal:

```
cd ~/Downloads
unzip -o asyncjobq.zip
cd asyncjobq
```

You should now be inside the folder that contains `pyproject.toml` and `jobq/`.
(Run `ls` to confirm you see them.)

## 2. Start the database

Make sure Docker Desktop is open, then:

```
docker compose up -d
docker compose ps          # postgres should show "healthy"
```

## 3. Install the package

```
pip install -e ".[test]"
```

This installs the dependencies (asyncpg, pytest) AND creates the `jobq` command.

## 4. Set the database connection (do this in EVERY terminal you open)

```
export JOBQ_DSN=postgres://postgres:postgres@127.0.0.1:5432/jobq
```

## 5. Create the tables

```
jobq init
```

## 6. (Optional) Prove it works

```
pytest -q
```
Expect: `14 passed`.

---

# Running it

A job queue is a long-running system: a **worker** process waits for jobs while
**you push jobs in** from elsewhere. So open multiple Terminal windows. In EACH
new terminal, first run steps from section 4 again (cd into the folder + export
JOBQ_DSN):

```
cd ~/Downloads/asyncjobq
export JOBQ_DSN=postgres://postgres:postgres@127.0.0.1:5432/jobq
```

### Terminal 1 — start a worker (it stays running)

```
jobq worker --concurrency 4
```

### Terminal 2 — push jobs in, then watch them process

```
jobq enqueue flaky --count 50 --payload '{"fail_rate":0.5}'
jobq watch
```

`watch` shows one line that updates live:
`queued=… running=… succeeded=… dead=…`. As the worker processes jobs, the
numbers shift from queued toward succeeded. Terminal 1 prints per-job logs
(failures, retries, dead-letters). Press Ctrl-C in Terminal 1 to drain and stop.

### Optional: a second worker (Terminal 3)

Start another `jobq worker --concurrency 4` before enqueuing to watch two workers
split the load with no job processed twice.

## Useful commands

```
jobq stats                                   # one-shot counts
jobq watch                                   # live counts
jobq enqueue echo  --payload '{"hi":1}'      # simple task
jobq enqueue sleep --payload '{"seconds":2}' # slow task
jobq enqueue flaky --count 20                # tasks that randomly fail/retry
jobq requeue-dead                            # revive dead-lettered jobs
jobq --help                                  # all commands
```

## Stopping / cleanup

- Stop a worker: Ctrl-C in its terminal (it drains in-flight jobs first).
- Stop the database: `docker compose down` (add `-v` to also delete its data).

## Troubleshooting

- `command not found: jobq` → rerun step 3 in the same Python environment; or
  run it as `python -m jobq ...` instead of `jobq ...`.
- `cannot connect to the Docker daemon` → Docker Desktop isn't open. Open it.
- `connection refused` on port 5432 → `docker compose ps`; if not healthy yet,
  wait a few seconds and retry.
- `no configuration file provided` / `does not appear to be a Python project`
  → you're not inside the `asyncjobq` folder. `cd ~/Downloads/asyncjobq`.
