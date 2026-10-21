"""A small durable job queue on SQLite: retries with exponential backoff, scheduled jobs, a dead-letter state,
a visibility timeout that gives a crashed worker's job to somebody else, and idempotency keys."""
import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Callable, Dict, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  payload TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',      -- pending | running | done | dead
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  run_at REAL NOT NULL,
  locked_until REAL,
  last_error TEXT,
  key TEXT UNIQUE
);
CREATE INDEX IF NOT EXISTS jobs_due ON jobs(status, run_at);
"""


@dataclass
class Job:
    id: int
    name: str
    payload: dict
    attempts: int
    max_attempts: int


class Queue:
    def __init__(self, path: str = ":memory:", clock: Callable[[], float] = time.time, base_delay: float = 2.0, visibility: float = 30.0):
        self.db = sqlite3.connect(path, timeout=30, isolation_level=None)   # explicit transactions below
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self.clock, self.base_delay, self.visibility = clock, base_delay, visibility

    def enqueue(self, name: str, payload: Optional[dict] = None, run_at: Optional[float] = None, max_attempts: int = 3, key: Optional[str] = None) -> int:
        """With a key, enqueuing the same work twice returns the existing job instead of creating a duplicate."""
        if key:
            row = self.db.execute("SELECT id FROM jobs WHERE key=?", (key,)).fetchone()
            if row:
                return row["id"]
        cur = self.db.execute("INSERT INTO jobs(name,payload,run_at,max_attempts,key) VALUES(?,?,?,?,?)",
                              (name, json.dumps(payload or {}), self.clock() if run_at is None else run_at, max_attempts, key))
        return cur.lastrowid

    def claim(self) -> Optional[Job]:
        now = self.clock()
        self.db.execute("BEGIN IMMEDIATE")                     # one writer at a time, so two workers never get the same job
        try:
            # a worker that died while running: out of attempts -> dead, otherwise it becomes due again
            self.db.execute("UPDATE jobs SET status='dead', last_error=COALESCE(last_error,'worker lost') "
                            "WHERE status='running' AND locked_until<=? AND attempts>=max_attempts", (now,))
            row = self.db.execute("SELECT * FROM jobs WHERE (status='pending' AND run_at<=?) OR (status='running' AND locked_until<=?) "
                                  "ORDER BY run_at, id LIMIT 1", (now, now)).fetchone()
            if not row:
                self.db.execute("COMMIT")
                return None
            self.db.execute("UPDATE jobs SET status='running', attempts=attempts+1, locked_until=? WHERE id=?", (now + self.visibility, row["id"]))
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise
        return Job(row["id"], row["name"], json.loads(row["payload"]), row["attempts"] + 1, row["max_attempts"])

    def complete(self, job_id: int) -> None:
        self.db.execute("UPDATE jobs SET status='done', locked_until=NULL WHERE id=?", (job_id,))

    def fail(self, job: Job, error: str) -> str:
        """Back off 2s, 4s, 8s... (base_delay * 2^(attempt-1)); after the last attempt the job is parked as dead."""
        if job.attempts >= job.max_attempts:
            self.db.execute("UPDATE jobs SET status='dead', last_error=?, locked_until=NULL WHERE id=?", (error, job.id))
            return "dead"
        self.db.execute("UPDATE jobs SET status='pending', last_error=?, locked_until=NULL, run_at=? WHERE id=?",
                        (error, self.clock() + self.base_delay * 2 ** (job.attempts - 1), job.id))
        return "retry"

    def stats(self) -> Dict[str, int]:
        return {r["status"]: r["n"] for r in self.db.execute("SELECT status, COUNT(*) n FROM jobs GROUP BY status")}

    def get(self, job_id: int) -> dict:
        return dict(self.db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())


class Worker:
    def __init__(self, queue: Queue, handlers: Dict[str, Callable[[dict], None]]):
        self.queue, self.handlers = queue, handlers

    def run_once(self) -> Optional[str]:
        """Process one due job; returns 'done', 'retry', 'dead', or None when nothing is due."""
        job = self.queue.claim()
        if job is None:
            return None
        handler = self.handlers.get(job.name)
        if handler is None:
            return self.queue.fail(Job(job.id, job.name, job.payload, job.max_attempts, job.max_attempts), "no handler for %r" % job.name)
        try:
            handler(job.payload)
        except Exception as e:                                 # noqa: BLE001 (any handler error is a failed attempt)
            return self.queue.fail(job, "%s: %s" % (type(e).__name__, e))
        self.queue.complete(job.id)
        return "done"

    def run_until_idle(self, limit: int = 1000) -> int:
        n = 0
        while n < limit and self.run_once() is not None:
            n += 1
        return n
