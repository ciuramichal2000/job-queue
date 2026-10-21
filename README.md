# job-queue

A durable background job queue on SQLite, no dependencies. What it handles:

- **Retries with exponential backoff** (2 s, 4 s, 8 s ...) and a **dead-letter state** after the last attempt, with the error kept.
- **Scheduled jobs** (`run_at`) and ordering by due time.
- **Crash safety:** a job whose worker disappears is given to another worker after a visibility timeout, and ends up `dead` instead of looping forever when it keeps killing workers.
- **No double processing:** claiming takes the database write lock, so two workers on one file never get the same job.
- **Idempotency keys:** enqueuing the same work twice returns the existing job.

```python
q = Queue("jobs.db")
q.enqueue("send_invoice", {"id": 7}, key="invoice-7")
Worker(q, {"send_invoice": handler}).run_until_idle()
```

`python -m pytest` runs the tests; they use a fake clock, so backoff and timeouts are checked without waiting. Built with AI coding assistance.
