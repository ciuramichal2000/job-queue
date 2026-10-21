import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from jobqueue import Queue, Worker  # noqa: E402


class Clock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t


def make(tmp=None, **kw):
    c = Clock()
    return c, Queue(tmp or ":memory:", clock=c, **kw)


def test_job_runs_once_and_is_marked_done():
    c, q = make()
    seen = []
    q.enqueue("send", {"to": "a@x.com"})
    w = Worker(q, {"send": seen.append})
    assert w.run_once() == "done" and w.run_once() is None
    assert seen == [{"to": "a@x.com"}] and q.stats() == {"done": 1}


def test_failed_job_backs_off_exponentially_then_dies():
    c, q = make(base_delay=2.0)
    jid = q.enqueue("boom", max_attempts=3)

    def boom(_):
        raise RuntimeError("down")
    w = Worker(q, {"boom": boom})
    assert w.run_once() == "retry" and q.get(jid)["run_at"] == 1002.0       # 2 s
    assert w.run_once() is None                                              # not due yet
    c.t = 1002.0
    assert w.run_once() == "retry" and q.get(jid)["run_at"] == 1006.0       # 4 s
    c.t = 1006.0
    assert w.run_once() == "dead"
    job = q.get(jid)
    assert job["status"] == "dead" and job["attempts"] == 3 and job["last_error"] == "RuntimeError: down"
    c.t = 99999.0
    assert w.run_once() is None                                              # dead jobs stay parked


def test_scheduled_job_waits_for_its_time_and_order_is_by_due_time():
    c, q = make()
    q.enqueue("late", run_at=1500.0)
    q.enqueue("now")
    order = []
    w = Worker(q, {"late": lambda p: order.append("late"), "now": lambda p: order.append("now")})
    assert w.run_until_idle() == 1 and order == ["now"]
    c.t = 1500.0
    assert w.run_until_idle() == 1 and order == ["now", "late"]


def test_idempotency_key_prevents_duplicate_jobs():
    c, q = make()
    a = q.enqueue("invoice", {"n": 1}, key="invoice-2024-001")
    b = q.enqueue("invoice", {"n": 1}, key="invoice-2024-001")
    assert a == b and q.stats() == {"pending": 1}


def test_crashed_worker_job_is_reclaimed_after_the_visibility_timeout():
    c, q = make(visibility=30.0)
    jid = q.enqueue("slow", max_attempts=3)
    first = q.claim()                                   # a worker takes it and then dies
    assert first.id == jid and q.claim() is None        # still locked
    c.t += 31
    second = q.claim()
    assert second.id == jid and second.attempts == 2    # somebody else gets it


def test_job_whose_worker_keeps_dying_ends_up_dead_not_looping_forever():
    c, q = make(visibility=10.0)
    jid = q.enqueue("crash", max_attempts=2)
    for _ in range(2):
        assert q.claim() is not None
        c.t += 11
    assert q.claim() is None and q.get(jid)["status"] == "dead" and q.get(jid)["last_error"] == "worker lost"


def test_unknown_handler_fails_the_job_instead_of_losing_it():
    c, q = make()
    jid = q.enqueue("mystery")
    assert Worker(q, {}).run_once() == "dead" and "no handler" in q.get(jid)["last_error"]


def test_two_queue_objects_on_one_file_never_claim_the_same_job(tmp_path):
    path = str(tmp_path / "q.db")
    c1, q1 = make(path)
    q2 = Queue(path, clock=c1)
    for i in range(6):
        q1.enqueue("work", {"i": i})
    got = []
    for q in (q1, q2, q1, q2, q1, q2, q1):
        j = q.claim()
        if j:
            got.append(j.id)
    assert sorted(got) == sorted(set(got)) and len(got) == 6
