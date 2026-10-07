"""Task C2: unit and integration tests (arrange - act - assert, no manual steps)."""
import os
import threading

from clocks import LamportClock

FAST = {"base_backoff": 0.01}  # short backoff so retries against a stopped replica do not slow the suite


# ---------- the 8 mandated tests ----------

def test_increment_applies_delta(running_server):
    client = running_server.new_client()

    result = client.incr("likes:post-42", 5)

    assert result.committed and result.new_value == 5
    assert client.get("likes:post-42").value == 5


def test_duplicate_key_not_reapplied(running_server):
    client = running_server.new_client()

    r1 = client.incr("x", 5, key="k-1")
    r2 = client.incr("x", 5, key="k-1")  # retry with the same key

    assert r1.new_value == 5 and not r1.was_duplicate
    assert r2.new_value == 5 and r2.was_duplicate  # not 10


def test_concurrent_increments_exact(running_server):
    def worker():
        client = running_server.new_client()
        for _ in range(1000):
            client.incr("hot", 1)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert running_server.new_client().get("hot").value == 2000


def test_get_missing_counter(running_server):
    client = running_server.new_client()

    reply = client.get("does-not-exist")  # must not raise

    assert reply.found is False


def test_retry_after_timeout_is_safe(make_cluster):
    # the replica applies the first write but replies after 800 ms; the client deadline is 300 ms
    slow = make_cluster(1, {0: {"delay_ms": 800}})
    client = slow.new_client(timeout=0.3, **FAST)

    result = client.incr("t", 1)

    assert result.committed and result.new_value == 1
    assert result.was_duplicate              # the answer came from the retry, via the dedup store
    assert slow.snapshot(0) == {"t": 1}      # the counter moved exactly once
    assert slow.log(0).count("APPLY") == 1


def test_majority_commit_two_acks(cluster):
    cluster.stop(1)  # replica B is down
    client = cluster.new_client(**FAST)

    result = client.incr("x", 1)

    assert result.committed and result.acks == 2
    assert cluster.snapshot(0) == cluster.snapshot(2) == {"x": 1}


def test_no_commit_below_majority(cluster):
    client = cluster.new_client(**FAST)
    client.incr("x", 1)        # committed baseline: x = 1 on every replica
    cluster.stop(1)
    cluster.stop(2)

    result = client.incr("x", 5)

    assert not result.committed and result.acks == 1
    assert result.new_value is None          # the survivor's value (6) is NOT presented as committed
    assert cluster.snapshot(0) == {"x": 6}   # ...although the survivor applied it (anomaly, see report)


def test_replicas_converge(cluster):
    client = cluster.new_client()

    for i in range(30):
        client.incr(f"c{i % 3}", 1)

    assert cluster.snapshot(0) == cluster.snapshot(1) == cluster.snapshot(2) == {"c0": 10, "c1": 10, "c2": 10}


# ---------- extra edge cases (named in the grading rubric) ----------

def test_counter_ids_are_isolated(running_server):
    client = running_server.new_client()

    client.incr("a", 3)
    client.incr("b", 10)

    assert client.get("a").value == 3 and client.get("b").value == 10


def test_retry_storm_same_key_applied_once(cluster):
    # 20 concurrent retries of ONE logical operation, sent to all three replicas
    client = cluster.new_client()
    threads = [threading.Thread(target=client.incr, args=("storm", 7), kwargs={"key": "same-op"})
               for _ in range(20)]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for i in range(3):
        assert cluster.snapshot(i) == {"storm": 7}
        assert cluster.log(i).count("APPLY") == 1


# ---------- Task B1: Lamport clock rules ----------

def test_lamport_clock_rules():
    clock = LamportClock("p", os.devnull)

    assert clock.tick("SEND", "m") == 1          # local event: +1
    assert clock.receive("msg", 10) == 11        # receive: max(1, 10) + 1
    assert clock.receive("old msg", 3) == 12     # receive: max(11, 3) + 1
