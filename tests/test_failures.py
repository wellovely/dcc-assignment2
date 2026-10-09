"""Task C3: failure-injection tests.

Route (a), process control: every replica is a real `python server.py` process started with
subprocess.Popen and killed with proc.kill(). Each test asserts an externally observable invariant.
"""
import os
import socket
import subprocess
import sys
import threading
import time

import pytest

from client import CounterClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FAST = {"base_backoff": 0.01}


def free_port():
    with socket.socket() as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


def wait_until_listening(port, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as s:
            if s.connect_ex(("localhost", port)) == 0:
                return
        time.sleep(0.05)
    raise TimeoutError(f"replica on port {port} did not start")


class Replicas:
    """Starts, kills and restarts replica processes for one test."""

    def __init__(self, log_dir):
        self.log_dir = log_dir
        self.procs = {}   # name -> Popen
        self.ports = {}   # name -> port

    def start(self, name, *flags):
        port = self.ports.setdefault(name, free_port())  # a restart reuses the same port
        self.procs[name] = subprocess.Popen(
            [sys.executable, "server.py", "--port", str(port), "--name", name,
             "--log", str(self.log_dir / f"{name}.log"), *flags],
            cwd=ROOT, stdout=subprocess.DEVNULL)
        wait_until_listening(port)

    def kill(self, name):
        self.procs[name].kill()
        self.procs[name].wait()

    def addresses(self):
        return [f"localhost:{self.ports[n]}" for n in sorted(self.ports)]

    def value(self, name, counter_id):
        """Read one replica directly."""
        client = CounterClient([f"localhost:{self.ports[name]}"], log_path=os.devnull, **FAST)
        reply = client.get(counter_id)
        client.close()
        return reply.value

    def log(self, name):
        return (self.log_dir / f"{name}.log").read_text()

    def stop_all(self):
        for proc in self.procs.values():
            proc.kill()
            proc.wait()


@pytest.fixture
def replicas(tmp_path):
    r = Replicas(tmp_path)
    yield r
    r.stop_all()


# ---------- the 3 mandated scenarios ----------

def test_replica_crash_mid_request(replicas):
    replicas.start("replica-A")
    replicas.start("replica-B", "--delay-ms", "1000")  # keeps the write in flight for 1 s
    replicas.start("replica-C")
    client = CounterClient(replicas.addresses(), log_path=os.devnull, **FAST)

    killer = threading.Timer(0.3, replicas.kill, args=("replica-B",))  # kill B while the write is in flight
    killer.start()
    result = client.incr("x", 1)  # must not raise
    killer.join()

    assert result.committed and result.acks == 2
    assert replicas.value("replica-A", "x") == replicas.value("replica-C", "x") == 1


def test_request_duplication(replicas):
    for name in ("replica-A", "replica-B", "replica-C"):
        replicas.start(name)
    client = CounterClient(replicas.addresses(), log_path=os.devnull)

    first = client.incr("x", 5, key="dup-key")
    second = client.incr("x", 5, key="dup-key")

    assert not first.was_duplicate and second.was_duplicate
    assert second.new_value == 5
    for name in ("replica-A", "replica-B", "replica-C"):
        assert replicas.value(name, "x") == 5  # moved once, not twice


def test_induced_timeout_with_retry(replicas):
    replicas.start("replica-A")
    replicas.start("replica-B", "--delay-ms", "1500")  # slower than the 0.5 s client deadline
    replicas.start("replica-C")
    client = CounterClient(replicas.addresses(), timeout=0.5, log_path=os.devnull, **FAST)

    result = client.incr("x", 1)

    assert result.committed and result.acks == 3  # B's retry succeeded too
    assert replicas.value("replica-B", "x") == 1  # exactly once on the slow replica
    log = replicas.log("replica-B")
    assert log.count("RECV   Increment") == 2 and log.count("APPLY") == 1 and log.count("DUP") == 1


# ---------- fault matrix: crash before / after applying the write ----------

@pytest.mark.parametrize("fault, b_applied", [("crash-before-apply", False), ("crash-after-apply", True)])
def test_crash_before_and_after_apply(replicas, fault, b_applied):
    replicas.start("replica-A")
    replicas.start("replica-B", "--fault", fault)
    replicas.start("replica-C")
    client = CounterClient(replicas.addresses(), log_path=os.devnull, **FAST)

    result = client.incr("x", 1)

    assert result.committed and result.acks == 2      # either way, the write commits on A and C
    assert replicas.procs["replica-B"].wait(timeout=5) != 0  # B really crashed
    assert ("APPLY" in replicas.log("replica-B")) == b_applied


# ---------- scripted restart: committed writes are never lost on the majority ----------

def test_restart_does_not_lose_committed_writes(replicas):
    for name in ("replica-A", "replica-B", "replica-C"):
        replicas.start(name)
    committed = 0

    for round_ in range(3):                    # three crash/restart rounds
        client = CounterClient(replicas.addresses(), log_path=os.devnull, **FAST)
        committed += sum(client.incr("x", 1).committed for _ in range(5))   # all up
        replicas.kill("replica-B")
        committed += sum(client.incr("x", 1).committed for _ in range(5))   # B down: 2/3
        client.close()
        replicas.start("replica-B")            # B comes back EMPTY (state is in memory only)

    assert committed == 30                                  # every write committed
    assert replicas.value("replica-A", "x") == 30           # nothing committed is lost on A and C
    assert replicas.value("replica-C", "x") == 30
    assert replicas.value("replica-B", "x") == 0            # B lost its state: known limitation, see report
