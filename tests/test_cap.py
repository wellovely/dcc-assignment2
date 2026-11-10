"""Lab 6-7, Task 3: CAP behaviour under a network partition.

Failure injection by SOCKET INTERCEPTION: every node is a real `python server.py` process, but it
reaches its peers only through a small TCP proxy, one proxy per directed link (A->B, A->C, ...).
Cutting a link makes its proxy drop the open connections and refuse new ones, which is what a node
sees during a real network partition. The client talks to each node directly, like a client that
sits in the same data center as that node.

    DC-1: A (leaseholder), B        DC-2: C
"""
import itertools
import socket
import subprocess
import sys
import threading

import pytest

from client import KVClient
from kvstore_pb2 import FOLLOWER_STALE, LINEARIZABLE, QUORUM_CP
from test_failures import ROOT, free_port, wait_until_listening

NODES = "ABC"
DC1, DC2 = {"A", "B"}, {"C"}


# ---------------------------------------------------------------- network simulation

class Network:
    """Which directed links are cut. All proxies consult it for every new connection."""

    def __init__(self):
        self._cut = set()  # {(src, dst)}
        self._lock = threading.Lock()
        self.proxies = {}  # (src, dst) -> LinkProxy

    def is_cut(self, src, dst):
        with self._lock:
            return (src, dst) in self._cut

    def partition(self, *groups):
        """partition({'A','B'}, {'C'}): cut every link between nodes of different groups, both ways."""
        links = {(a, b) for g1, g2 in itertools.permutations(groups, 2) for a in g1 for b in g2}
        with self._lock:
            self._cut |= links
        for link in links:
            self.proxies[link].drop_connections()

    def heal(self):
        with self._lock:
            self._cut.clear()


class LinkProxy:
    """TCP forwarder for one directed link src -> dst (gRPC/HTTP2 bytes pass through unchanged)."""

    def __init__(self, src, dst, target_port, network):
        self.src, self.dst, self.target_port, self.network = src, dst, target_port, network
        self._listener = socket.socket()
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(64)
        self.port = self._listener.getsockname()[1]
        self._open = set()  # sockets of live connections
        self._lock = threading.Lock()
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def _accept_loop(self):
        while True:
            try:
                downstream, _ = self._listener.accept()
            except OSError:
                return  # proxy closed
            if self.network.is_cut(self.src, self.dst):
                downstream.close()  # partitioned: the connection dies immediately
                continue
            try:
                upstream = socket.create_connection(("127.0.0.1", self.target_port))
            except OSError:
                downstream.close()  # target node is down
                continue
            with self._lock:
                self._open |= {downstream, upstream}
            threading.Thread(target=self._pump, args=(downstream, upstream), daemon=True).start()
            threading.Thread(target=self._pump, args=(upstream, downstream), daemon=True).start()

    def _pump(self, source, sink):
        try:
            while data := source.recv(65536):
                sink.sendall(data)
        except OSError:
            pass
        finally:
            self._kill(source)
            self._kill(sink)

    def _kill(self, sock):
        with self._lock:
            self._open.discard(sock)
        try:
            sock.shutdown(socket.SHUT_RDWR)  # wakes up the thread blocked in recv()
        except OSError:
            pass
        sock.close()

    def drop_connections(self):
        with self._lock:
            open_now = list(self._open)
        for sock in open_now:
            self._kill(sock)

    def close(self):
        self._listener.close()
        self.drop_connections()


# ---------------------------------------------------------------- cluster of real processes

class CapCluster:
    """Three server.py processes wired to each other through LinkProxy objects."""

    def __init__(self, log_dir, sync_interval_ms=100, write_quorum=None):
        self.log_dir = log_dir
        self.net = Network()
        self.ports = {n: free_port() for n in NODES}
        for src, dst in itertools.permutations(NODES, 2):
            self.net.proxies[(src, dst)] = LinkProxy(src, dst, self.ports[dst], self.net)

        self.procs = {}
        for n in NODES:
            peers = ",".join(f"{m}=127.0.0.1:{self.net.proxies[(n, m)].port}" for m in NODES if m != n)
            cmd = [sys.executable, "server.py", "--port", str(self.ports[n]), "--node", n,
                   "--peers", peers, "--log", str(log_dir / f"node-{n}.log"),
                   "--sync-interval-ms", str(sync_interval_ms)]
            if write_quorum:
                cmd += ["--write-quorum", str(write_quorum)]
            self.procs[n] = subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.DEVNULL)
        for n in NODES:
            wait_until_listening(self.ports[n])
        self.client = KVClient({n: f"127.0.0.1:{self.ports[n]}" for n in NODES}, timeout=3.0)

    def partition(self, *groups):
        self.net.partition(*groups)

    def heal(self):
        self.net.heal()

    def local_value(self, node, key):
        """What `node` holds locally right now (unbounded follower read), or None."""
        reply = self.client.get(key, FOLLOWER_STALE, 0, node=node)
        return reply.value if reply.found else None

    def log(self, node):
        return (self.log_dir / f"node-{node}.log").read_text()

    def shutdown(self):
        self.client.close()
        for proc in self.procs.values():
            proc.kill()
            proc.wait()
        for proxy in self.net.proxies.values():
            proxy.close()


@pytest.fixture
def make_cluster(tmp_path):
    clusters = []

    def make(**options):
        clusters.append(CapCluster(tmp_path, **options))
        return clusters[-1]

    yield make
    for c in clusters:
        c.shutdown()


@pytest.fixture
def cap(make_cluster):
    """Default cluster: majority writes (W=2), anti-entropy every 100 ms."""
    return make_cluster()


# ---------------------------------------------------------------- Part A: CP mode (strict quorum)

def test_cp_write_healthy_cluster_reaches_all_replicas(cap):
    reply = cap.client.put("x", "v1", QUORUM_CP, node="B")  # B forwards to the leaseholder A
    assert reply.success and reply.acks_received == 3
    assert all(cap.local_value(n, "x") == "v1" for n in NODES)


def test_cp_write_on_majority_partition_succeeds(cap):
    cap.partition(DC1, DC2)
    reply = cap.client.put("x", "dc1", QUORUM_CP, node="A")
    assert reply.success, reply.error_message
    assert reply.acks_received == 2  # A + B; C is unreachable
    assert cap.local_value("B", "x") == "dc1"
    assert cap.local_value("C", "x") is None  # the minority never saw it


def test_cp_write_on_minority_partition_fails(cap):
    cap.partition(DC1, DC2)
    reply = cap.client.put("x", "dc2", QUORUM_CP, node="C")
    assert not reply.success
    assert "ReplicaUnavailableError" in reply.error_message and "quorum lost" in reply.error_message
    # consistency is kept: the rejected write is visible nowhere
    assert all(cap.local_value(n, "x") is None for n in NODES)
    assert cap.client.get("x", LINEARIZABLE, node="A").found is False
