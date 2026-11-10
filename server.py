import argparse
import os
import re
import sys
import threading
import time
from concurrent import futures
from dataclasses import dataclass

import grpc

import counter_pb2
import counter_pb2_grpc
import kvstore_pb2
import kvstore_pb2_grpc
from clocks import LamportClock

# C3 fault injection. Each fault affects only the FIRST Increment this replica receives.
#   --delay-ms N                 apply the write, but reply N ms later (slower than the client deadline)
#   --fault crash-before-apply   the process dies as soon as the request arrives (write NOT applied)
#   --fault crash-after-apply    the process applies the write and dies before replying
FAULTS = ["crash-before-apply", "crash-after-apply"]


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, name="replica", log_path=None, fault=None, delay_ms=0):
        # A3: this single lock protects ALL shared state (_values and _seen).
        # The dedup check and the mutation run in one critical section, so neither
        # concurrent increments nor concurrent retries with the same key can interleave.
        self._lock = threading.Lock()
        self._values = {}  # counter_id -> int
        self._seen = {}    # idempotency_key -> (counter_id, resulting value)
        self.clock = LamportClock(name, log_path)  # B1: this replica's logical clock
        self.fault = fault        # C3: one of FAULTS or None
        self.delay_ms = delay_ms  # C3: delay of the first reply
        self._first_increment = True

    def snapshot(self):
        """Copy of all counter values (used by tests to compare replicas)."""
        with self._lock:
            return dict(self._values)

    def _is_first_increment(self):
        with self._lock:
            first = self._first_increment
            self._first_increment = False
            return first

    def _crash(self):
        self.clock.tick("FAULT", f"{self.fault}: process exits")
        os._exit(1)

    def Increment(self, request, context):
        self.clock.receive(
            f"Increment(counter={request.counter_id}, delta={request.delta})",
            request.lamport_time)

        if not request.idempotency_key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key is required")

        first = self._is_first_increment()
        if first and self.fault == "crash-before-apply":
            self._crash()

        with self._lock:
            # duplicate check and mutation in ONE critical section
            if request.idempotency_key in self._seen:
                _, value = self._seen[request.idempotency_key]
                was_duplicate = True
                self.clock.tick("DUP", f"key={request.idempotency_key[:8]} already applied -> {value}")
            else:
                value = self._values.get(request.counter_id, 0) + request.delta
                self._values[request.counter_id] = value
                self._seen[request.idempotency_key] = (request.counter_id, value)
                was_duplicate = False
                self.clock.tick("APPLY", f"counter={request.counter_id} -> {value}")

        if first and self.fault == "crash-after-apply":
            self._crash()
        if first and self.delay_ms:
            self.clock.tick("FAULT", f"reply delayed {self.delay_ms} ms")
            time.sleep(self.delay_ms / 1000)  # outside the lock: other requests are not blocked

        lamport_time = self.clock.tick("SEND", f"IncrementReply(new_value={value})")
        return counter_pb2.IncrementReply(
            new_value=value, was_duplicate=was_duplicate, lamport_time=lamport_time)

    def Get(self, request, context):
        self.clock.receive(f"Get(counter={request.counter_id})", request.lamport_time)

        with self._lock:
            found = request.counter_id in self._values
            value = self._values.get(request.counter_id, 0)

        lamport_time = self.clock.tick("SEND", f"GetReply(value={value}, found={found})")
        return counter_pb2.GetReply(value=value, found=found, lamport_time=lamport_time)


# ======================================================================
# Lab 6-7: key-value node with configurable consistency (KeyValueStore service)
# ======================================================================

LEASEHOLDER = "A"                 # Node 1 (port 50051) holds the lease for the keyspace [a-z]
KEY_RANGE = re.compile(r"^[a-z]")  # keys whose first character is a-z


def now_us():
    return time.time_ns() // 1000


class HybridClock:
    """Write timestamps: wall clock in microseconds, but never smaller than the last one
    issued or seen, so the timestamps of one node are strictly increasing."""

    def __init__(self):
        self._last = 0
        self._lock = threading.Lock()

    def next(self):
        with self._lock:
            self._last = max(now_us(), self._last + 1)
            return self._last

    def observe(self, timestamp):
        with self._lock:
            self._last = max(self._last, timestamp)


@dataclass(frozen=True)
class Version:
    value: str
    timestamp: int  # microseconds
    origin: str     # node that accepted the write

    def wins_over(self, other):
        """Last-Write-Wins: the higher timestamp wins; equal timestamps are broken by node name."""
        return other is None or (self.timestamp, self.origin) > (other.timestamp, other.origin)


class LWWStore:
    """key -> Version. Every update goes through apply(), so all replicas that receive the
    same set of versions end up with the same state, whatever the delivery order."""

    def __init__(self):
        self._lock = threading.Lock()
        self._data = {}

    def apply(self, key, version):
        with self._lock:
            if version.wins_over(self._data.get(key)):
                self._data[key] = version
                return True
            return False

    def get(self, key):
        with self._lock:
            return self._data.get(key)

    def entries(self):
        with self._lock:
            return list(self._data.items())


def parse_peers(text):
    """'B=localhost:50052,C=localhost:50053' -> {'B': 'localhost:50052', 'C': 'localhost:50053'}"""
    peers = {}
    for item in filter(None, (text or "").split(",")):
        name, addr = item.split("=", 1)
        peers[name.strip()] = addr.strip()
    return peers


class KVNode(kvstore_pb2_grpc.KeyValueStoreServicer):
    def __init__(self, node=LEASEHOLDER, peers=None, leaseholder=LEASEHOLDER, log_path=None):
        self.node = node
        self.peers = dict(peers or {})  # name -> address of every OTHER node
        self.leaseholder = leaseholder
        self.store = LWWStore()
        self.clock = HybridClock()
        self._out = open(log_path, "a", buffering=1) if log_path else sys.stderr

    @property
    def is_leaseholder(self):
        return self.node == self.leaseholder

    def log(self, event, text):
        print(f"[{self.node}] {event:<8} {text}", file=self._out, flush=True)

    def _check_key(self, key, context):
        if not KEY_RANGE.match(key):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          f"key {key!r} is outside the keyspace [a-z] of leaseholder {self.leaseholder}")

    def _new_version(self, request):
        """A client-supplied timestamp is used as is (deterministic tests); otherwise the clock assigns one."""
        if request.timestamp:
            self.clock.observe(request.timestamp)
            return Version(request.value, request.timestamp, self.node)
        return Version(request.value, self.clock.next(), self.node)

    def _local_reply(self, key, is_stale):
        version = self.store.get(key)
        if version is None:
            return kvstore_pb2.GetReply(found=False, is_stale=is_stale)
        return kvstore_pb2.GetReply(value=version.value, found=True,
                                    read_timestamp=version.timestamp, is_stale=is_stale)

    def Put(self, request, context):
        self._check_key(request.key, context)
        version = self._new_version(request)
        applied = self.store.apply(request.key, version)
        self.log("APPLY" if applied else "OLDER", f"{request.key}={request.value!r} ts={version.timestamp}")
        return kvstore_pb2.PutReply(success=True, acks_received=1)

    def Get(self, request, context):
        self._check_key(request.key, context)
        return self._local_reply(request.key, is_stale=not self.is_leaseholder)


def start_server(port=0, name=None, log_path=None, fault=None, delay_ms=0,
                 node=None, peers=None, leaseholder=LEASEHOLDER):
    """Start a server in the background. port=0 means 'pick any free port' (used by tests).
    One gRPC server hosts both services: Counter (Assignment 2) and KeyValueStore (Lab 6-7)."""
    servicer = CounterServicer(name or f"replica-{port}", log_path, fault, delay_ms)
    kv = KVNode(node or leaseholder, peers, leaseholder, log_path)
    # A3: requests are handled in parallel; shared state is guarded by locks.
    # 16 workers: a node may block one worker while it forwards a request to the leaseholder.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(kv, server)
    bound_port = server.add_insecure_port(f"[::]:{port}")
    server.start()
    return server, bound_port, servicer


def main():
    parser = argparse.ArgumentParser(description="Counter replica")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", default=None, help="process name in the event log, e.g. replica-A")
    parser.add_argument("--log", default=None, help="event log file (default: stderr)")
    parser.add_argument("--fault", choices=FAULTS, default=None,
                        help="crash on the first Increment, before or after applying it")
    parser.add_argument("--delay-ms", type=int, default=0,
                        help="delay the reply to the first Increment by this many ms")
    # Lab 6-7: key-value cluster
    parser.add_argument("--node", default=None,
                        help="node name in the KV cluster: A, B or C (default: the leaseholder, A)")
    parser.add_argument("--peers", default="",
                        help="the other nodes, e.g. B=localhost:50052,C=localhost:50053")
    parser.add_argument("--leaseholder", default=LEASEHOLDER, help="name of the leaseholder node for [a-z]")
    args = parser.parse_args()

    server, port, _ = start_server(args.port, args.name, args.log, args.fault, args.delay_ms,
                                   node=args.node, peers=parse_peers(args.peers),
                                   leaseholder=args.leaseholder)
    role = "leaseholder" if (args.node or args.leaseholder) == args.leaseholder else "follower"
    print(f"server listening on port {port} (KV node {args.node or args.leaseholder}, {role})", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
