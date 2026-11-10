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
PEER_TIMEOUT = 0.5                # deadline (s) of one node-to-node call
# After a partition heals, reconnect within ~0.5 s (gRPC's default backoff grows up to 120 s).
CHANNEL_OPTIONS = [
    ("grpc.initial_reconnect_backoff_ms", 100),
    ("grpc.min_reconnect_backoff_ms", 100),
    ("grpc.max_reconnect_backoff_ms", 500),
]


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


def to_entry(key, version):
    return kvstore_pb2.Entry(key=key, value=version.value, timestamp=version.timestamp, origin=version.origin)


class KVNode(kvstore_pb2_grpc.KeyValueStoreServicer, kvstore_pb2_grpc.ReplicaSyncServicer):
    def __init__(self, node=LEASEHOLDER, peers=None, leaseholder=LEASEHOLDER, log_path=None,
                 write_quorum=None):
        self.node = node
        self.peers = dict(peers or {})  # name -> address of every OTHER node
        self.leaseholder = leaseholder
        self.n = len(self.peers) + 1
        self.write_quorum = write_quorum or self.n // 2 + 1  # W, default majority: 2 of 3
        self.store = LWWStore()
        self.clock = HybridClock()
        self._out = open(log_path, "a", buffering=1) if log_path else sys.stderr

        channels = {name: grpc.insecure_channel(addr, options=CHANNEL_OPTIONS)
                    for name, addr in self.peers.items()}
        self._kv_stubs = {name: kvstore_pb2_grpc.KeyValueStoreStub(c) for name, c in channels.items()}
        self._sync_stubs = {name: kvstore_pb2_grpc.ReplicaSyncStub(c) for name, c in channels.items()}
        self._pool = futures.ThreadPoolExecutor(max_workers=max(1, 2 * len(self.peers)))

        # idempotency_key -> PutReply of a successful write, so a client retry is not applied twice
        self._seen = {}
        self._seen_lock = threading.Lock()

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

    def _replicate_to_peers(self, key, version):
        """Send the version to every peer in parallel. Returns the names of the peers that acknowledged."""
        request = kvstore_pb2.ReplicateRequest(entry=to_entry(key, version), sender=self.node)

        def send(name):
            try:
                self._sync_stubs[name].Replicate(request, timeout=PEER_TIMEOUT)
                return name
            except grpc.RpcError:
                return None  # unreachable (crash or partition) or too slow

        return [name for name in self._pool.map(send, self.peers) if name]

    # ---------------------------------------------------------------- writes

    def Put(self, request, context):
        self._check_key(request.key, context)
        if request.idempotency_key:
            with self._seen_lock:
                cached = self._seen.get(request.idempotency_key)
            if cached is not None:
                self.log("DUP", f"key={request.idempotency_key[:8]} already committed")
                return cached

        if request.strategy == kvstore_pb2.ALWAYS_AP:
            reply = self._put_ap(request)
        elif self.is_leaseholder:
            reply = self._put_cp(request)
        else:
            reply = self._forward_to_leaseholder(request)

        if request.idempotency_key and reply.success:
            with self._seen_lock:
                self._seen[request.idempotency_key] = reply
        return reply

    def _put_cp(self, request):
        """QUORUM_CP on the leaseholder: commit only when W nodes (this one included) hold the version.

        Without a quorum the leaseholder does NOT apply the write and the client gets an error:
        the system gives up availability to stay consistent. (Peers that did ack keep the version;
        a real CP store such as Raft/CockroachDB would use a two-phase log to avoid that.)"""
        version = self._new_version(request)
        acks = 1 + len(self._replicate_to_peers(request.key, version))
        if acks < self.write_quorum:
            self.log("NOQUORUM", f"{request.key}={request.value!r} acks={acks}/{self.n}, "
                                 f"need {self.write_quorum} -> rejected")
            return kvstore_pb2.PutReply(
                success=False, acks_received=acks,
                error_message=f"ReplicaUnavailableError: quorum lost ({acks}/{self.n} acks, "
                              f"need {self.write_quorum})")
        self.store.apply(request.key, version)
        self.log("COMMIT", f"{request.key}={request.value!r} ts={version.timestamp} acks={acks}/{self.n}")
        return kvstore_pb2.PutReply(success=True, acks_received=acks)

    def _forward_to_leaseholder(self, request):
        """QUORUM_CP on a follower: every CP write goes through the leaseholder (CockroachDB style),
        so the leaseholder always holds every committed write."""
        stub = self._kv_stubs.get(self.leaseholder)
        try:
            if stub is None:
                raise LookupError(f"no address for leaseholder {self.leaseholder}")
            # the leaseholder itself waits up to PEER_TIMEOUT for its replicas
            return stub.Put(request, timeout=3 * PEER_TIMEOUT)
        except (grpc.RpcError, LookupError):
            self.log("NOQUORUM", f"{request.key}={request.value!r}: leaseholder {self.leaseholder} "
                                 f"unreachable -> rejected")
            return kvstore_pb2.PutReply(
                success=False, acks_received=0,
                error_message=f"ReplicaUnavailableError: quorum lost (leaseholder {self.leaseholder} "
                              f"unreachable from {self.node})")

    def _put_ap(self, request):
        """ALWAYS_AP: accept locally right away, then push to whoever is reachable (sloppy quorum).
        Divergent versions are reconciled later with Last-Write-Wins on the timestamp."""
        version = self._new_version(request)
        self.store.apply(request.key, version)
        acks = 1 + len(self._replicate_to_peers(request.key, version))
        self.log("ACCEPT", f"{request.key}={request.value!r} ts={version.timestamp} acks={acks}/{self.n} (AP)")
        return kvstore_pb2.PutReply(success=True, acks_received=acks)

    # ---------------------------------------------------------------- node-to-node

    def Replicate(self, request, context):
        entry = request.entry
        self.clock.observe(entry.timestamp)
        applied = self.store.apply(entry.key, Version(entry.value, entry.timestamp, entry.origin))
        self.log("REPLICA" if applied else "OLDER",
                 f"{entry.key}={entry.value!r} ts={entry.timestamp} from {request.sender}")
        return kvstore_pb2.ReplicateReply(applied=applied)

    # ---------------------------------------------------------------- reads

    def Get(self, request, context):
        self._check_key(request.key, context)
        return self._local_reply(request.key, is_stale=not self.is_leaseholder)


def start_server(port=0, name=None, log_path=None, fault=None, delay_ms=0,
                 node=None, peers=None, leaseholder=LEASEHOLDER, write_quorum=None):
    """Start a server in the background. port=0 means 'pick any free port' (used by tests).
    One gRPC server hosts both services: Counter (Assignment 2) and KeyValueStore (Lab 6-7)."""
    servicer = CounterServicer(name or f"replica-{port}", log_path, fault, delay_ms)
    kv = KVNode(node or leaseholder, peers, leaseholder, log_path, write_quorum)
    # A3: requests are handled in parallel; shared state is guarded by locks.
    # 16 workers: a node may block one worker while it forwards a request to the leaseholder.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    kvstore_pb2_grpc.add_KeyValueStoreServicer_to_server(kv, server)
    kvstore_pb2_grpc.add_ReplicaSyncServicer_to_server(kv, server)
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
    parser.add_argument("--write-quorum", type=int, default=None,
                        help="W: acks needed to commit a QUORUM_CP write (default: majority, N/2+1)")
    args = parser.parse_args()

    server, port, _ = start_server(args.port, args.name, args.log, args.fault, args.delay_ms,
                                   node=args.node, peers=parse_peers(args.peers),
                                   leaseholder=args.leaseholder, write_quorum=args.write_quorum)
    role = "leaseholder" if (args.node or args.leaseholder) == args.leaseholder else "follower"
    print(f"server listening on port {port} (KV node {args.node or args.leaseholder}, {role})", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
