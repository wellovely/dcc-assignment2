import argparse
import sys
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Optional

import grpc

import counter_pb2
import counter_pb2_grpc
import kvstore_pb2
import kvstore_pb2_grpc
from clocks import LamportClock

# Only these errors mean "the call may not have reached the server / the reply was lost".
# Anything else (e.g. INVALID_ARGUMENT) is a real error and retrying would not help.
RETRYABLE = {grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE}


@dataclass
class IncrementResult:
    committed: bool                  # True when a majority of replicas acknowledged
    new_value: Optional[int]         # None when not committed: a minority value is never reported
    was_duplicate: bool
    acks: int                        # how many replicas acknowledged
    total: int                       # how many replicas the write was sent to


def describe(message):
    """Text of a message for the event log, e.g. Increment(counter=x, delta=1)."""
    if isinstance(message, counter_pb2.IncrementRequest):
        return f"Increment(counter={message.counter_id}, delta={message.delta})"
    if isinstance(message, counter_pb2.IncrementReply):
        return f"IncrementReply(new_value={message.new_value})"
    if isinstance(message, counter_pb2.GetRequest):
        return f"Get(counter={message.counter_id})"
    return f"GetReply(value={message.value}, found={message.found})"


class CounterClient:
    def __init__(self, addresses, timeout=2.0, max_retries=3, base_backoff=0.2,
                 name="client-1", log_path=None):
        if isinstance(addresses, str):
            addresses = addresses.split(",")
        self.addresses = addresses
        self._channels = [grpc.insecure_channel(a) for a in addresses]
        self._stubs = [counter_pb2_grpc.CounterStub(c) for c in self._channels]
        self.majority = len(addresses) // 2 + 1  # C1: 2 of 3 (1 of 1 for a single replica)
        self.timeout = timeout            # deadline for every single attempt, seconds
        self.max_retries = max_retries    # retries AFTER the first attempt
        self.base_backoff = base_backoff  # 0.2 -> 0.4 -> 0.8 s
        self.clock = LamportClock(name, log_path)  # B1: this client's logical clock
        self._pool = ThreadPoolExecutor(max_workers=len(addresses))  # one thread per replica

    def close(self):
        self._pool.shutdown()
        for channel in self._channels:
            channel.close()
        self.clock.close()

    def _call_with_retry(self, rpc, request):
        for attempt in range(self.max_retries + 1):
            # each attempt is a new SEND event with a fresh Lamport time;
            # the rest of the request (including the idempotency key) stays the same
            request.lamport_time = self.clock.tick("SEND", describe(request))
            try:
                reply = rpc(request, timeout=self.timeout)
            except grpc.RpcError as e:
                if e.code() not in RETRYABLE or attempt == self.max_retries:
                    raise
                time.sleep(self.base_backoff * (2 ** attempt))
                continue
            self.clock.receive(describe(reply), reply.lamport_time)
            return reply

    def _send_increment(self, stub, request):
        """Send to one replica. Returns its reply, or None if it did not acknowledge."""
        request = counter_pb2.IncrementRequest(  # own copy: each replica's thread sets its own lamport_time
            counter_id=request.counter_id, delta=request.delta, idempotency_key=request.idempotency_key)
        try:
            return self._call_with_retry(stub.Increment, request)
        except grpc.RpcError:
            return None

    def incr(self, counter_id, delta, key=None):
        # The key is created ONCE per logical operation and reused on every retry
        # and on every replica, so each replica's dedup store recognises a retry.
        if key is None:
            key = str(uuid.uuid4())
        request = counter_pb2.IncrementRequest(counter_id=counter_id, delta=delta, idempotency_key=key)

        # C1: send to ALL replicas in parallel and wait for every answer
        replies = list(self._pool.map(lambda stub: self._send_increment(stub, request), self._stubs))
        acks = [r for r in replies if r is not None]

        # committed only if a majority acknowledged
        if len(acks) < self.majority:
            return IncrementResult(False, None, False, len(acks), len(replies))
        value = Counter(r.new_value for r in acks).most_common(1)[0][0]  # value most replicas agree on
        duplicate = any(r.was_duplicate for r in acks)
        return IncrementResult(True, value, duplicate, len(acks), len(replies))

    def get(self, counter_id):
        # C1: a read goes to a single replica (the first one that answers)
        last_error = None
        for stub in self._stubs:
            try:
                return self._call_with_retry(stub.Get, counter_pb2.GetRequest(counter_id=counter_id))
            except grpc.RpcError as e:
                last_error = e
        raise last_error


# ======================================================================
# Lab 6-7: key-value client
# ======================================================================

KV_NODES = "localhost:50051,localhost:50052,localhost:50053"  # A (leaseholder), B, C
STRATEGIES = {"cp": kvstore_pb2.QUORUM_CP, "ap": kvstore_pb2.ALWAYS_AP}
CONSISTENCIES = {"linearizable": kvstore_pb2.LINEARIZABLE, "stale": kvstore_pb2.FOLLOWER_STALE}


class KVClient:
    """Talks to the KeyValueStore service. Every call picks the node it is sent to.

    nodes: {'A': 'localhost:50051', ...} or a comma-separated address list, named A, B, C in order."""

    def __init__(self, nodes=KV_NODES, timeout=2.0):
        if isinstance(nodes, str):
            nodes = nodes.split(",")
        if not isinstance(nodes, dict):
            nodes = {"ABCDEFG"[i]: addr for i, addr in enumerate(nodes)}
        self.nodes = nodes
        self.timeout = timeout
        self._channels = {name: grpc.insecure_channel(addr) for name, addr in nodes.items()}
        self._stubs = {name: kvstore_pb2_grpc.KeyValueStoreStub(c) for name, c in self._channels.items()}

    def close(self):
        for channel in self._channels.values():
            channel.close()

    def put(self, key, value, strategy=kvstore_pb2.QUORUM_CP, node="A", timestamp=0, idempotency_key=None):
        request = kvstore_pb2.PutRequest(key=key, value=value, strategy=strategy, timestamp=timestamp,
                                         idempotency_key=idempotency_key or str(uuid.uuid4()))
        return self._stubs[node].Put(request, timeout=self.timeout)

    def get(self, key, consistency=kvstore_pb2.LINEARIZABLE, max_staleness_ms=0, node="A"):
        """Raises grpc.RpcError: UNAVAILABLE (no leaseholder) or FAILED_PRECONDITION (STALE_READ_EXCEEDED)."""
        request = kvstore_pb2.GetRequest(key=key, consistency=consistency, max_staleness_ms=max_staleness_ms)
        return self._stubs[node].Get(request, timeout=self.timeout)

    def get_quorum(self, key, r, nodes=None):
        """Quorum read: read the local copy of R nodes and keep the version with the highest timestamp.
        With R + W > N at least one of them holds the latest committed write.
        Returns (best reply, {node: reply}). Raises RuntimeError if fewer than R nodes answer."""
        replies = {}
        for name in nodes or self.nodes:
            if len(replies) == r:
                break
            try:
                replies[name] = self.get(key, kvstore_pb2.FOLLOWER_STALE, 0, node=name)
            except grpc.RpcError:
                continue
        if len(replies) < r:
            raise RuntimeError(f"read quorum not reached: {len(replies)}/{r} nodes answered")
        best = max(replies.values(), key=lambda reply: reply.read_timestamp)
        return best, replies


def run_kv(args):
    """The put / kv-get commands. Returns the exit code."""
    client = KVClient(args.addr or KV_NODES)
    try:
        if args.cmd == "put":
            reply = client.put(args.key, args.value, STRATEGIES[args.strategy], args.node, args.timestamp)
            if reply.success:
                print(f"OK {args.strategy.upper()} write via {args.node} (acks: {reply.acks_received})")
                return 0
            print(f"FAILED via {args.node}: {reply.error_message} (acks: {reply.acks_received})")
            return 1
        if args.r:
            reply, replies = client.get_quorum(args.key, args.r)
            print(f"quorum read R={args.r} from {','.join(replies)}: "
                  + (f"value={reply.value} ts={reply.read_timestamp}" if reply.found else "not found"))
            return 0
        reply = client.get(args.key, CONSISTENCIES[args.consistency], args.max_staleness_ms, args.node)
        if reply.found:
            print(f"value={reply.value} ts={reply.read_timestamp} stale={'yes' if reply.is_stale else 'no'}")
        else:
            print("not found")
        return 0
    except grpc.RpcError as e:
        print(f"FAILED: {e.code().name} {e.details()}")
        return 1
    except RuntimeError as e:
        print(f"FAILED: {e}")
        return 1
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(description="Counter and key-value client")
    parser.add_argument("--addr", default=None,
                        help="replica address(es), comma-separated, e.g. "
                             "localhost:50051,localhost:50052,localhost:50053 "
                             "(default: localhost:50051 for incr/get, all three for put/kv-get)")
    parser.add_argument("--name", default="client-1", help="process name in the event log")
    parser.add_argument("--log", default=None, help="event log file (default: stderr)")
    parser.add_argument("--repeat", type=int, default=1,
                        help="run the command N times in this process (one Lamport clock, new key each time)")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_incr = sub.add_parser("incr", help="increment a counter")
    p_incr.add_argument("counter_id")
    p_incr.add_argument("--by", type=int, default=1, help="delta")
    p_incr.add_argument("--key", default=None, help="idempotency key (default: new uuid4)")

    p_get = sub.add_parser("get", help="read a counter")
    p_get.add_argument("counter_id")

    # Lab 6-7: key-value store. Nodes in --addr are named A, B, C in order; A is the leaseholder.
    p_put = sub.add_parser("put", help="write a key (KV store)")
    p_put.add_argument("key")
    p_put.add_argument("value")
    p_put.add_argument("--strategy", choices=STRATEGIES, default="cp", help="cp = QUORUM_CP, ap = ALWAYS_AP")
    p_put.add_argument("--node", default="A", help="node to send the write to")
    p_put.add_argument("--timestamp", type=int, default=0, help="write timestamp (default: assigned by the node)")

    p_kv_get = sub.add_parser("kv-get", help="read a key (KV store)")
    p_kv_get.add_argument("key")
    p_kv_get.add_argument("--consistency", choices=CONSISTENCIES, default="linearizable")
    p_kv_get.add_argument("--max-staleness-ms", type=int, default=0, help="bound for stale reads (0 = none)")
    p_kv_get.add_argument("--node", default="A", help="node to send the read to")
    p_kv_get.add_argument("--r", type=int, default=0, help="quorum read from R nodes instead")

    args = parser.parse_args()
    if args.cmd in ("put", "kv-get"):
        sys.exit(run_kv(args))

    client = CounterClient(args.addr or "localhost:50051", name=args.name, log_path=args.log)
    failed = False
    try:
        for _ in range(args.repeat):
            if args.cmd == "incr":
                result = client.incr(args.counter_id, args.by, key=args.key)
                acked = f"replicas acked: {result.acks}/{result.total}"
                if result.committed:
                    dup = "yes" if result.was_duplicate else "no"
                    print(f"OK committed value={result.new_value} ({acked}, duplicate: {dup})")
                else:
                    print(f"FAILED not committed ({acked}, majority is {client.majority})")
                    failed = True
            else:
                reply = client.get(args.counter_id)
                print(f"value={reply.value}" if reply.found else "not found")
    except grpc.RpcError as e:
        print(f"FAILED: {e.code().name} {e.details()}")
        failed = True
    finally:
        client.close()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
