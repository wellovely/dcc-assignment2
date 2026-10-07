import argparse
import sys
import time
import uuid
from collections import Counter
from concurrent import futures
from dataclasses import dataclass, field
from typing import Optional

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock

# Only these errors mean "the call may not have reached the server / the reply was lost".
# Anything else (e.g. INVALID_ARGUMENT) is a real error and retrying would not help.
RETRYABLE = {grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE}


@dataclass
class IncrementResult:
    committed: bool = False
    new_value: Optional[int] = None  # None when not committed: a minority value is never presented as committed
    was_duplicate: bool = False
    total: int = 0                   # number of replicas the write was sent to
    outcomes: dict = field(default_factory=dict)  # address -> "OK" or gRPC status name, filled as replies arrive

    @property
    def acks(self):
        return sum(1 for o in list(self.outcomes.values()) if o == "OK")


class CounterClient:
    def __init__(self, addresses, timeout=2.0, max_retries=3, base_backoff=0.2,
                 name="client-1", log_path=None):
        if isinstance(addresses, str):
            addresses = addresses.split(",")
        self.addresses = [a.strip() for a in addresses]
        self._channels = [grpc.insecure_channel(a) for a in self.addresses]
        self._stubs = [counter_pb2_grpc.CounterStub(c) for c in self._channels]
        self.majority = len(self.addresses) // 2 + 1  # C1: 2 of 3 (1 of 1 for a single replica)
        self.timeout = timeout            # deadline for every single attempt, seconds
        self.max_retries = max_retries    # retries AFTER the first attempt
        self.base_backoff = base_backoff  # 0.2 -> 0.4 -> 0.8 s
        self.clock = LamportClock(name, log_path)  # B1: this client's logical clock
        self._pool = futures.ThreadPoolExecutor(max_workers=32)  # parallel sends to replicas
        self._inflight = []               # sends still running after the write already committed

    def flush(self):
        """Wait until replies (or final errors) from ALL replicas have arrived."""
        futures.wait(self._inflight)
        self._inflight = []

    def close(self):
        self.flush()
        self._pool.shutdown(wait=True)
        for channel in self._channels:
            channel.close()
        self.clock.close()

    def _call_with_retry(self, rpc, build_request, send_desc, reply_desc):
        for attempt in range(self.max_retries + 1):
            # every attempt is a new SEND event with a fresh Lamport time,
            # but build_request always puts the SAME idempotency key inside
            lamport_time = self.clock.tick("SEND", send_desc)
            try:
                reply = rpc(build_request(lamport_time), timeout=self.timeout)
            except grpc.RpcError as e:
                if e.code() not in RETRYABLE or attempt == self.max_retries:
                    raise
                time.sleep(self.base_backoff * (2 ** attempt))
                continue
            self.clock.receive(reply_desc(reply), reply.lamport_time)
            return reply

    def _send_to_replica(self, result, address, stub, build_request, send_desc, reply_desc):
        try:
            reply = self._call_with_retry(stub.Increment, build_request, send_desc, reply_desc)
        except grpc.RpcError as e:
            result.outcomes[address] = e.code().name
            raise
        result.outcomes[address] = "OK"
        return reply

    def incr(self, counter_id, delta, key=None):
        # The key is created ONCE per logical operation and reused on every retry
        # and on every replica, so each replica's dedup store recognises a retry.
        if key is None:
            key = str(uuid.uuid4())

        def build_request(lamport_time):
            return counter_pb2.IncrementRequest(
                counter_id=counter_id, delta=delta, idempotency_key=key,
                lamport_time=lamport_time)

        send_desc = f"Increment(counter={counter_id}, delta={delta})"
        reply_desc = lambda r: f"IncrementReply(new_value={r.new_value})"

        # C1: send the write to ALL replicas in parallel ...
        result = IncrementResult(total=len(self.addresses))
        sends = [self._pool.submit(self._send_to_replica, result, address, stub,
                                   build_request, send_desc, reply_desc)
                 for address, stub in zip(self.addresses, self._stubs)]

        # ... and stop waiting as soon as a majority has acknowledged it.
        replies = []
        for done in futures.as_completed(sends):
            if done.exception() is None:
                replies.append(done.result())
            if len(replies) >= self.majority:
                break

        # slower replicas keep going in the background; flush()/close() waits for them
        self._inflight = [f for f in self._inflight + sends if not f.done()]

        if len(replies) >= self.majority:
            result.committed = True
            result.new_value = Counter(r.new_value for r in replies).most_common(1)[0][0]
            result.was_duplicate = any(r.was_duplicate for r in replies)
        return result

    def get(self, counter_id):
        # C1: a read goes to a single replica (the first one that answers)
        def build_request(lamport_time):
            return counter_pb2.GetRequest(counter_id=counter_id, lamport_time=lamport_time)

        last_error = None
        for stub in self._stubs:
            try:
                return self._call_with_retry(
                    stub.Get, build_request,
                    f"Get(counter={counter_id})",
                    lambda r: f"GetReply(value={r.value}, found={r.found})")
            except grpc.RpcError as e:
                last_error = e
        raise last_error


def main():
    parser = argparse.ArgumentParser(description="Counter client")
    parser.add_argument("--addr", default="localhost:50051",
                        help="replica address(es), comma-separated, e.g. "
                             "localhost:50051,localhost:50052,localhost:50053")
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

    args = parser.parse_args()
    client = CounterClient(args.addr, name=args.name, log_path=args.log)
    failed = False
    try:
        for _ in range(args.repeat):
            if args.cmd == "incr":
                result = client.incr(args.counter_id, args.by, key=args.key)
                client.flush()  # CLI only: wait for slow replicas so the ack count is final
                acked = f"replicas acked: {result.acks}/{result.total}"
                if result.committed:
                    dup = "yes" if result.was_duplicate else "no"
                    print(f"OK committed value={result.new_value} ({acked}, duplicate: {dup})")
                else:
                    errors = {a: o for a, o in result.outcomes.items() if o != "OK"}
                    print(f"FAILED not committed ({acked}, majority is {client.majority}; errors: {errors})")
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
