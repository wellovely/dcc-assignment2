import argparse
import sys
import time
import uuid

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock

# Only these errors mean "the call may not have reached the server / the reply was lost".
# Anything else (e.g. INVALID_ARGUMENT) is a real error and retrying would not help.
RETRYABLE = {grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE}


class CounterClient:
    def __init__(self, address, timeout=2.0, max_retries=3, base_backoff=0.2,
                 name="client-1", log_path=None):
        self._channel = grpc.insecure_channel(address)
        self._stub = counter_pb2_grpc.CounterStub(self._channel)
        self.timeout = timeout            # deadline for every single attempt, seconds
        self.max_retries = max_retries    # retries AFTER the first attempt
        self.base_backoff = base_backoff  # 0.2 -> 0.4 -> 0.8 s
        self.attempts = 0                 # attempts used by the last call (useful in tests)
        self.clock = LamportClock(name, log_path)  # B1: this client's logical clock

    def close(self):
        self._channel.close()
        self.clock.close()

    def _call_with_retry(self, rpc, build_request, send_desc, reply_desc):
        for attempt in range(self.max_retries + 1):
            self.attempts = attempt + 1
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

    def incr(self, counter_id, delta, key=None):
        # The key is created ONCE per logical operation and reused on every retry,
        # so the server's dedup store can recognise a retry of the same operation.
        if key is None:
            key = str(uuid.uuid4())

        def build_request(lamport_time):
            return counter_pb2.IncrementRequest(
                counter_id=counter_id, delta=delta, idempotency_key=key,
                lamport_time=lamport_time)

        return self._call_with_retry(
            self._stub.Increment, build_request,
            f"Increment(counter={counter_id}, delta={delta})",
            lambda r: f"IncrementReply(new_value={r.new_value})")

    def get(self, counter_id):
        def build_request(lamport_time):
            return counter_pb2.GetRequest(counter_id=counter_id, lamport_time=lamport_time)

        return self._call_with_retry(
            self._stub.Get, build_request,
            f"Get(counter={counter_id})",
            lambda r: f"GetReply(value={r.value}, found={r.found})")


def main():
    parser = argparse.ArgumentParser(description="Counter client")
    parser.add_argument("--addr", default="localhost:50051", help="replica address host:port")
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
    try:
        for _ in range(args.repeat):
            if args.cmd == "incr":
                reply = client.incr(args.counter_id, args.by, key=args.key)
                dup = "yes" if reply.was_duplicate else "no"
                print(f"OK committed value={reply.new_value} (duplicate: {dup})")
            else:
                reply = client.get(args.counter_id)
                print(f"value={reply.value}" if reply.found else "not found")
    except grpc.RpcError as e:
        print(f"FAILED after {client.attempts} attempt(s): {e.code().name} {e.details()}")
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
