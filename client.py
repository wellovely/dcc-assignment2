import argparse
import sys
import time
import uuid

import grpc

import counter_pb2
import counter_pb2_grpc

# Only these errors mean "the call may not have reached the server / the reply was lost".
# Anything else (e.g. INVALID_ARGUMENT) is a real error and retrying would not help.
RETRYABLE = {grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE}


class CounterClient:
    def __init__(self, address, timeout=2.0, max_retries=3, base_backoff=0.2):
        self._channel = grpc.insecure_channel(address)
        self._stub = counter_pb2_grpc.CounterStub(self._channel)
        self.timeout = timeout            # deadline for every single attempt, seconds
        self.max_retries = max_retries    # retries AFTER the first attempt
        self.base_backoff = base_backoff  # 0.2 -> 0.4 -> 0.8 s
        self.attempts = 0                 # attempts used by the last call (useful in tests)

    def close(self):
        self._channel.close()

    def _call_with_retry(self, rpc, request):
        for attempt in range(self.max_retries + 1):
            self.attempts = attempt + 1
            try:
                return rpc(request, timeout=self.timeout)
            except grpc.RpcError as e:
                if e.code() not in RETRYABLE or attempt == self.max_retries:
                    raise
                time.sleep(self.base_backoff * (2 ** attempt))

    def incr(self, counter_id, delta, key=None):
        # The key is created ONCE per logical operation and reused on every retry,
        # so the server's dedup store can recognise a retry of the same operation.
        if key is None:
            key = str(uuid.uuid4())
        request = counter_pb2.IncrementRequest(
            counter_id=counter_id, delta=delta, idempotency_key=key)
        return self._call_with_retry(self._stub.Increment, request)

    def get(self, counter_id):
        request = counter_pb2.GetRequest(counter_id=counter_id)
        return self._call_with_retry(self._stub.Get, request)


def main():
    parser = argparse.ArgumentParser(description="Counter client")
    parser.add_argument("--addr", default="localhost:50051", help="replica address host:port")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_incr = sub.add_parser("incr", help="increment a counter")
    p_incr.add_argument("counter_id")
    p_incr.add_argument("--by", type=int, default=1, help="delta")
    p_incr.add_argument("--key", default=None, help="idempotency key (default: new uuid4)")

    p_get = sub.add_parser("get", help="read a counter")
    p_get.add_argument("counter_id")

    args = parser.parse_args()
    client = CounterClient(args.addr)
    try:
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
