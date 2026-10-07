import argparse
import threading
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self):
        # A3: this single lock protects ALL shared state (_values and _seen).
        # The dedup check and the mutation run in one critical section, so neither
        # concurrent increments nor concurrent retries with the same key can interleave.
        self._lock = threading.Lock()
        self._values = {}  # counter_id -> int
        self._seen = {}    # idempotency_key -> (counter_id, resulting value)

    def Increment(self, request, context):
        if not request.idempotency_key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key is required")

        with self._lock:
            # duplicate check and mutation in ONE critical section
            if request.idempotency_key in self._seen:
                _, stored_value = self._seen[request.idempotency_key]
                return counter_pb2.IncrementReply(new_value=stored_value, was_duplicate=True)

            new_value = self._values.get(request.counter_id, 0) + request.delta
            self._values[request.counter_id] = new_value
            self._seen[request.idempotency_key] = (request.counter_id, new_value)
            return counter_pb2.IncrementReply(new_value=new_value, was_duplicate=False)

    def Get(self, request, context):
        with self._lock:
            if request.counter_id in self._values:
                return counter_pb2.GetReply(value=self._values[request.counter_id], found=True)
            return counter_pb2.GetReply(value=0, found=False)


def start_server(port=0):
    """Start a server in the background. port=0 means 'pick any free port' (used by tests)."""
    servicer = CounterServicer()
    # A3: up to 8 requests are handled in parallel; shared state is guarded by servicer._lock
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    bound_port = server.add_insecure_port(f"[::]:{port}")
    server.start()
    return server, bound_port, servicer


def main():
    parser = argparse.ArgumentParser(description="Counter replica")
    parser.add_argument("--port", type=int, default=50051)
    args = parser.parse_args()

    server, port, _ = start_server(args.port)
    print(f"server listening on port {port}", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    main()