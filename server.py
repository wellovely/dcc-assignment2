import argparse
import threading
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, name="replica", log_path=None):
        # A3: this single lock protects ALL shared state (_values and _seen).
        # The dedup check and the mutation run in one critical section, so neither
        # concurrent increments nor concurrent retries with the same key can interleave.
        self._lock = threading.Lock()
        self._values = {}  # counter_id -> int
        self._seen = {}    # idempotency_key -> (counter_id, resulting value)
        self.clock = LamportClock(name, log_path)  # B1: this replica's logical clock

    def Increment(self, request, context):
        self.clock.receive(
            f"Increment(counter={request.counter_id}, delta={request.delta})",
            request.lamport_time)

        if not request.idempotency_key:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "idempotency_key is required")

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


def start_server(port=0, name=None, log_path=None):
    """Start a server in the background. port=0 means 'pick any free port' (used by tests)."""
    servicer = CounterServicer(name or f"replica-{port}", log_path)
    # A3: up to 8 requests are handled in parallel; shared state is guarded by servicer._lock
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    bound_port = server.add_insecure_port(f"[::]:{port}")
    server.start()
    return server, bound_port, servicer


def main():
    parser = argparse.ArgumentParser(description="Counter replica")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--name", default=None, help="process name in the event log, e.g. replica-A")
    parser.add_argument("--log", default=None, help="event log file (default: stderr)")
    args = parser.parse_args()

    server, port, _ = start_server(args.port, args.name, args.log)
    print(f"server listening on port {port}", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
