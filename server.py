import argparse
import os
import threading
import time
from concurrent import futures

import grpc

import counter_pb2
import counter_pb2_grpc
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


def start_server(port=0, name=None, log_path=None, fault=None, delay_ms=0):
    """Start a server in the background. port=0 means 'pick any free port' (used by tests)."""
    servicer = CounterServicer(name or f"replica-{port}", log_path, fault, delay_ms)
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
    parser.add_argument("--fault", choices=FAULTS, default=None,
                        help="crash on the first Increment, before or after applying it")
    parser.add_argument("--delay-ms", type=int, default=0,
                        help="delay the reply to the first Increment by this many ms")
    args = parser.parse_args()

    server, port, _ = start_server(args.port, args.name, args.log, args.fault, args.delay_ms)
    print(f"server listening on port {port}", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
