"""Task C4: Increment latency, single replica vs three-replica quorum, at 1 and 16 concurrent clients.

Run:  python tests/perf_benchmark.py            (3 runs per configuration, 2000 requests each)
      python tests/perf_benchmark.py --runs 1   (quick)

Replicas are separate `server.py` processes; clients are threads in this process, each with its own
CounterClient. Latency = wall time of one client.incr() call (time.perf_counter), in milliseconds.
"""
import argparse
import math
import os
import platform
import socket
import statistics
import subprocess
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from client import CounterClient  # noqa: E402

CONFIGS = [  # (label, replicas, clients)
    ("Single replica, 1 client", 1, 1),
    ("Single replica, 16 clients", 1, 16),
    ("Quorum (3 replicas), 1 client", 3, 1),
    ("Quorum (3 replicas), 16 clients", 3, 16),
]
WARMUP = 50  # requests per client before measuring (opens connections), not counted


def free_port():
    with socket.socket() as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


def is_listening(port):
    with socket.socket() as s:
        return s.connect_ex(("localhost", port)) == 0


def start_replicas(n):
    procs, addresses = [], []
    for _ in range(n):
        port = free_port()
        procs.append(subprocess.Popen(
            [sys.executable, "server.py", "--port", str(port), "--log", os.devnull],
            cwd=ROOT, stdout=subprocess.DEVNULL))
        addresses.append(f"localhost:{port}")
    for address in addresses:  # wait until every replica accepts connections
        port = int(address.split(":")[1])
        while not is_listening(port):
            time.sleep(0.05)
    return procs, addresses


def p95(latencies):
    ordered = sorted(latencies)
    return ordered[min(math.ceil(0.95 * len(ordered)), len(ordered) - 1)]  # index ceil(0.95 * n)


def run_once(n_replicas, n_clients, total_requests):
    procs, addresses = start_replicas(n_replicas)
    per_client = total_requests // n_clients
    latencies = []                 # ms, from all clients
    lock = threading.Lock()
    warmed_up = threading.Barrier(n_clients + 1)  # the clock starts only after every client warmed up

    def worker(i):
        client = CounterClient(addresses, name=f"bench-{i}", log_path=os.devnull)
        for _ in range(WARMUP):
            client.incr(f"warmup-{i}", 1)
        warmed_up.wait()
        mine = []
        for _ in range(per_client):
            start = time.perf_counter()
            result = client.incr("likes", 1)
            mine.append((time.perf_counter() - start) * 1000)
            assert result.committed
        client.close()
        with lock:
            latencies.extend(mine)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_clients)]
    for t in threads:
        t.start()
    warmed_up.wait()
    started = time.perf_counter()
    for t in threads:
        t.join()
    wall = time.perf_counter() - started

    for proc in procs:
        proc.kill()
        proc.wait()
    return {"median": statistics.median(latencies), "p95": p95(latencies),
            "requests": len(latencies), "throughput": len(latencies) / wall}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=2000, help="requests per configuration")
    parser.add_argument("--runs", type=int, default=3, help="repeated runs per configuration")
    args = parser.parse_args()

    print(f"# {platform.platform()}, Python {platform.python_version()}, {os.cpu_count()} CPUs, "
          f"{args.runs} runs x {args.requests} requests\n")

    rows = []
    for label, n_replicas, n_clients in CONFIGS:
        runs = [run_once(n_replicas, n_clients, args.requests) for _ in range(args.runs)]
        rows.append((label, runs))
        print(f"done: {label}", file=sys.stderr)

    # the mandated table (values: median over the repeated runs)
    print("| Configuration | Median latency (ms) | p95 latency (ms) | Requests |")
    print("|---|---|---|---|")
    for label, runs in rows:
        med = statistics.median(r["median"] for r in runs)
        p = statistics.median(r["p95"] for r in runs)
        print(f"| {label} | {med:.2f} | {p:.2f} | {runs[0]['requests']} |")

    # extra: variance across runs and throughput
    print("\n| Configuration | Median per run (ms) | p95 per run (ms) | Throughput (req/s), mean ± stdev |")
    print("|---|---|---|---|")
    for label, runs in rows:
        meds = ", ".join(f"{r['median']:.2f}" for r in runs)
        p95s = ", ".join(f"{r['p95']:.2f}" for r in runs)
        tps = [r["throughput"] for r in runs]
        spread = statistics.stdev(tps) if len(tps) > 1 else 0.0
        print(f"| {label} | {meds} | {p95s} | {statistics.mean(tps):.0f} ± {spread:.0f} |")


if __name__ == "__main__":
    main()
