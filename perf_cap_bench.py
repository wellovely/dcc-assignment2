"""Lab 6-7, Task 4: PACELC benchmark - LINEARIZABLE vs FOLLOWER_STALE reads on a 3-node cluster.

Run:  python perf_cap_bench.py                 (1000 reads per scenario)
      python perf_cap_bench.py --iterations 200 (quick)

The client sits next to follower B and sends every read to B:
  * LINEARIZABLE   -> B proxies the read to the leaseholder A (one extra network hop)
  * FOLLOWER_STALE -> B answers from its local copy (max_staleness_ms = 5000)

Three phases measure both sides of PACELC:
  1. healthy cluster                 - Else: latency vs consistency
  2. leaseholder A killed (< 5 s)    - Partition/crash: availability vs consistency
  3. leaseholder A down for > 5 s    - the staleness bound is exceeded, follower reads fail too

Latency = wall time of one successful Get (time.perf_counter), in ms.
Availability = successful reads / attempted reads.
"""
import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import time

import grpc

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT, "tests"))
from perf_benchmark import free_port, is_listening, p95  # noqa: E402  (Assignment 2 helpers)

from client import KVClient  # noqa: E402
from kvstore_pb2 import FOLLOWER_STALE, LINEARIZABLE, QUORUM_CP  # noqa: E402

NODES = "ABC"
MAX_STALENESS_MS = 5000
WARMUP = 50  # reads per scenario before measuring (opens connections), not counted
SCENARIOS = [  # (label, target node description, consistency)
    ("Linearizable", "Leaseholder (Remote)", LINEARIZABLE),
    ("Follower Stale", "Local Follower", FOLLOWER_STALE),
]


def start_cluster(ports):
    procs = {}
    for n in NODES:
        peers = ",".join(f"{m}=127.0.0.1:{ports[m]}" for m in NODES if m != n)
        procs[n] = subprocess.Popen(
            [sys.executable, "server.py", "--port", str(ports[n]), "--node", n,
             "--peers", peers, "--log", os.devnull],
            cwd=ROOT, stdout=subprocess.DEVNULL)
    for n in NODES:
        while not is_listening(ports[n]):
            time.sleep(0.05)
    return procs


def measure(client, consistency, iterations, warmup=WARMUP):
    for _ in range(warmup):
        try:
            client.get("bench", consistency, MAX_STALENESS_MS, node="B")
        except grpc.RpcError:
            pass
    latencies, errors = [], {}
    for _ in range(iterations):
        start = time.perf_counter()
        try:
            client.get("bench", consistency, MAX_STALENESS_MS, node="B")
            latencies.append((time.perf_counter() - start) * 1000)
        except grpc.RpcError as e:
            errors[e.code().name] = errors.get(e.code().name, 0) + 1
    return {
        "median_ms": round(statistics.median(latencies), 3) if latencies else None,
        "p95_ms": round(p95(latencies), 3) if latencies else None,
        "availability_pct": round(100 * len(latencies) / iterations, 1),
        "ok": len(latencies), "attempts": iterations, "errors": errors,
    }


def print_table(title, rows):
    print(f"\n{title}")
    print(f"{'Read Mode':<16}{'Target Node':<23}{'Median (ms)':>12}{'p95 (ms)':>10}{'Availability':>14}  errors")
    for label, target, r in rows:
        med = f"{r['median_ms']:.3f}" if r["median_ms"] is not None else "-"
        p = f"{r['p95_ms']:.3f}" if r["p95_ms"] is not None else "-"
        print(f"{label:<16}{target:<23}{med:>12}{p:>10}{r['availability_pct']:>13.1f}%  {r['errors'] or ''}")


def main():
    parser = argparse.ArgumentParser(description="PACELC benchmark: linearizable vs follower reads")
    parser.add_argument("--iterations", type=int, default=1000, help="reads per scenario")
    parser.add_argument("--ports", default=None,
                        help="ports of A,B,C, e.g. 50051,50052,50053 (default: free ports)")
    parser.add_argument("--out", default=os.path.join(ROOT, "bench_results", "cap_bench.json"))
    args = parser.parse_args()

    ports = dict(zip(NODES, map(int, args.ports.split(",")))) if args.ports else \
        {n: free_port() for n in NODES}
    procs = start_cluster(ports)
    client = KVClient({n: f"127.0.0.1:{ports[n]}" for n in NODES}, timeout=2.0)
    results = {"iterations": args.iterations, "max_staleness_ms": MAX_STALENESS_MS,
               "python": platform.python_version(), "platform": platform.platform(), "phases": {}}
    try:
        assert client.put("bench", "value-1", QUORUM_CP, node="A").success
        # wait until B has heard from the leaseholder, so follower reads are within the bound
        while True:
            try:
                client.get("bench", FOLLOWER_STALE, MAX_STALENESS_MS, node="B")
                break
            except grpc.RpcError:
                time.sleep(0.05)

        phases = [
            ("healthy", "Phase 1 - healthy cluster (3/3 nodes up)", None),
            ("leaseholder_down", "Phase 2 - leaseholder A killed (B lags < 5 s)", "kill"),
            ("bound_exceeded", f"Phase 3 - A down for > {MAX_STALENESS_MS / 1000:.0f} s "
                               f"(staleness bound exceeded)", "wait"),
        ]
        for key, title, action in phases:
            if action == "kill":
                procs["A"].kill()
                procs["A"].wait()
            elif action == "wait":
                time.sleep(MAX_STALENESS_MS / 1000 + 0.5)
            rows = []
            for label, target, consistency in SCENARIOS:
                # after a kill the leaseholder is gone: no warmup, every attempt counts
                r = measure(client, consistency, args.iterations, warmup=WARMUP if key == "healthy" else 0)
                rows.append((label, target, r))
            results["phases"][key] = {label: r for label, _, r in rows}
            print_table(title, rows)
    finally:
        client.close()
        for proc in procs.values():
            proc.kill()
            proc.wait()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nresults written to {os.path.relpath(args.out, ROOT)}")


if __name__ == "__main__":
    main()
