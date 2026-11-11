# Replicated Counter Service + Key-Value Store

A gRPC counter service in Python: safe retries with idempotency keys, Lamport clocks,
and three replicas with majority (2 of 3) writes. The test report is `report.pdf`.

Lab 6-7 adds a replicated key-value store to the same servers: linearizable and follower reads,
CP and AP writes, network-partition tests and a PACELC benchmark (section 7, report
`Lab_Report_CAP.pdf`). The Assignment 2 version is tagged `assignment2`.

All commands are run from this folder.

## 1. Setup

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install grpcio grpcio-tools pytest
```

The generated files `counter_pb2*.py` and `kvstore_pb2*.py` are already in the repo.
To regenerate them after changing a `.proto` file:

```bash
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto kvstore.proto
```

## 2. Start one replica

```bash
# terminal 1
python3 server.py --port 50051

# terminal 2
python3 client.py incr likes:post-42 --by 5
python3 client.py get likes:post-42                       # value=5
python3 client.py incr likes:post-42 --by 1 --key abc
python3 client.py incr likes:post-42 --by 1 --key abc     # duplicate: yes, value does not change
```

## 3. Start three replicas

```bash
# terminals 1, 2, 3
python3 server.py --port 50051 --name replica-A
python3 server.py --port 50052 --name replica-B
python3 server.py --port 50053 --name replica-C

# terminal 4
python3 client.py --addr localhost:50051,localhost:50052,localhost:50053 incr likes:post-42 --by 1
python3 client.py --addr localhost:50051,localhost:50052,localhost:50053 get likes:post-42
```

Stop one replica (Ctrl+C) and the write still commits (`replicas acked: 2/3`).
Stop two and it fails (`FAILED not committed`).

## 4. Run the tests

```bash
python3 -m pytest tests/ -v
```

35 tests, about 9 seconds. `tests/test_counter.py` holds the Task C2 tests,
`tests/test_failures.py` holds the Task C3 failure tests and `tests/test_cap.py` holds
the 18 Lab 6-7 tests (section 7).

## 5. Run the benchmark

```bash
python3 tests/perf_benchmark.py              # 4 configurations x 3 runs x 2000 requests
python3 tests/perf_benchmark.py --runs 1     # quicker
```

## 6. Lamport trace for Task B2 (optional)

```bash
rm -rf logs && mkdir logs
python3 server.py --port 50051 --name replica-A --log logs/replica-A.log & SERVER=$!
sleep 1
python3 client.py --name client-1 --log logs/client-1.log --repeat 2 incr x & C1=$!
python3 client.py --name client-2 --log logs/client-2.log --repeat 2 incr y & C2=$!
python3 client.py --name client-3 --log logs/client-3.log --repeat 2 get x  & C3=$!
wait $C1 $C2 $C3; kill $SERVER

# merge the logs, sorted by Lamport value
python3 -c "
import glob, re
lines = [l for f in sorted(glob.glob('logs/*.log')) for l in open(f)]
lines.sort(key=lambda l: int(re.search(r' L=(\d+)', l).group(1)))
open('logs/b2_merged.log', 'w').writelines(lines)"
cat logs/b2_merged.log
```

The order is different on every run.

## 7. Lab 6-7: key-value store, consistency and CAP

Node A (port 50051) is the leaseholder for keys `[a-z]`; B and C are followers.

```bash
# terminals 1, 2, 3
python3 server.py --port 50051 --node A --peers B=localhost:50052,C=localhost:50053
python3 server.py --port 50052 --node B --peers A=localhost:50051,C=localhost:50053
python3 server.py --port 50053 --node C --peers A=localhost:50051,B=localhost:50052

# terminal 4 (put / kv-get talk to localhost:50051-50053 by default)
python3 client.py put x hello                        # QUORUM_CP, goes through the leaseholder
python3 client.py put x world --node C --strategy ap # ALWAYS_AP, accepted by C right away
python3 client.py kv-get x                           # LINEARIZABLE, answered by A
python3 client.py kv-get x --node B --consistency stale --max-staleness-ms 5000
python3 client.py kv-get x --r 2                     # quorum read from 2 nodes
```

Stop A (Ctrl+C): CP writes fail with `ReplicaUnavailableError: quorum lost`, linearizable reads
fail with `UNAVAILABLE`, AP writes and follower reads keep working. After 5 s without A,
`--max-staleness-ms 5000` reads fail with `STALE_READ_EXCEEDED`.

If port 50051 is taken (e.g. by Multipass), use other ports and pass them with
`--addr localhost:P1,localhost:P2,localhost:P3`.

Tests (network partitions are simulated by TCP proxies between the nodes):

```bash
python3 -m pytest tests/test_cap.py -v     # 18 tests, about 3 seconds
```

Benchmark (1000 reads per scenario, results in `bench_results/cap_bench.json`):

```bash
python3 perf_cap_bench.py
```

| What | How it works |
|---|---|
| `QUORUM_CP` write | forwarded to the leaseholder; committed only with W acks (default 2 of 3) |
| `ALWAYS_AP` write | applied on the receiving node, pushed to reachable peers |
| `LINEARIZABLE` read | answered by the leaseholder; a follower proxies it there |
| `FOLLOWER_STALE` read | answered from the local copy if it lags less than `max_staleness_ms` |
| conflicts | Last-Write-Wins on (timestamp, node), merged by anti-entropy every 200 ms |

## Server flags

| Flag | Meaning |
|---|---|
| `--port` | port to listen on |
| `--name` | name in the event log, e.g. `replica-A` |
| `--log` | event log file (default: stderr) |
| `--delay-ms N` | apply the first Increment, but reply N ms later |
| `--fault crash-before-apply` | exit when the first Increment arrives |
| `--fault crash-after-apply` | apply the first Increment, then exit without replying |
| `--node A\|B\|C` | KV node name (default `A`) |
| `--peers` | the other KV nodes, e.g. `B=localhost:50052,C=localhost:50053` |
| `--leaseholder` | leaseholder node for `[a-z]` (default `A`) |
| `--write-quorum W` | acks needed for a `QUORUM_CP` write (default: majority) |
| `--sync-interval-ms N` | anti-entropy period (default 200, 0 = off) |

## Files

| File | What it is |
|---|---|
| `counter.proto` | gRPC interface of the counter (Assignment 2) |
| `kvstore.proto` | gRPC interface of the key-value store (Lab 6-7) |
| `server.py` | one replica: Counter and KeyValueStore services |
| `client.py` | client: `incr` / `get` (counter), `put` / `kv-get` (key-value store) |
| `clocks.py` | Lamport clock |
| `tests/` | tests and the Assignment 2 benchmark |
| `perf_cap_bench.py` | Lab 6-7 PACELC benchmark |
| `bench_results/` | benchmark output used in the Lab 6-7 report |
| `logs/` | captured event logs used in the report |
| `report.pdf` | Assignment 2 test report |
| `Lab_Report_CAP.pdf` | Lab 6-7 report |
