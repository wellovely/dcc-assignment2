# Replicated Counter Service

A gRPC counter service in Python: safe retries with idempotency keys, Lamport clocks,
and three replicas with majority (2 of 3) writes. The test report is `report.pdf`.

All commands are run from this folder.

## 1. Setup

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install grpcio grpcio-tools pytest
```

The generated files `counter_pb2.py` and `counter_pb2_grpc.py` are already in the repo.
To regenerate them after changing `counter.proto`:

```bash
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto
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

17 tests, about 6 seconds. `tests/test_counter.py` holds the Task C2 tests and
`tests/test_failures.py` holds the Task C3 failure tests.

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

## Server flags

| Flag | Meaning |
|---|---|
| `--port` | port to listen on |
| `--name` | name in the event log, e.g. `replica-A` |
| `--log` | event log file (default: stderr) |
| `--delay-ms N` | apply the first Increment, but reply N ms later |
| `--fault crash-before-apply` | exit when the first Increment arrives |
| `--fault crash-after-apply` | apply the first Increment, then exit without replying |

## Files

| File | What it is |
|---|---|
| `counter.proto` | gRPC interface |
| `server.py` | one replica |
| `client.py` | client: retries, quorum writes, `incr` / `get` commands |
| `clocks.py` | Lamport clock |
| `tests/` | tests and benchmark |
| `logs/` | captured event logs used in the report |
| `report.pdf` | test report |
