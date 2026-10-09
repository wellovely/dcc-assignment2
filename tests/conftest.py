"""Shared pytest fixtures: replicas run in-process on ephemeral ports and are stopped after each test."""
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)  # so tests can import server, client, clocks, counter_pb2

import server  # noqa: E402
from client import CounterClient  # noqa: E402


class Cluster:
    """N independent replicas, each with its own state, started with server.start_server(port=0)."""

    def __init__(self, log_dir, n, delay_ms=0):
        self.log_dir = log_dir
        self.replicas = []  # (grpc_server, port, servicer)
        for i in range(n):
            name = f"replica-{'ABC'[i]}"
            self.replicas.append(server.start_server(0, name, str(log_dir / f"{name}.log"), delay_ms=delay_ms))
        self._clients = []

    @property
    def addresses(self):
        return [f"localhost:{port}" for _, port, _ in self.replicas]

    def new_client(self, **options):
        options.setdefault("log_path", os.devnull)
        client = CounterClient(self.addresses, **options)
        self._clients.append(client)
        return client

    def stop(self, i):
        self.replicas[i][0].stop(0).wait()

    def snapshot(self, i):
        return self.replicas[i][2].snapshot()

    def log(self, i):
        return (self.log_dir / f"replica-{'ABC'[i]}.log").read_text()

    def shutdown(self):
        for client in self._clients:
            client.close()
        for grpc_server, _, _ in self.replicas:
            grpc_server.stop(0).wait()


@pytest.fixture
def running_server(tmp_path):
    """A single replica (Part A setup)."""
    cluster = Cluster(tmp_path, 1)
    yield cluster
    cluster.shutdown()


@pytest.fixture
def slow_server(tmp_path):
    """A single replica that replies to its first Increment after 800 ms."""
    cluster = Cluster(tmp_path, 1, delay_ms=800)
    yield cluster
    cluster.shutdown()


@pytest.fixture
def cluster(tmp_path):
    """Three replicas (Part C setup)."""
    cluster = Cluster(tmp_path, 3)
    yield cluster
    cluster.shutdown()
