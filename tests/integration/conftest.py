"""Integration harness: boots real Airflow 2 and Airflow 3 clusters in containers.

Each cluster runs `airflow standalone` in a single container with our seed DAGs
mounted and a Variable + Connection seeded via the CLI. The same test body runs
against both versions (parametrized), which also exercises the server's version
detection and per-version auth. Requires Docker; skipped otherwise.
"""

import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from airflow_dev_mcp import server

# Testcontainers' Ryuk reaper bind-mounts the Docker socket, which fails on some
# backends (e.g. colima: "operation not supported"). We stop containers explicitly
# in the fixture's finally block, so the reaper isn't needed.
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")

DAGS_ROOT = Path(__file__).parent / "dags"

CLUSTERS = {
    "af2": {
        "image": "apache/airflow:2.10.5",
        "prefix": "/api/v1",
        "password_path": "/opt/airflow/standalone_admin_password.txt",
        "env": {
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__API__AUTH_BACKENDS": (
                "airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
            ),
        },
    },
    "af3": {
        "image": "apache/airflow:3.2.1",
        "prefix": "/api/v2",
        "password_path": "/opt/airflow/simple_auth_manager_passwords.json.generated",
        "env": {"AIRFLOW__CORE__LOAD_EXAMPLES": "False"},
    },
}

# One triggered hello_dag run and one parse-confirmation per version, reused across tests.
_HELLO_RUNS: dict[str, str] = {}
_PARSED: set[str] = set()


def _docker_available() -> bool:
    try:
        import docker  # noqa: F401  (installed with testcontainers)

        client = docker.from_env()
        client.ping()
        return True
    except Exception:
        return False


def _exec(container, cmd):
    return container.get_wrapped_container().exec_run(cmd)


def _wait_http_ready(base_url: str, prefix: str, timeout: float = 300.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = httpx.get(f"{base_url}{prefix}/version", timeout=5)
            if r.status_code in (200, 401, 403):
                return
        except httpx.HTTPError:
            pass
        time.sleep(3)
    raise TimeoutError(f"{base_url}{prefix} not ready within {timeout}s")


def _admin_password(container, version: str, path: str) -> str:
    for _ in range(30):
        code, out = _exec(container, ["cat", path])
        if code == 0 and out.strip():
            text = out.decode().strip()
            if version == "af2":
                return text
            data = json.loads(text)
            return data.get("admin") or next(iter(data.values()))
        time.sleep(2)
    raise RuntimeError(f"could not read admin password at {path}")


@pytest.fixture(scope="session", params=["af2", "af3"])
def airflow_cluster(request):
    if not _docker_available():
        pytest.skip("Docker not available")
    from testcontainers.core.container import DockerContainer

    version = request.param
    cfg = CLUSTERS[version]

    container = DockerContainer(cfg["image"]).with_command("standalone")
    for k, v in cfg["env"].items():
        container.with_env(k, v)
    container.with_exposed_ports(8080)
    container.with_volume_mapping(str(DAGS_ROOT / version), "/opt/airflow/dags", mode="ro")

    container.start()
    try:
        base_url = f"http://{container.get_container_host_ip()}:{container.get_exposed_port(8080)}"
        _wait_http_ready(base_url, cfg["prefix"])
        password = _admin_password(container, version, cfg["password_path"])

        # Seed a Variable and a Connection (CLI is stable across AF2/AF3).
        _exec(container, ["airflow", "variables", "set", "test_var", "hello_value"])
        _exec(
            container,
            ["airflow", "connections", "add", "test_conn",
             "--conn-type", "http", "--conn-host", "example.com"],
        )

        yield SimpleNamespace(
            version=version, base_url=base_url, username="admin", password=password
        )
    finally:
        container.stop()


def wait_for(predicate, timeout: float = 90.0, interval: float = 3.0):
    """Poll `predicate` until it returns something truthy or the timeout elapses.

    For Airflow state that is only eventually consistent (import errors and DAG
    registration appear on a scheduler parse cycle). Returns the last result.
    """
    deadline = time.monotonic() + timeout
    result = predicate()
    while not result and time.monotonic() < deadline:
        time.sleep(interval)
        result = predicate()
    return result


@pytest.fixture
def poll():
    """Expose `wait_for` to tests for eventually-consistent Airflow state."""
    return wait_for


def _wait_for_dag(dag_id: str, timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if any(d.dag_id == dag_id for d in server.list_dags(limit=200).dags):
                return
        except Exception:
            pass
        time.sleep(3)
    raise TimeoutError(f"DAG {dag_id} was not parsed within {timeout}s")


@pytest.fixture
def configured(airflow_cluster, monkeypatch):
    """Point the server at the cluster, reset caches, and ensure DAGs have parsed."""
    monkeypatch.setenv("AIRFLOW_URL", airflow_cluster.base_url)
    monkeypatch.setenv("AIRFLOW_USERNAME", airflow_cluster.username)
    monkeypatch.setenv("AIRFLOW_PASSWORD", airflow_cluster.password)
    server._api_prefix_cache = None
    server._token_cache = None

    if airflow_cluster.version not in _PARSED:
        _wait_for_dag("hello_dag")
        _PARSED.add(airflow_cluster.version)
    return airflow_cluster


@pytest.fixture
def hello_run(configured):
    """A completed hello_dag run for this cluster (triggered once, reused)."""
    version = configured.version
    if version in _HELLO_RUNS:
        return _HELLO_RUNS[version]

    server.set_dag_paused("hello_dag", False)
    run_id = server.trigger_dag("hello_dag", conf={"seed": True}).dag_run_id
    for _ in range(90):
        state = server.get_run_status("hello_dag", run_id, include_tasks=False).run.state
        if state in ("success", "failed"):
            break
        time.sleep(2)
    _HELLO_RUNS[version] = run_id
    return run_id
