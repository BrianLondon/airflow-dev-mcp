"""Fixtures for the mock-based unit tests."""

import httpx
import pytest

from airflow_dev_mcp import server


@pytest.fixture(autouse=True)
def _unit_env(monkeypatch):
    """Baseline environment for unit tests: a URL and credentials, no real network."""
    monkeypatch.setenv("AIRFLOW_URL", "http://af.test")
    monkeypatch.setenv("AIRFLOW_USERNAME", "u")
    monkeypatch.setenv("AIRFLOW_PASSWORD", "p")


@pytest.fixture
def mock_client(monkeypatch):
    """Install a fake `_client()` backed by an httpx MockTransport.

    Call it with a handler `(httpx.Request) -> httpx.Response`. It pins the API
    prefix to /api/v2 (so tools don't try to detect), routes every tool request
    through the handler, and returns a list that records the requests made so a
    test can assert on method / URL / params / body.
    """

    def install(handler):
        server._api_prefix_cache = "/api/v2"
        recorded: list[httpx.Request] = []

        def recording(request: httpx.Request) -> httpx.Response:
            recorded.append(request)
            return handler(request)

        transport = httpx.MockTransport(recording)
        monkeypatch.setattr(
            server,
            "_client",
            lambda: httpx.Client(transport=transport, base_url=server._base_url()),
        )
        return recorded

    return install
