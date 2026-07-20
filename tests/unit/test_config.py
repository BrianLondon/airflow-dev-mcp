"""Config helpers and API-version detection."""

import httpx
import pytest

from airflow_dev_mcp import server


def test_base_url_default(monkeypatch):
    monkeypatch.delenv("AIRFLOW_URL", raising=False)
    assert server._base_url() == "http://localhost:8080"


def test_base_url_custom_strips_trailing_slash(monkeypatch):
    monkeypatch.setenv("AIRFLOW_URL", "http://host:8081/")
    assert server._base_url() == "http://host:8081"


def test_timeout_default_and_custom(monkeypatch):
    monkeypatch.delenv("AIRFLOW_TIMEOUT", raising=False)
    assert server._timeout() == 30.0
    monkeypatch.setenv("AIRFLOW_TIMEOUT", "5")
    assert server._timeout() == 5.0


@pytest.mark.parametrize(
    "value,expected",
    [(None, True), ("true", True), ("false", False), ("False", False), ("0", True)],
)
def test_verify_ssl(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("AIRFLOW_VERIFY_SSL", raising=False)
    else:
        monkeypatch.setenv("AIRFLOW_VERIFY_SSL", value)
    assert server._verify_ssl() is expected


def _fake_get(responses):
    """Return a fake httpx.get yielding the given responses/exceptions in order."""
    calls = {"n": 0}

    def fake(url, **kwargs):
        i = calls["n"]
        calls["n"] += 1
        result = responses[i]
        if isinstance(result, Exception):
            raise result
        return httpx.Response(result)

    return fake, calls


def test_detect_prefix_v2(monkeypatch):
    fake, calls = _fake_get([200])
    monkeypatch.setattr(server.httpx, "get", fake)
    assert server._detect_api_prefix() == "/api/v2"
    assert calls["n"] == 1


def test_detect_prefix_falls_back_to_v1(monkeypatch):
    fake, calls = _fake_get([404, 200])
    monkeypatch.setattr(server.httpx, "get", fake)
    assert server._detect_api_prefix() == "/api/v1"
    assert calls["n"] == 2


def test_detect_prefix_both_404_defaults_v2(monkeypatch):
    fake, _ = _fake_get([404, 404])
    monkeypatch.setattr(server.httpx, "get", fake)
    assert server._detect_api_prefix() == "/api/v2"


def test_detect_prefix_unreachable_defaults_v2(monkeypatch):
    fake, _ = _fake_get([httpx.ConnectError("boom")])
    monkeypatch.setattr(server.httpx, "get", fake)
    assert server._detect_api_prefix() == "/api/v2"


def test_api_prefix_detects_once_and_caches(monkeypatch):
    calls = {"n": 0}

    def fake_detect():
        calls["n"] += 1
        return "/api/v2"

    monkeypatch.setattr(server, "_detect_api_prefix", fake_detect)
    assert server._api_prefix() == "/api/v2"
    assert server._api_prefix() == "/api/v2"
    assert calls["n"] == 1
