"""JWT exchange, the refreshing auth flow, and auth selection."""

import httpx
import pytest

from airflow_dev_mcp import server


def _fake_post(response):
    def fake(url, **kwargs):
        if isinstance(response, Exception):
            raise response
        return response

    return fake


@pytest.mark.parametrize("key", ["access_token", "token", "jwt"])
def test_exchange_jwt_success_keys(monkeypatch, key):
    monkeypatch.setattr(server.httpx, "post", _fake_post(httpx.Response(201, json={key: "tok"})))
    assert server._exchange_jwt("u", "p") == "tok"


def test_exchange_jwt_non_2xx(monkeypatch):
    monkeypatch.setattr(server.httpx, "post", _fake_post(httpx.Response(401, json={})))
    assert server._exchange_jwt("u", "p") is None


def test_exchange_jwt_http_error(monkeypatch):
    monkeypatch.setattr(server.httpx, "post", _fake_post(httpx.ConnectError("down")))
    assert server._exchange_jwt("u", "p") is None


def test_exchange_jwt_non_json(monkeypatch):
    resp = httpx.Response(200, content=b"not json", headers={"content-type": "text/plain"})
    monkeypatch.setattr(server.httpx, "post", _fake_post(resp))
    assert server._exchange_jwt("u", "p") is None


def test_exchange_jwt_non_dict(monkeypatch):
    monkeypatch.setattr(server.httpx, "post", _fake_post(httpx.Response(200, json=["x"])))
    assert server._exchange_jwt("u", "p") is None


# --- _JwtAuth.auth_flow via MockTransport ---------------------------------------


def _run_auth(auth, responses):
    """Drive one GET through `auth` against a MockTransport; record request headers."""
    seen = []
    it = iter(responses)

    def handler(request):
        seen.append(request.headers.get("Authorization"))
        return next(it)

    client = httpx.Client(transport=httpx.MockTransport(handler), auth=auth, base_url="http://x")
    resp = client.get("/probe")
    return resp, seen


def test_jwt_auth_happy_path(monkeypatch):
    monkeypatch.setattr(server, "_exchange_jwt", lambda u, p: "tok")
    resp, seen = _run_auth(server._JwtAuth("u", "p"), [httpx.Response(200)])
    assert resp.status_code == 200
    assert seen == ["Bearer tok"]


def test_jwt_auth_refreshes_on_401(monkeypatch):
    tokens = iter(["tok1", "tok2"])
    calls = {"n": 0}

    def fake_exchange(u, p):
        calls["n"] += 1
        return next(tokens)

    monkeypatch.setattr(server, "_exchange_jwt", fake_exchange)
    resp, seen = _run_auth(
        server._JwtAuth("u", "p"), [httpx.Response(401), httpx.Response(200)]
    )
    assert resp.status_code == 200
    assert seen == ["Bearer tok1", "Bearer tok2"]
    assert calls["n"] == 2


def test_jwt_auth_persistent_401_is_bounded(monkeypatch):
    monkeypatch.setattr(server, "_exchange_jwt", lambda u, p: "bad")
    resp, seen = _run_auth(
        server._JwtAuth("u", "p"), [httpx.Response(401), httpx.Response(401)]
    )
    assert resp.status_code == 401
    assert len(seen) == 2  # original + exactly one retry, no loop


def test_jwt_auth_no_token_sends_no_header(monkeypatch):
    monkeypatch.setattr(server, "_exchange_jwt", lambda u, p: None)
    resp, seen = _run_auth(server._JwtAuth("u", "p"), [httpx.Response(200)])
    assert resp.status_code == 200
    assert seen == [None]


# --- _resolve_auth --------------------------------------------------------------


def test_resolve_auth_no_credentials(monkeypatch):
    monkeypatch.delenv("AIRFLOW_USERNAME", raising=False)
    monkeypatch.delenv("AIRFLOW_PASSWORD", raising=False)
    assert server._resolve_auth() is None


def test_resolve_auth_v1_is_basic():
    server._api_prefix_cache = "/api/v1"
    assert isinstance(server._resolve_auth(), httpx.BasicAuth)


def test_resolve_auth_v2_is_jwt():
    server._api_prefix_cache = "/api/v2"
    assert isinstance(server._resolve_auth(), server._JwtAuth)


def test_client_is_constructed(monkeypatch):
    # _client() is monkeypatched away in tool tests; exercise the real one here.
    server._api_prefix_cache = "/api/v2"
    monkeypatch.setenv("AIRFLOW_URL", "http://af.test")
    with server._client() as c:
        assert isinstance(c, httpx.Client)
        assert str(c.base_url).rstrip("/") == "http://af.test"
