"""_raise error formatting and _log_text_from_response parsing."""

import json

import httpx
import pytest

from airflow_dev_mcp import server


def _resp(status, *, json_body=None, text=None, ctype=None):
    req = httpx.Request("GET", "http://af.test/api/v2/dags")
    headers = {}
    if ctype:
        headers["content-type"] = ctype
    if json_body is not None:
        return httpx.Response(status, json=json_body, request=req)
    return httpx.Response(status, content=(text or "").encode(), headers=headers, request=req)


def test_raise_noop_on_success():
    server._raise(_resp(200, json_body={"ok": True}))  # no exception


def test_raise_uses_json_detail():
    with pytest.raises(RuntimeError, match="boom"):
        server._raise(_resp(400, json_body={"detail": "boom"}))


def test_raise_uses_json_message():
    with pytest.raises(RuntimeError, match="msg"):
        server._raise(_resp(400, json_body={"message": "msg"}))


def test_raise_includes_method_and_status():
    with pytest.raises(RuntimeError, match="GET .* HTTP 500"):
        server._raise(_resp(500, json_body={"foo": "bar"}))


def test_raise_non_json_uses_text():
    with pytest.raises(RuntimeError, match="plain failure"):
        server._raise(_resp(503, text="plain failure", ctype="text/plain"))


# --- _log_text_from_response ----------------------------------------------------


def test_log_non_json_returns_text():
    assert server._log_text_from_response(_resp(200, text="raw log", ctype="text/plain")) == "raw log"


def test_log_af2_string_content():
    assert server._log_text_from_response(_resp(200, json_body={"content": "a\nb"})) == "a\nb"


def test_log_af3_list_of_event_dicts():
    resp = _resp(200, json_body={"content": [{"event": "x"}, {"message": "y"}, {"foo": 1}]})
    assert server._log_text_from_response(resp) == "x\ny\n" + json.dumps({"foo": 1})


def test_log_list_of_pairs():
    resp = _resp(200, json_body={"content": [["src", "hello"], []]})
    assert server._log_text_from_response(resp) == "hello\n"


def test_log_list_of_scalars():
    assert server._log_text_from_response(_resp(200, json_body={"content": [1, 2]})) == "1\n2"


def test_log_content_none_returns_text():
    resp = _resp(200, json_body={"content": None})
    assert server._log_text_from_response(resp) == resp.text


def test_log_invalid_json_returns_text():
    resp = _resp(200, text="{bad", ctype="application/json")
    assert server._log_text_from_response(resp) == "{bad"


def test_log_scalar_content():
    assert server._log_text_from_response(_resp(200, json_body={"content": 5})) == "5"
