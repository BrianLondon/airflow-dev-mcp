"""Every tool's request assembly, response parsing, and error path."""

import json

import httpx
import pytest

from airflow_dev_mcp import server
from airflow_dev_mcp.server import (
    clear_task_instances,
    get_import_errors,
    get_run_status,
    get_task_logs,
    list_connections,
    list_dag_runs,
    list_dags,
    list_variables,
    set_dag_paused,
    trigger_dag,
)


def _ok(body):
    return lambda req: httpx.Response(200, json=body)


# --- trigger_dag ----------------------------------------------------------------


def test_trigger_dag_minimal(mock_client):
    reqs = mock_client(_ok({"dag_id": "d", "dag_run_id": "run1", "state": "queued"}))
    res = trigger_dag("d")
    assert res.dag_run_id == "run1" and res.state == "queued"
    req = reqs[0]
    assert req.method == "POST"
    assert req.url.path == "/api/v2/dags/d/dagRuns"
    assert json.loads(req.content) == {"logical_date": None}


def test_trigger_dag_with_all_options(mock_client):
    reqs = mock_client(_ok({"dag_id": "d", "run_id": "r"}))
    trigger_dag("d", conf={"k": "v"}, logical_date="2026-07-01T00:00:00Z", note="hi")
    body = json.loads(reqs[0].content)
    assert body == {"conf": {"k": "v"}, "logical_date": "2026-07-01T00:00:00Z", "note": "hi"}


def test_trigger_dag_v1_omits_logical_date(mock_client):
    reqs = mock_client(_ok({"dag_id": "d", "run_id": "r"}))
    server._api_prefix_cache = "/api/v1"  # Airflow 2 rejects a null logical_date
    trigger_dag("d")
    assert json.loads(reqs[0].content) == {}
    assert reqs[0].url.path == "/api/v1/dags/d/dagRuns"


def test_trigger_dag_quotes_dag_id(mock_client):
    reqs = mock_client(_ok({"dag_id": "a/b", "dag_run_id": "r"}))
    trigger_dag("a/b")
    assert "a%2Fb" in reqs[0].url.raw_path.decode()


# --- get_run_status -------------------------------------------------------------


def test_get_run_status_with_tasks(mock_client):
    def handler(req):
        if req.url.path.endswith("/taskInstances"):
            return httpx.Response(200, json={"task_instances": [{"task_id": "t1", "state": "success"}]})
        return httpx.Response(200, json={"dag_id": "d", "dag_run_id": "r", "state": "success"})

    reqs = mock_client(handler)
    res = get_run_status("d", "r")
    assert res.run.state == "success"
    assert [t.task_id for t in res.tasks] == ["t1"]
    assert len(reqs) == 2


def test_get_run_status_without_tasks(mock_client):
    reqs = mock_client(_ok({"dag_id": "d", "dag_run_id": "r", "state": "running"}))
    res = get_run_status("d", "r", include_tasks=False)
    assert res.tasks is None
    assert len(reqs) == 1


# --- get_task_logs --------------------------------------------------------------


def test_get_task_logs_defaults(mock_client):
    reqs = mock_client(_ok({"content": "l1\nl2\nl3"}))
    res = get_task_logs("d", "r", "t")
    assert res.content == "l1\nl2\nl3"
    assert res.truncated is False and res.line_count == 3 and res.try_number == 1
    req = reqs[0]
    assert req.url.path == "/api/v2/dags/d/dagRuns/r/taskInstances/t/logs/1"
    assert req.url.params.get("full_content") == "true"
    assert "map_index" not in req.url.params
    assert req.headers["accept"] == "application/json"


def test_get_task_logs_map_index(mock_client):
    reqs = mock_client(_ok({"content": "x"}))
    get_task_logs("d", "r", "t", map_index=2)
    assert reqs[0].url.params.get("map_index") == "2"


def test_get_task_logs_tail_truncates(mock_client):
    mock_client(_ok({"content": "\n".join(f"line{i}" for i in range(10))}))
    res = get_task_logs("d", "r", "t", tail_lines=3)
    assert res.truncated is True
    assert res.line_count == 3
    assert res.content == "line7\nline8\nline9"


def test_get_task_logs_no_tail(mock_client):
    mock_client(_ok({"content": "a\nb\nc"}))
    res = get_task_logs("d", "r", "t", tail_lines=None)
    assert res.truncated is False and res.line_count == 3


def test_get_task_logs_tail_larger_than_log(mock_client):
    mock_client(_ok({"content": "a\nb"}))
    res = get_task_logs("d", "r", "t", tail_lines=100)
    assert res.truncated is False and res.line_count == 2


# --- list_dags ------------------------------------------------------------------


def test_list_dags_defaults(mock_client):
    reqs = mock_client(_ok({"dags": [{"dag_id": "d", "is_paused": True}], "total_entries": 1}))
    res = list_dags()
    assert res.total_entries == 1 and res.dags[0].dag_id == "d"
    p = reqs[0].url.params
    assert p.get("limit") == "100" and p.get("offset") == "0"
    assert "dag_id_pattern" not in p and "tags" not in p


def test_list_dags_with_filters(mock_client):
    reqs = mock_client(_ok({"dags": [], "total_entries": 0}))
    list_dags(dag_id_pattern="etl", tags=["a", "b"])
    p = reqs[0].url.params
    assert p.get("dag_id_pattern") == "etl"
    assert p.get_list("tags") == ["a", "b"]


# --- get_import_errors ----------------------------------------------------------


def test_get_import_errors(mock_client):
    reqs = mock_client(
        _ok({"import_errors": [{"filename": "bad.py", "stack_trace": "Trace"}], "total_entries": 1})
    )
    res = get_import_errors()
    assert res.import_errors[0].filename == "bad.py"
    assert reqs[0].url.path == "/api/v2/importErrors"


# --- set_dag_paused -------------------------------------------------------------


def test_set_dag_paused(mock_client):
    reqs = mock_client(_ok({"dag_id": "d", "is_paused": False}))
    res = set_dag_paused("d", False)
    assert res.is_paused is False
    req = reqs[0]
    assert req.method == "PATCH"
    assert json.loads(req.content) == {"is_paused": False}


# --- list_dag_runs --------------------------------------------------------------


def test_list_dag_runs_defaults(mock_client):
    reqs = mock_client(_ok({"dag_runs": [{"dag_id": "d", "run_id": "r"}], "total_entries": 1}))
    res = list_dag_runs("d")
    assert res.dag_runs[0].dag_run_id == "r"
    assert reqs[0].url.params.get("limit") == "25"
    assert "state" not in reqs[0].url.params


def test_list_dag_runs_state_filter_and_tilde(mock_client):
    reqs = mock_client(_ok({"dag_runs": [], "total_entries": 0}))
    list_dag_runs("~", state=["failed", "running"])
    assert reqs[0].url.params.get_list("state") == ["failed", "running"]
    assert "/dags/~/dagRuns" in reqs[0].url.raw_path.decode()


# --- clear_task_instances -------------------------------------------------------


def test_clear_task_instances_dry_run_default(mock_client):
    reqs = mock_client(_ok({"task_instances": [{"task_id": "t", "state": "success"}]}))
    res = clear_task_instances("d")
    assert res.dry_run is True
    assert [t.task_id for t in res.task_instances] == ["t"]
    body = json.loads(reqs[0].content)
    assert body == {"dry_run": True, "only_failed": False, "reset_dag_runs": True}


def test_clear_task_instances_full_body(mock_client):
    reqs = mock_client(_ok({"task_instances": []}))
    res = clear_task_instances(
        "d", dag_run_id="r", task_ids=["t1", "t2"], only_failed=True, reset_dag_runs=False, dry_run=False
    )
    assert res.dry_run is False
    body = json.loads(reqs[0].content)
    assert body == {
        "dry_run": False,
        "only_failed": True,
        "reset_dag_runs": False,
        "dag_run_id": "r",
        "task_ids": ["t1", "t2"],
    }


# --- list_variables / list_connections ------------------------------------------


def test_list_variables(mock_client):
    reqs = mock_client(
        _ok({"variables": [{"key": "k", "value": "v"}], "total_entries": 1})
    )
    res = list_variables()
    assert res.variables[0].key == "k"
    assert reqs[0].url.path == "/api/v2/variables"


def test_list_connections_no_password(mock_client):
    reqs = mock_client(
        _ok({"connections": [{"connection_id": "c", "conn_type": "http", "schema": "public"}], "total_entries": 1})
    )
    res = list_connections()
    conn = res.connections[0]
    assert conn.connection_id == "c" and conn.db_schema == "public"
    assert not hasattr(conn, "password")
    assert reqs[0].url.path == "/api/v2/connections"


# --- error path for every tool --------------------------------------------------

TOOL_CALLS = [
    lambda: trigger_dag("d"),
    lambda: get_run_status("d", "r"),
    lambda: get_task_logs("d", "r", "t"),
    lambda: list_dags(),
    lambda: get_import_errors(),
    lambda: set_dag_paused("d", True),
    lambda: list_dag_runs("d"),
    lambda: clear_task_instances("d"),
    lambda: list_variables(),
    lambda: list_connections(),
]


@pytest.mark.parametrize("call", TOOL_CALLS)
def test_tool_error_path_raises(mock_client, call):
    mock_client(lambda req: httpx.Response(500, json={"detail": "kaboom"}))
    with pytest.raises(RuntimeError, match="kaboom"):
        call()


# --- _check and main ------------------------------------------------------------


def test_check_success(mock_client, capsys):
    mock_client(_ok({"total_entries": 7}))
    assert server._check() == 0
    assert "OK" in capsys.readouterr().out


def test_check_failure(mock_client, capsys):
    mock_client(lambda req: httpx.Response(500, json={"detail": "nope"}))
    assert server._check() == 1
    assert "FAIL" in capsys.readouterr().err


def test_main_check_path(monkeypatch):
    monkeypatch.setattr(server.sys, "argv", ["airflow-dev-mcp", "--check"])
    monkeypatch.setattr(server, "_check", lambda: 0)
    with pytest.raises(SystemExit) as exc:
        server.main()
    assert exc.value.code == 0


def test_main_runs_server(monkeypatch):
    monkeypatch.setattr(server.sys, "argv", ["airflow-dev-mcp"])
    called = {"n": 0}
    monkeypatch.setattr(server.mcp, "run", lambda: called.__setitem__("n", called["n"] + 1))
    server.main()
    assert called["n"] == 1
