"""End-to-end tool behavior against real Airflow 2 and Airflow 3 clusters.

Every test runs against both versions via the parametrized `airflow_cluster`
fixture, so this also validates version detection and per-version auth.
"""

import pytest

from airflow_dev_mcp import server

pytestmark = pytest.mark.integration

MARKER = "HELLO_MARKER_LINE"


def test_list_dags_shows_hello(configured):
    dags = {d.dag_id: d for d in server.list_dags(limit=200).dags}
    assert "hello_dag" in dags


def test_get_import_errors_reports_broken(configured, poll):
    broken = poll(
        lambda: [
            e
            for e in server.get_import_errors().import_errors
            if e.filename and "broken_dag" in e.filename
        ]
    )
    assert broken, "expected an import error for broken_dag.py"
    assert "intentional" in (broken[0].stack_trace or "")


def test_set_dag_paused_roundtrip(configured):
    assert server.set_dag_paused("hello_dag", False).is_paused is False
    assert server.set_dag_paused("hello_dag", True).is_paused is True
    server.set_dag_paused("hello_dag", False)  # leave enabled for run tests


def test_trigger_and_status_reaches_success(hello_run):
    status = server.get_run_status("hello_dag", hello_run)
    assert status.run.state == "success"
    task_ids = {t.task_id for t in status.tasks}
    assert {"say_hello", "second"} <= task_ids


def test_get_task_logs_contains_marker(hello_run):
    logs = server.get_task_logs("hello_dag", hello_run, "say_hello", try_number=1)
    assert MARKER in logs.content
    assert logs.line_count > 0


def test_list_dag_runs_finds_run(hello_run):
    run_ids = {r.dag_run_id for r in server.list_dag_runs("hello_dag", limit=50).dag_runs}
    assert hello_run in run_ids


def test_clear_task_instances_dry_run(hello_run):
    result = server.clear_task_instances("hello_dag", dag_run_id=hello_run, dry_run=True)
    assert result.dry_run is True
    assert isinstance(result.task_instances, list)


def test_list_variables_has_seeded(configured):
    variables = {v.key: v.value for v in server.list_variables().variables}
    assert variables.get("test_var") == "hello_value"


def test_list_connections_has_seeded_without_password(configured):
    conns = {c.connection_id: c for c in server.list_connections().connections}
    assert "test_conn" in conns
    assert "password" not in conns["test_conn"].model_dump()
