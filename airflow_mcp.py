#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "mcp[cli]>=1.9.0",
#   "httpx>=0.27",
#   "pydantic>=2",
# ]
# ///
"""MCP server for driving a dev Airflow cluster over its REST API.

Configure via environment variables:
    AIRFLOW_URL             Base URL, e.g. http://localhost:8081. Default: http://localhost:8080.
    AIRFLOW_API_PREFIX      REST API path prefix. Default: /api/v2 (Airflow 3.x). Use /api/v1 for AF2.
    AIRFLOW_USERNAME        Username (used with AIRFLOW_PASSWORD).
    AIRFLOW_PASSWORD        Password.
    AIRFLOW_TOKEN           Explicit bearer token; skips creds/JWT exchange.
    AIRFLOW_AUTH_MODE       'auto' (default), 'jwt', or 'basic'.
    AIRFLOW_TOKEN_ENDPOINT  Path to exchange creds for a JWT. Default: /auth/token.
    AIRFLOW_TIMEOUT         HTTP timeout in seconds. Default: 30.
    AIRFLOW_VERIFY_SSL      'false' to skip TLS verification. Default: true.

Run:
    uv run --script airflow_mcp.py           # stdio MCP server (for Claude Code)
    uv run --script airflow_mcp.py --check   # one-shot connectivity check, then exit
"""
import os
import sys
from typing import Any
from urllib.parse import quote

import httpx
from mcp.server.fastmcp import FastMCP
from pydantic import AliasChoices, BaseModel, Field, field_validator

mcp = FastMCP("airflow-dev")

_token_cache: str | None = None


def _base_url() -> str:
    return os.environ.get("AIRFLOW_URL", "http://localhost:8080").rstrip("/")


def _api_prefix() -> str:
    prefix = os.environ.get("AIRFLOW_API_PREFIX", "/api/v2").rstrip("/")
    if not prefix.startswith("/"):
        prefix = "/" + prefix
    return prefix


def _timeout() -> float:
    return float(os.environ.get("AIRFLOW_TIMEOUT", "30"))


def _verify_ssl() -> bool:
    return os.environ.get("AIRFLOW_VERIFY_SSL", "true").lower() != "false"


def _exchange_jwt(user: str, pw: str) -> str | None:
    endpoint = os.environ.get("AIRFLOW_TOKEN_ENDPOINT", "/auth/token")
    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint
    try:
        r = httpx.post(
            f"{_base_url()}{endpoint}",
            json={"username": user, "password": pw},
            timeout=_timeout(),
            verify=_verify_ssl(),
        )
    except httpx.HTTPError:
        return None
    if not r.is_success:
        return None
    try:
        j = r.json()
    except ValueError:
        return None
    if not isinstance(j, dict):
        return None
    return j.get("access_token") or j.get("token") or j.get("jwt")


def _resolve_auth() -> tuple[dict[str, str], httpx.BasicAuth | None]:
    """Return (extra_headers, basic_auth) based on env vars.

    Precedence: AIRFLOW_TOKEN > (username+password with AIRFLOW_AUTH_MODE) > no auth.
    """
    global _token_cache

    if token := os.environ.get("AIRFLOW_TOKEN"):
        return {"Authorization": f"Bearer {token}"}, None

    user = os.environ.get("AIRFLOW_USERNAME")
    pw = os.environ.get("AIRFLOW_PASSWORD")
    if not (user and pw):
        return {}, None

    mode = os.environ.get("AIRFLOW_AUTH_MODE", "auto").lower()
    if mode not in ("auto", "jwt", "basic"):
        raise RuntimeError(f"Unknown AIRFLOW_AUTH_MODE: {mode!r} (want auto|jwt|basic)")

    if mode == "basic":
        return {}, httpx.BasicAuth(user, pw)

    if _token_cache is None:
        _token_cache = _exchange_jwt(user, pw)

    if _token_cache:
        return {"Authorization": f"Bearer {_token_cache}"}, None

    if mode == "jwt":
        raise RuntimeError(
            "JWT token exchange failed. Verify AIRFLOW_URL, credentials, and "
            "AIRFLOW_TOKEN_ENDPOINT, or set AIRFLOW_AUTH_MODE=basic for AF2."
        )

    return {}, httpx.BasicAuth(user, pw)


def _client() -> httpx.Client:
    extra_headers, basic = _resolve_auth()
    headers = {"Accept": "application/json"}
    headers.update(extra_headers)
    return httpx.Client(
        base_url=_base_url(),
        headers=headers,
        auth=basic,
        timeout=_timeout(),
        verify=_verify_ssl(),
    )


def _raise(resp: httpx.Response) -> None:
    if resp.is_success:
        return
    detail: Any = resp.text
    try:
        j = resp.json()
        if isinstance(j, dict):
            detail = j.get("detail") or j.get("message") or j
    except ValueError:
        pass
    raise RuntimeError(
        f"Airflow API {resp.request.method} {resp.request.url} "
        f"→ HTTP {resp.status_code}: {detail}"
    )


class DagRunSummary(BaseModel):
    """Condensed view of an Airflow DAG run.

    Populated directly from the API response; extra fields are ignored. The
    aliases absorb the AF2/AF3 naming differences (`run_id`/`execution_date`
    vs. `dag_run_id`/`logical_date`).
    """

    dag_id: str | None = None
    dag_run_id: str | None = Field(
        default=None, validation_alias=AliasChoices("dag_run_id", "run_id")
    )
    state: str | None = None
    run_type: str | None = None
    logical_date: str | None = Field(
        default=None, validation_alias=AliasChoices("logical_date", "execution_date")
    )
    start_date: str | None = None
    end_date: str | None = None
    note: str | None = None
    conf: dict[str, Any] | None = None


class TaskInstanceSummary(BaseModel):
    """Condensed view of a single task instance within a run."""

    task_id: str | None = None
    state: str | None = None
    try_number: int | None = None
    map_index: int | None = None
    operator: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    duration: float | None = None


class RunStatus(BaseModel):
    """A DAG run plus, optionally, its task instances."""

    run: DagRunSummary
    tasks: list[TaskInstanceSummary] | None = None


class TaskLogResult(BaseModel):
    """Logs for one task instance attempt."""

    content: str
    truncated: bool
    line_count: int
    try_number: int


class DagInfo(BaseModel):
    """Registration-level view of a DAG (not a specific run)."""

    dag_id: str | None = None
    is_paused: bool | None = None
    is_active: bool | None = None
    has_import_errors: bool | None = None
    fileloc: str | None = None
    description: str | None = None
    tags: list[str] | None = None
    next_dagrun: str | None = Field(
        default=None,
        validation_alias=AliasChoices("next_dagrun", "next_dagrun_logical_date"),
    )
    last_parsed_time: str | None = None

    @field_validator("tags", mode="before")
    @classmethod
    def _flatten_tags(cls, v: Any) -> Any:
        # Airflow returns tags as [{"name": "x"}, ...]; flatten to ["x", ...].
        if isinstance(v, list):
            return [t.get("name") if isinstance(t, dict) else t for t in v]
        return v


class DagList(BaseModel):
    dags: list[DagInfo]
    total_entries: int | None = None


class ImportErrorInfo(BaseModel):
    """A DAG parse failure recorded by the scheduler."""

    import_error_id: int | None = None
    timestamp: str | None = None
    filename: str | None = None
    stack_trace: str | None = None


class ImportErrorList(BaseModel):
    import_errors: list[ImportErrorInfo]
    total_entries: int | None = None


class DagRunList(BaseModel):
    dag_runs: list[DagRunSummary]
    total_entries: int | None = None


class ClearResult(BaseModel):
    """Result of a clearTaskInstances call."""

    dry_run: bool
    task_instances: list[TaskInstanceSummary]


class VariableInfo(BaseModel):
    key: str | None = None
    value: str | None = None
    description: str | None = None


class VariableList(BaseModel):
    variables: list[VariableInfo]
    total_entries: int | None = None


class ConnectionInfo(BaseModel):
    """Connection metadata. The API never returns the password."""

    connection_id: str | None = Field(
        default=None, validation_alias=AliasChoices("connection_id", "conn_id")
    )
    conn_type: str | None = None
    host: str | None = None
    db_schema: str | None = Field(
        default=None, validation_alias=AliasChoices("schema", "db_schema")
    )
    login: str | None = None
    port: int | None = None
    description: str | None = None


class ConnectionList(BaseModel):
    connections: list[ConnectionInfo]
    total_entries: int | None = None


@mcp.tool()
def trigger_dag(
    dag_id: str,
    conf: dict[str, Any] | None = None,
    logical_date: str | None = None,
    note: str | None = None,
) -> DagRunSummary:
    """Trigger a manual run of a DAG in the dev Airflow cluster.

    Args:
        dag_id: DAG identifier as it appears in Airflow.
        conf: Optional dict passed to the run (accessible as `dag_run.conf` inside tasks).
        logical_date: Optional ISO-8601 timestamp for the run's logical date. Defaults to now.
        note: Optional human-readable note attached to the run.

    Returns:
        DagRunSummary for the created run, including `dag_run_id` needed for status/log lookups.
        Note: if the DAG is paused, the run is created in `queued` state but will not execute
        until the DAG is unpaused in the Airflow UI.
    """
    body: dict[str, Any] = {}
    if conf is not None:
        body["conf"] = conf
    if logical_date:
        body["logical_date"] = logical_date
    if note:
        body["note"] = note

    with _client() as c:
        resp = c.post(f"{_api_prefix()}/dags/{quote(dag_id, safe='')}/dagRuns", json=body)
        _raise(resp)
        return DagRunSummary.model_validate(resp.json())


@mcp.tool()
def get_run_status(
    dag_id: str,
    run_id: str,
    include_tasks: bool = True,
) -> RunStatus:
    """Get the state of a DAG run and (optionally) its task instances.

    Args:
        dag_id: DAG identifier.
        run_id: DAG run identifier returned by `trigger_dag`
            (e.g. `manual__2026-07-02T14:23:11+00:00`).
        include_tasks: When True (default), also fetch per-task states.

    Returns:
        RunStatus with `run` (a DagRunSummary) and, if requested, `tasks` (a list of
        TaskInstanceSummary: task_id, state, try_number, operator, start/end dates,
        duration, map_index). `tasks` is null when include_tasks is False.
    """
    prefix = _api_prefix()
    dag = quote(dag_id, safe="")
    run = quote(run_id, safe="")

    with _client() as c:
        r = c.get(f"{prefix}/dags/{dag}/dagRuns/{run}")
        _raise(r)
        status = RunStatus(run=DagRunSummary.model_validate(r.json()))
        if include_tasks:
            r2 = c.get(f"{prefix}/dags/{dag}/dagRuns/{run}/taskInstances")
            _raise(r2)
            status.tasks = [
                TaskInstanceSummary.model_validate(ti)
                for ti in r2.json().get("task_instances", [])
            ]
        return status


@mcp.tool()
def get_task_logs(
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: int = 1,
    map_index: int = -1,
    tail_lines: int | None = 500,
) -> TaskLogResult:
    """Fetch logs for a single task instance attempt.

    Args:
        dag_id: DAG identifier.
        run_id: DAG run identifier.
        task_id: Task identifier within the DAG.
        try_number: Attempt number (1-indexed). Retried tasks have multiple attempts —
            call `get_run_status` to see the latest `try_number` per task.
        map_index: Mapped task index for dynamic task mapping. Use -1 for a normal task.
        tail_lines: Return only the last N lines (default 500). Pass null for the full log —
            beware, large tasks can produce many MB of output that will blow up context.

    Returns:
        TaskLogResult with `content` (log text), `truncated` (True if tailing dropped earlier
        lines), `line_count` (lines returned), and `try_number` (echoed back).
    """
    prefix = _api_prefix()
    dag = quote(dag_id, safe="")
    run = quote(run_id, safe="")
    task = quote(task_id, safe="")

    params: dict[str, Any] = {"full_content": "true"}
    if map_index >= 0:
        params["map_index"] = map_index

    with _client() as c:
        resp = c.get(
            f"{prefix}/dags/{dag}/dagRuns/{run}/taskInstances/{task}/logs/{try_number}",
            params=params,
            headers={"Accept": "text/plain"},
        )
        _raise(resp)

        text = resp.text
        ctype = resp.headers.get("content-type", "")
        if "application/json" in ctype:
            try:
                j = resp.json()
                if isinstance(j, dict) and "content" in j:
                    content = j["content"]
                    text = content if isinstance(content, str) else str(content)
            except ValueError:
                pass

        lines = text.splitlines()
        truncated = False
        if tail_lines is not None and len(lines) > tail_lines:
            lines = lines[-tail_lines:]
            truncated = True

        return TaskLogResult(
            content="\n".join(lines),
            truncated=truncated,
            line_count=len(lines),
            try_number=try_number,
        )


@mcp.tool()
def list_dags(
    limit: int = 100,
    offset: int = 0,
    dag_id_pattern: str | None = None,
    tags: list[str] | None = None,
) -> DagList:
    """List DAGs registered in the cluster with their paused / import-error flags.

    Use this to confirm a DAG parsed and registered. Important: a DAG whose file fails
    to import at module load does NOT appear here at all — call `get_import_errors` for that.

    Args:
        limit: Max DAGs to return (default 100).
        offset: Pagination offset.
        dag_id_pattern: Optional case-insensitive substring filter on dag_id.
        tags: Optional list of tags; only DAGs carrying one of them are returned.

    Returns:
        DagList with `dags` (dag_id, is_paused, is_active, has_import_errors, fileloc,
        description, tags, next_dagrun, last_parsed_time) and `total_entries`.
    """
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if dag_id_pattern:
        params["dag_id_pattern"] = dag_id_pattern
    if tags:
        params["tags"] = tags

    with _client() as c:
        r = c.get(f"{_api_prefix()}/dags", params=params)
        _raise(r)
        return DagList.model_validate(r.json())


@mcp.tool()
def get_import_errors(limit: int = 100, offset: int = 0) -> ImportErrorList:
    """List DAG import errors (parse failures) recorded by the scheduler.

    The primary debugging tool when a DAG you just wrote isn't showing up: a file that
    raises at import time is recorded here with its filename and full traceback.

    Args:
        limit: Max errors to return (default 100).
        offset: Pagination offset.

    Returns:
        ImportErrorList with `import_errors` (filename, stack_trace, timestamp,
        import_error_id) and `total_entries`.
    """
    with _client() as c:
        r = c.get(f"{_api_prefix()}/importErrors", params={"limit": limit, "offset": offset})
        _raise(r)
        return ImportErrorList.model_validate(r.json())


@mcp.tool()
def set_dag_paused(dag_id: str, paused: bool) -> DagInfo:
    """Pause or unpause a DAG.

    Locally, newly added DAGs are paused by default, so `trigger_dag` will queue a run
    that never executes until the DAG is unpaused. Call this with paused=False to enable it.

    Args:
        dag_id: DAG identifier.
        paused: True to pause, False to unpause.

    Returns:
        DagInfo reflecting the updated state.
    """
    with _client() as c:
        r = c.patch(
            f"{_api_prefix()}/dags/{quote(dag_id, safe='')}",
            json={"is_paused": paused},
        )
        _raise(r)
        return DagInfo.model_validate(r.json())


@mcp.tool()
def list_dag_runs(
    dag_id: str,
    limit: int = 25,
    offset: int = 0,
    state: list[str] | None = None,
) -> DagRunList:
    """List recent runs of a DAG — useful when you don't already hold a run_id.

    Args:
        dag_id: DAG identifier. Pass "~" to list runs across all DAGs.
        limit: Max runs to return (default 25).
        offset: Pagination offset.
        state: Optional filter, e.g. ["running"], ["failed"], ["success", "queued"].

    Returns:
        DagRunList with `dag_runs` (each a DagRunSummary) and `total_entries`.
    """
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if state:
        params["state"] = state

    with _client() as c:
        r = c.get(f"{_api_prefix()}/dags/{quote(dag_id, safe='')}/dagRuns", params=params)
        _raise(r)
        return DagRunList.model_validate(r.json())


@mcp.tool()
def clear_task_instances(
    dag_id: str,
    dag_run_id: str | None = None,
    task_ids: list[str] | None = None,
    only_failed: bool = False,
    reset_dag_runs: bool = True,
    dry_run: bool = True,
) -> ClearResult:
    """Clear task instances so they re-run — the fast way to re-test a task after a fix.

    Defaults to a DRY RUN: it reports which task instances *would* be cleared without
    touching them. Pass dry_run=False to actually clear; with reset_dag_runs=True the
    affected run is put back into a running state so cleared tasks re-execute.

    Args:
        dag_id: DAG identifier.
        dag_run_id: Restrict to a single run (recommended). If omitted, the API's other
            filters apply across runs.
        task_ids: Restrict to specific task_ids. If omitted, all matching tasks are cleared.
        only_failed: When True, only clear failed task instances.
        reset_dag_runs: When True (default), set affected runs back to running so cleared
            tasks are re-scheduled.
        dry_run: When True (default), preview only. Set False to actually clear.

    Returns:
        ClearResult with `dry_run` (echoed) and `task_instances` (the affected TIs).
    """
    body: dict[str, Any] = {
        "dry_run": dry_run,
        "only_failed": only_failed,
        "reset_dag_runs": reset_dag_runs,
    }
    if dag_run_id:
        body["dag_run_id"] = dag_run_id
    if task_ids:
        body["task_ids"] = task_ids

    with _client() as c:
        r = c.post(
            f"{_api_prefix()}/dags/{quote(dag_id, safe='')}/clearTaskInstances",
            json=body,
        )
        _raise(r)
        return ClearResult(
            dry_run=dry_run,
            task_instances=[
                TaskInstanceSummary.model_validate(ti)
                for ti in r.json().get("task_instances", [])
            ],
        )


@mcp.tool()
def list_variables(limit: int = 100, offset: int = 0) -> VariableList:
    """List Airflow Variables (read-only) — handy when troubleshooting why a task can't
    find config it expects.

    Values flagged sensitive by Airflow's secrets masker come back masked. Read-only by
    design: this tool cannot create or modify variables.

    Args:
        limit: Max variables to return (default 100).
        offset: Pagination offset.

    Returns:
        VariableList with `variables` (key, value, description) and `total_entries`.
    """
    with _client() as c:
        r = c.get(f"{_api_prefix()}/variables", params={"limit": limit, "offset": offset})
        _raise(r)
        return VariableList.model_validate(r.json())


@mcp.tool()
def list_connections(limit: int = 100, offset: int = 0) -> ConnectionList:
    """List Airflow Connections (read-only) — passwords are never returned by the API.

    Read-only by design: use it to confirm a connection exists with the expected
    conn_type / host / schema when a task fails to connect.

    Args:
        limit: Max connections to return (default 100).
        offset: Pagination offset.

    Returns:
        ConnectionList with `connections` (connection_id, conn_type, host, db_schema,
        login, port, description) and `total_entries`.
    """
    with _client() as c:
        r = c.get(f"{_api_prefix()}/connections", params={"limit": limit, "offset": offset})
        _raise(r)
        return ConnectionList.model_validate(r.json())


def _check() -> int:
    """One-shot connectivity check for debugging outside Claude Code."""
    try:
        with _client() as c:
            r = c.get(f"{_api_prefix()}/dags", params={"limit": 1})
            _raise(r)
            data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        print(f"OK — {_base_url()}{_api_prefix()} reachable, "
              f"total DAGs: {data.get('total_entries', '?')}")
        return 0
    except Exception as e:
        print(f"FAIL — {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    if "--check" in sys.argv:
        sys.exit(_check())
    mcp.run()
