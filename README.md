# airflow-dev-mcp

Single-file MCP server that lets Claude Code trigger and inspect DAG runs on a dev
Airflow cluster via its REST API. No local Airflow source or filesystem coupling —
talks over HTTP only, configured entirely by environment variables.

## Tools

| Tool | Purpose |
| --- | --- |
| `trigger_dag(dag_id, conf?, logical_date?, note?)` | Start a manual DAG run. Returns `dag_run_id`. |
| `get_run_status(dag_id, run_id, include_tasks=true)` | Get run state + per-task states. |
| `get_task_logs(dag_id, run_id, task_id, try_number=1, map_index=-1, tail_lines=500)` | Fetch task logs, tailed by default. |

## Prerequisites

- [`uv`](https://docs.astral.sh/uv/) — `brew install uv`. Dependencies (`mcp`, `httpx`) are declared
  inline via PEP 723 and fetched on first run; no `pip install` step.
- A dev Airflow cluster reachable over HTTP from your machine.

## Configure Claude Code

Add this to `~/.claude.json` (or a project-level `.claude/settings.json`):

```json
{
  "mcpServers": {
    "airflow-dev": {
      "command": "uv",
      "args": ["run", "--script", "/Users/brianlondon/src/tmp/airflow-tool/airflow_mcp.py"],
      "env": {
        "AIRFLOW_URL": "http://localhost:8081",
        "AIRFLOW_USERNAME": "admin",
        "AIRFLOW_PASSWORD": "admin"
      }
    }
  }
}
```

Restart Claude Code. Tools appear as `mcp__airflow-dev__trigger_dag`, etc.

If `uv` isn't on PATH for Claude Code, use its absolute path (`which uv`).

## Environment variables

| Var | Default | Notes |
| --- | --- | --- |
| `AIRFLOW_URL` | `http://localhost:8080` | Base URL, no path. |
| `AIRFLOW_API_PREFIX` | `/api/v2` | Use `/api/v1` for Airflow 2.x. |
| `AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD` | — | Used together. |
| `AIRFLOW_TOKEN` | — | Explicit bearer token; skips creds. |
| `AIRFLOW_AUTH_MODE` | `auto` | `auto` (JWT with basic fallback), `jwt`, or `basic`. |
| `AIRFLOW_TOKEN_ENDPOINT` | `/auth/token` | JWT exchange path. |
| `AIRFLOW_TIMEOUT` | `30` | Seconds. |
| `AIRFLOW_VERIFY_SSL` | `true` | `false` skips TLS verification. |

## Auth cheatsheet

- **Airflow 3.x with SimpleAuthManager (default in the MWAA-style local docker image):**
  leave `AIRFLOW_AUTH_MODE=auto`. The server POSTs `{username, password}` to `/auth/token`,
  caches the JWT for the process lifetime, and sends it as `Authorization: Bearer …`.
- **Airflow 2.x:** set `AIRFLOW_API_PREFIX=/api/v1` and `AIRFLOW_AUTH_MODE=basic`.
- **Pre-existing token (e.g. CI):** set `AIRFLOW_TOKEN` and skip username/password.
- **MWAA production:** don't. This is a dev tool; MWAA auth needs IAM-signed requests.

## Smoke test

Outside Claude Code, verify the config end-to-end:

```
AIRFLOW_URL=http://localhost:8081 \
AIRFLOW_USERNAME=admin AIRFLOW_PASSWORD=admin \
  uv run --script airflow_mcp.py --check
```

Prints `OK — …` on success, `FAIL — …` on any error (auth, URL, API version).

## Typical Claude Code flow

1. `trigger_dag(dag_id="my_dag", conf={"date": "2026-07-01"})` → note `dag_run_id`.
2. `get_run_status(dag_id="my_dag", run_id="…")` — poll until `state` is `success`/`failed`.
3. If a task failed, `get_task_logs(dag_id, run_id, task_id, try_number=<latest from status>)`.
