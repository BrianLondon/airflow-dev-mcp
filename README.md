# airflow-dev-mcp

An [MCP](https://modelcontextprotocol.io) server that lets an AI coding assistant
(Claude Code, Cursor, and other MCP clients) drive a **development or local Airflow
cluster** through its REST API — trigger DAG runs, watch their status, read task logs,
and diagnose parse errors, without leaving your editor.

It talks to Airflow over HTTP only. There's no dependency on your Airflow source tree,
no filesystem access, and no local config files — everything is set through environment
variables. It works against both **Airflow 3.x** (`/api/v2`, the default) and
**Airflow 2.x** (`/api/v1`).

> **Meant for dev/local clusters.** It's designed for the write-a-DAG / run-it / read-the-logs
> loop against a throwaway environment. Pointing it at a production cluster is not recommended.

## Install & run

The package ships a single console command, `airflow-dev-mcp`, which starts the MCP
server on stdio. You rarely run it by hand — your MCP client launches it for you (see
below). To try it directly, the zero-install option is [uv](https://docs.astral.sh/uv/):

```bash
uvx airflow-dev-mcp --check      # fetch + run a one-shot connectivity check
```

Or install it as a persistent tool:

```bash
uv tool install airflow-dev-mcp
# or
pipx install airflow-dev-mcp
```

## Configure your MCP client

### Claude Code

Add the server to `~/.claude.json` (applies everywhere) or a project's
`.claude/settings.json` (just that project):

```json
{
  "mcpServers": {
    "airflow-dev": {
      "command": "uvx",
      "args": ["airflow-dev-mcp"],
      "env": {
        "AIRFLOW_URL": "http://localhost:8081",
        "AIRFLOW_USERNAME": "admin",
        "AIRFLOW_PASSWORD": "admin"
      }
    }
  }
}
```

Using `uvx` means you don't have to manage a virtualenv — it fetches and caches the
package on first launch. If you'd rather pin an installed copy, replace the command with
`"command": "airflow-dev-mcp", "args": []` after `uv tool install`.

Restart Claude Code. The tools show up namespaced as `mcp__airflow-dev__trigger_dag`,
and so on.

### Other MCP clients

Any client that launches stdio MCP servers works the same way: run the command
`airflow-dev-mcp` (or `uvx airflow-dev-mcp`) with the environment variables below.

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `AIRFLOW_URL` | `http://localhost:8080` | Base URL of the cluster, no path. |
| `AIRFLOW_API_PREFIX` | `/api/v2` | API path prefix. Use `/api/v1` for Airflow 2.x. |
| `AIRFLOW_USERNAME` | — | Username. Used together with `AIRFLOW_PASSWORD`. |
| `AIRFLOW_PASSWORD` | — | Password. |
| `AIRFLOW_TOKEN` | — | Explicit bearer token; skips username/password entirely. |
| `AIRFLOW_AUTH_MODE` | `auto` | `auto`, `jwt`, or `basic` (see below). |
| `AIRFLOW_TOKEN_ENDPOINT` | `/auth/token` | Path used to exchange credentials for a JWT. |
| `AIRFLOW_TIMEOUT` | `30` | HTTP timeout, in seconds. |
| `AIRFLOW_VERIFY_SSL` | `true` | Set `false` to skip TLS verification (self-signed dev certs). |

### Authentication

- **Airflow 3.x** (the default local/MWAA-style image): leave `AIRFLOW_AUTH_MODE=auto`.
  The server posts your username/password to `/auth/token`, caches the returned JWT, and
  sends it as a bearer token on every request.
- **Airflow 2.x**: set `AIRFLOW_API_PREFIX=/api/v1` and `AIRFLOW_AUTH_MODE=basic` (2.x
  uses HTTP basic auth against the REST API).
- **Pre-issued token**: set `AIRFLOW_TOKEN` and omit the username/password.

## Tools

| Tool | What it does |
| --- | --- |
| `trigger_dag` | Start a manual DAG run, optionally with a `conf` payload. Returns the `dag_run_id`. |
| `get_run_status` | State of a run plus per-task states (task, state, try number, operator, timing). |
| `get_task_logs` | Logs for one task attempt, tailed to the last N lines by default. |
| `list_dag_runs` | Recent runs of a DAG — find a run when you don't already have its id. |
| `clear_task_instances` | Clear tasks so they re-run. Defaults to a dry-run preview. |
| `list_dags` | Registered DAGs with their paused / import-error / active flags. |
| `get_import_errors` | Parse failures with filename and traceback — *why a new DAG isn't showing up.* |
| `set_dag_paused` | Pause or unpause a DAG (new local DAGs start paused). |
| `list_variables` | Read Airflow Variables (read-only). |
| `list_connections` | Read Airflow Connections, minus passwords (read-only). |

The four `list_*` tools, `get_run_status`, `get_task_logs`, and `get_import_errors` are
strictly read-only. `trigger_dag`, `set_dag_paused`, and `clear_task_instances` change
cluster state. There are deliberately **no** tools that create or modify Variables or
Connections — that's cluster administration, out of scope for a DAG-development helper.

### A typical loop

1. Write or edit a DAG file; Airflow re-parses it.
2. `list_dags` to confirm it registered — or `get_import_errors` if it didn't.
3. `set_dag_paused(dag_id, paused=false)` to enable it (new DAGs start paused).
4. `trigger_dag(dag_id, conf={...})`, note the returned `dag_run_id`.
5. `get_run_status(dag_id, run_id)` until it finishes.
6. On failure, `get_task_logs(...)`; fix the code, then `clear_task_instances(dag_id,
   dag_run_id, dry_run=false)` to re-run just the affected tasks.

## Approving tools once (Claude Code)

Because each capability is its own MCP tool, Claude Code can remember your approval
per tool — unlike shell `curl` calls, which re-prompt whenever the command string
changes. When a tool first runs, choosing **"don't ask again"** persists an allow rule.
You can also pre-approve tools in settings so they never prompt.

A reasonable split is to allow the read-only tools and let the state-changing ones prompt.
In `.claude/settings.json` (project) or `~/.claude/settings.json` (global):

```json
{
  "permissions": {
    "allow": [
      "mcp__airflow-dev__list_dags",
      "mcp__airflow-dev__get_run_status",
      "mcp__airflow-dev__get_task_logs",
      "mcp__airflow-dev__get_import_errors",
      "mcp__airflow-dev__list_dag_runs",
      "mcp__airflow-dev__list_variables",
      "mcp__airflow-dev__list_connections"
    ]
  }
}
```

Leaving `trigger_dag`, `set_dag_paused`, and `clear_task_instances` off the list means
they still ask before acting.

## Verifying your setup

Before wiring it into an editor, confirm the URL and credentials work end-to-end:

```bash
AIRFLOW_URL=http://localhost:8081 \
AIRFLOW_USERNAME=admin AIRFLOW_PASSWORD=admin \
  uvx airflow-dev-mcp --check
```

It prints `OK — …` with the DAG count on success, or `FAIL — …` with the reason
(wrong URL, auth failure, or an `/api/v1` vs `/api/v2` mismatch).

## License

MIT — see [LICENSE](LICENSE).
