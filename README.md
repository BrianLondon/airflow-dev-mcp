# airflow-dev-mcp

An [MCP](https://modelcontextprotocol.io) server that lets an AI coding assistant
(Claude Code, Cursor, and other MCP clients) drive a development or local Airflow
cluster through Airflow's REST API. It can trigger DAG runs, watch their status, read task logs,
and diagnose parse errors.

It talks to Airflow over HTTP only. There's no dependency on your Airflow source tree,
no filesystem or database access, and no local config files. All configuration is set through environment
variables. It support both Airflow 3 (via `/api/v2`, the default) and
Airflow 2 (`/api/v1`).

> Airflow-dev-mcp is designed for the write-a-DAG / run-it / read-the-logs
> loop against a development and/or local environment. Pointing it at a production cluster is not recommended.

## Install & run

The package ships a single console command, `airflow-dev-mcp`, which starts the MCP
server on stdio. Installation requires [uv](https://docs.astral.sh/uv/).

_Note: most users will skip this and just add it to their coding environment (See: below)

To download and validate the package, run:

```bash
uvx airflow-dev-mcp --check      # fetch + run a one-shot connectivity check
```

It can be installed as a persistent tool but typical installation is to
just have your coding agent call it through `uvx` (See: Configure your
MCP client below). If you do want to install it system wide, use one of the 
two following commands

```bash
uv tool install airflow-dev-mcp
# or
pipx install airflow-dev-mcp
```

## Configure your MCP client

### Claude Code

For most users all you should need to do is add the server to `~/.claude.json` 
(applies everywhere) or a project's `.claude/settings.json` (just that project):

```json
{
  "mcpServers": {
    "airflow-dev": {
      "command": "uvx",
      "args": ["airflow-dev-mcp"],
      "env": {
        "AIRFLOW_URL": "http://localhost:8080",
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
Connections.

## License

MIT — see [LICENSE](LICENSE).
