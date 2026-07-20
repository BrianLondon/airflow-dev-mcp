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

The easiest way is the `claude mcp add` CLI, which writes the config to the correct
place for you. From your project directory:

```bash
claude mcp add airflow-dev \
  -e AIRFLOW_URL=http://localhost:8080 \
  -e AIRFLOW_USERNAME=admin \
  -e AIRFLOW_PASSWORD=admin \
  -- uvx airflow-dev-mcp
```

Add `--scope user` to make it available in every project, or `--scope project` to write
a checked-in `.mcp.json` you can commit for your team (the default scope is local to you
in the current project).

To configure it by hand instead, create a `.mcp.json` file in the project root — this
is the file Claude Code reads for project-scoped MCP servers.

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

For a user-global setup, put the same `mcpServers` block in `~/.claude.json` instead.

### Other MCP clients

Any client that launches stdio MCP servers works the same way: run the command
`airflow-dev-mcp` (or `uvx airflow-dev-mcp`) with the environment variables below.

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `AIRFLOW_URL` | `http://localhost:8080` | Base URL of the cluster, no path. |
| `AIRFLOW_USERNAME` | — | Username. Used together with `AIRFLOW_PASSWORD`. |
| `AIRFLOW_PASSWORD` | — | Password. |
| `AIRFLOW_TIMEOUT` | `30` | HTTP timeout, in seconds. |
| `AIRFLOW_VERIFY_SSL` | `true` | Set `false` to skip TLS verification (self-signed dev certs). |

### Authentication

Just set `AIRFLOW_USERNAME` and `AIRFLOW_PASSWORD`. On first use the server detects whether
it's talking to Airflow 3 or Airflow 2 and authenticates accordingly.

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
