"""The MCP server surface: tool registration, output schemas, instructions."""

import asyncio

from airflow_dev_mcp import server

EXPECTED_TOOLS = {
    "trigger_dag",
    "get_run_status",
    "get_task_logs",
    "list_dags",
    "get_import_errors",
    "set_dag_paused",
    "list_dag_runs",
    "clear_task_instances",
    "list_variables",
    "list_connections",
}


def _tools():
    return asyncio.run(server.mcp.list_tools())


def test_all_tools_registered():
    assert {t.name for t in _tools()} == EXPECTED_TOOLS


def test_every_tool_has_output_schema():
    missing = [t.name for t in _tools() if not t.outputSchema]
    assert missing == []


def test_server_has_instructions():
    assert server.mcp.instructions
    assert "Typical development loop" in server.mcp.instructions
