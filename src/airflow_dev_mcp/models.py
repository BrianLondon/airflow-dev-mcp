"""Pydantic models returned by the MCP tools.

Each model is populated directly from an Airflow REST API response with
``model_validate``; unknown fields are ignored. Validation aliases absorb the
naming differences between the Airflow 2.x (``/api/v1``) and Airflow 3.x
(``/api/v2``) APIs so the same models work against both.
"""

from typing import Any

from pydantic import AliasChoices, BaseModel, Field, field_validator


class DagRunSummary(BaseModel):
    """Condensed view of an Airflow DAG run.

    The aliases absorb the AF2/AF3 naming differences (``run_id``/``execution_date``
    vs. ``dag_run_id``/``logical_date``).
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
