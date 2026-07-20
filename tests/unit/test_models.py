"""Model validation: cross-version aliases, the tag validator, extra-field tolerance."""

from airflow_dev_mcp import models


def test_dagrunsummary_af3_shape():
    m = models.DagRunSummary.model_validate(
        {"dag_id": "d", "dag_run_id": "r", "logical_date": "2026-07-01", "extra": "ignored"}
    )
    assert m.dag_run_id == "r" and m.logical_date == "2026-07-01"


def test_dagrunsummary_af2_aliases():
    m = models.DagRunSummary.model_validate(
        {"dag_id": "d", "run_id": "r2", "execution_date": "2026-07-02"}
    )
    assert m.dag_run_id == "r2" and m.logical_date == "2026-07-02"


def test_daginfo_flattens_tags():
    m = models.DagInfo.model_validate({"dag_id": "d", "tags": [{"name": "etl"}, {"name": "daily"}]})
    assert m.tags == ["etl", "daily"]


def test_daginfo_tags_plain_strings_passthrough():
    m = models.DagInfo.model_validate({"dag_id": "d", "tags": ["a", "b"]})
    assert m.tags == ["a", "b"]


def test_daginfo_tags_omitted():
    assert models.DagInfo.model_validate({"dag_id": "d"}).tags is None


def test_daginfo_tags_explicit_none():
    # exercises the validator's non-list passthrough branch
    assert models.DagInfo.model_validate({"dag_id": "d", "tags": None}).tags is None


def test_daginfo_next_dagrun_alias():
    m = models.DagInfo.model_validate({"dag_id": "d", "next_dagrun_logical_date": "2026-07-03"})
    assert m.next_dagrun == "2026-07-03"


def test_connectioninfo_aliases():
    m = models.ConnectionInfo.model_validate(
        {"conn_id": "c", "conn_type": "http", "schema": "public"}
    )
    assert m.connection_id == "c" and m.db_schema == "public"


def test_connectioninfo_never_exposes_password():
    m = models.ConnectionInfo.model_validate(
        {"connection_id": "c", "conn_type": "http", "password": "secret"}
    )
    assert "password" not in m.model_dump()


def test_list_wrappers_reuse_summaries():
    dl = models.DagRunList.model_validate(
        {"dag_runs": [{"dag_id": "d", "run_id": "r"}], "total_entries": 1}
    )
    assert dl.dag_runs[0].dag_run_id == "r" and dl.total_entries == 1
