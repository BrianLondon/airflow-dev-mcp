# An airflow DAG that intentionally fails at import so get_import_errors has
# something to find. The words "airflow" and "dag" above are required: with
# dag_discovery_safe_mode on, Airflow only imports files containing both, so
# without them this file is skipped and no import error is ever recorded.
raise RuntimeError("intentional import failure for get_import_errors test")
