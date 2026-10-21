"""Shared settings for the WikiPulse DAGs."""

from datetime import timedelta

from airflow.datasets import Dataset

ROOT = "/opt/wikipulse"
DBT = "/opt/dbt-venv/bin/dbt"
QUALITY_PY = "/opt/quality-venv/bin/python"

# Updated by dbt_build when its tests pass; export_dashboard runs on each update, so a
# failed quality check blocks the public export (PRD §8).
WAREHOUSE = Dataset("wikipulse://warehouse/wikipulse.duckdb")

DEFAULT_ARGS = {
    "owner": "wikipulse",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# dbt and the quality scripts read the lake through the in-network S3 endpoint.
TASK_ENV = {
    "DBT_DUCKDB_PATH": f"{ROOT}/warehouse/wikipulse.duckdb",
    "DBT_S3_ENDPOINT": "seaweedfs:8333",
    "S3_ENDPOINT_HOST": "seaweedfs:8333",
    "KAFKA_BOOTSTRAP": "kafka:9092",
    "DBT_PROFILES_DIR": f"{ROOT}/dbt",
    "DBT_LOG_PATH": "/tmp/dbt-logs",
    "DBT_TARGET_PATH": f"{ROOT}/dbt/target",
}
