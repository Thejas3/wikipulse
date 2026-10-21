"""Data quality outside dbt.

quality_report (daily): Great Expectations suites -> Data Docs (PRD §8).
lake_audit (hourly): Kafka -> Bronze -> Silver -> Gold reconciliation; fails the task on any
lost or duplicate event (follow-up to docs/incidents/2026-10-20-silver-late-drops.md).
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from wikipulse_common import DEFAULT_ARGS, QUALITY_PY, ROOT, TASK_ENV

with DAG(
    dag_id="quality_report",
    schedule="30 4 * * *",
    start_date=datetime(2026, 10, 20),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["wikipulse", "quality"],
) as quality_report:
    BashOperator(
        task_id="great_expectations",
        bash_command=f"cd {ROOT}/quality/great_expectations && {QUALITY_PY} run_checks.py "
                     f"--warehouse {ROOT}/warehouse/wikipulse.duckdb --fail-on-error",
        env=TASK_ENV,
        append_env=True,
    )

with DAG(
    dag_id="lake_audit",
    schedule="10 * * * *",
    start_date=datetime(2026, 10, 20),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 0},  # a failed audit should alert, not retry quietly
    tags=["wikipulse", "quality"],
) as lake_audit:
    BashOperator(
        task_id="audit",
        bash_command=f"{QUALITY_PY} {ROOT}/quality/audit_lake.py",
        env=TASK_ENV,
        append_env=True,
    )
