"""After each successful dbt_build: export dashboard snapshots to Parquet + manifest.json (PRD §9).

Writes to dashboard/data/. Publishing (Cloudflare R2, or the data-branch fallback) is added
in Milestone 6.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from wikipulse_common import DEFAULT_ARGS, QUALITY_PY, ROOT, TASK_ENV, WAREHOUSE

with DAG(
    dag_id="export_dashboard",
    schedule=[WAREHOUSE],  # data-aware: runs when dbt_build's tests pass
    start_date=datetime(2026, 10, 20),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["wikipulse", "dashboard"],
) as dag:
    BashOperator(
        task_id="export_parquet",
        bash_command=f"{QUALITY_PY} {ROOT}/dashboard/export.py --out {ROOT}/dashboard/data",
        env=TASK_ENV,
        append_env=True,
    )
