"""Every 15 min: source freshness -> dbt run -> dbt snapshot -> dbt test (PRD §8).

The PRD lists snapshot before run; here run comes first because dim_user is snapshotted
from fct_edits, so the snapshot must see the edits the run just loaded.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from wikipulse_common import DBT, DEFAULT_ARGS, ROOT, TASK_ENV, WAREHOUSE

with DAG(
    dag_id="dbt_build",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 10, 20),
    catchup=False,
    max_active_runs=1,  # DuckDB allows one writer
    default_args=DEFAULT_ARGS,
    tags=["wikipulse", "dbt"],
) as dag:
    def dbt(cmd: str) -> str:
        return f"cd {ROOT}/dbt && {DBT} {cmd}"

    # The Airflow-side health check on the streaming jobs: warns when the newest Silver
    # row is over 5 minutes old, fails over 30 (sources.yml).
    freshness = BashOperator(task_id="source_freshness", bash_command=dbt("source freshness"),
                             env=TASK_ENV, append_env=True)
    run = BashOperator(task_id="dbt_run", bash_command=dbt("run"), env=TASK_ENV, append_env=True)
    snapshot = BashOperator(task_id="dbt_snapshot", bash_command=dbt("snapshot"), env=TASK_ENV, append_env=True)
    test = BashOperator(task_id="dbt_test", bash_command=dbt("test"), env=TASK_ENV, append_env=True,
                        outlets=[WAREHOUSE])

    freshness >> run >> snapshot >> test
