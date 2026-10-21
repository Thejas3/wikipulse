"""Daily 03:00: OPTIMIZE (compact small files) -> VACUUM (7-day retention) on the Delta tables.

Airflow schedules it, but the Spark streaming app runs it: Delta's S3 log store assumes a
single writer process per table, so this DAG writes a request object and waits for the
pipeline's result object instead of writing to the tables itself (ADR 0006).
"""

import json
import os
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from botocore.exceptions import ClientError

from wikipulse_common import DEFAULT_ARGS

BUCKET = os.getenv("LAKE_BUCKET", "lakehouse")


def s3():
    return boto3.client("s3", endpoint_url="http://seaweedfs:8333", region_name="us-east-1",
                        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
                        aws_secret_access_key=os.environ["S3_SECRET_KEY"])


def request(run_id: str, **_):
    body = {"requested_at": datetime.utcnow().isoformat(timespec="seconds"), "requested_by": run_id}
    s3().put_object(Bucket=BUCKET, Key=f"_control/maintenance/requests/{run_id}.json", Body=json.dumps(body).encode())


def done(run_id: str, **_) -> bool:
    try:
        s3().head_object(Bucket=BUCKET, Key=f"_control/maintenance/done/{run_id}.json")
        return True
    except ClientError:
        return False


def report(run_id: str, **_):
    result = json.loads(s3().get_object(Bucket=BUCKET, Key=f"_control/maintenance/done/{run_id}.json")["Body"].read())
    print(json.dumps(result, indent=2))
    if result["status"] != "success":
        raise RuntimeError(f"maintenance failed: {result.get('error')}")


with DAG(
    dag_id="delta_maintenance",
    schedule="0 3 * * *",
    start_date=datetime(2026, 10, 20),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["wikipulse", "delta"],
) as dag:
    # run_id contains ':' and '+'; keep the object key S3-safe.
    rid = "{{ run_id | replace(':', '-') | replace('+', '-') }}"
    send = PythonOperator(task_id="request_maintenance", python_callable=request, op_kwargs={"run_id": rid})
    wait = PythonSensor(task_id="wait_for_pipeline", python_callable=done, op_kwargs={"run_id": rid},
                        mode="reschedule", poke_interval=60, timeout=int(timedelta(hours=2).total_seconds()))
    result = PythonOperator(task_id="report", python_callable=report, op_kwargs={"run_id": rid})
    send >> wait >> result
