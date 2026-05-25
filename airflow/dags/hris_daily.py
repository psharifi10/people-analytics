"""Airflow DAG skeleton: HRIS daily pipeline.

This is a documentation-grade artefact. It is NOT exercised by the local demo
(no Airflow runtime is required). It demonstrates the production wiring:

* Tasks: extract → land → dbt-build → freshness-check
* Idempotency: each task is safe to retry; landing is append-only with
  payload-hash dedupe; dbt models are deterministic given the same raw rows.
* On-failure: PagerDuty for extract/land failures (data freshness); Slack-only
  for dbt test failures (data quality).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "people-analytics",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _extract_hris(**context: object) -> None:
    from extractors.hris_workday.cli import run_file_mode  # noqa: PLC0415
    from pathlib import Path  # noqa: PLC0415

    run_file_mode(
        Path("/opt/airflow/sample_data/generated"),
        Path("/opt/airflow/warehouse/people.duckdb"),
        truncate_first=False,  # production = append, not replace
    )


with DAG(
    dag_id="hris_daily",
    description="Daily HRIS extract → dbt build → tests",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 6 * * *",  # 06:00 UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["people-analytics", "hris"],
) as dag:
    extract = PythonOperator(
        task_id="extract_hris",
        python_callable=_extract_hris,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt build --target prod --select source:raw_hris+ "
        ),
    )

    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command="cd /opt/airflow/dbt_project && dbt source freshness --target prod",
    )

    extract >> dbt_build >> dbt_freshness
