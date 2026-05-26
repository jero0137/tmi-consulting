"""
Data Jobs Pipeline DAG — on-demand only (schedule=None).

Task graph:
    ingest_raw >> dbt_run >> apply_marts_constraints >> dbt_test

ingest_raw              — extract CSV, load raw.data_jobs, clean into
                          staging.stg_job_postings, run GX validation.
dbt_run                 — build all 6 mart tables (no constraints yet).
apply_marts_constraints — apply PKs, FKs and indexes from
                          sql/marts_constraints.sql once all tables exist.
dbt_test                — run all dbt schema + custom SQL tests.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

_CSV_PATH = "/opt/airflow/data/raw/data_jobs.csv"
_DBT_DIR = "/opt/airflow/dbt"
_SQL_CONSTRAINTS = "/opt/airflow/sql/marts_constraints.sql"


# ------------------------------------------------------------------ #
# Callables                                                           #
# ------------------------------------------------------------------ #


def _ingest_raw() -> None:
    """Extract CSV → raw.data_jobs → staging.stg_job_postings + GX."""
    from pipeline import run_ingest

    run_ingest(_CSV_PATH)


def _apply_marts_constraints() -> None:
    """Apply PKs, FKs and indexes to all mart tables."""
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )
    conn.autocommit = True
    with open(_SQL_CONSTRAINTS) as fh:
        sql = fh.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.close()
    log.info("marts_constraints.sql applied successfully")


# ------------------------------------------------------------------ #
# DAG                                                                 #
# ------------------------------------------------------------------ #

with DAG(
    dag_id="data_jobs_pipeline",
    description="ELT: CSV → raw → staging → marts 3NF model",
    schedule=None,  # triggered manually only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["elt", "data_jobs"],
) as dag:
    ingest_raw = PythonOperator(
        task_id="ingest_raw",
        python_callable=_ingest_raw,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt run --no-use-colors "
            f"--project-dir {_DBT_DIR} "
            f"--profiles-dir {_DBT_DIR}"
        ),
    )

    apply_marts_constraints = PythonOperator(
        task_id="apply_marts_constraints",
        python_callable=_apply_marts_constraints,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test --no-use-colors "
            f"--project-dir {_DBT_DIR} "
            f"--profiles-dir {_DBT_DIR}"
        ),
    )

    ingest_raw >> dbt_run >> apply_marts_constraints >> dbt_test
