"""
Data Jobs Pipeline DAG — on-demand only (schedule=None).

Task graph:
    ingest_raw >> create_marts_schema >> dbt_run >> dbt_test

ingest_raw          — extract CSV, load raw.data_jobs, clean into
                      staging.stg_job_postings, run GX validation.
create_marts_schema — apply sql/marts_schema.sql DDL (creates tables with
                      PKs, FKs and indexes if they do not already exist).
dbt_run             — build all marts models (post-hooks re-apply constraints).
dbt_test            — run all dbt schema + custom SQL tests.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

_CSV_PATH   = "/opt/airflow/data/raw/data_jobs.csv"
_DBT_DIR    = "/opt/airflow/dbt"
_SQL_SCHEMA = "/opt/airflow/sql/marts_schema.sql"


# ------------------------------------------------------------------ #
# Callables                                                           #
# ------------------------------------------------------------------ #

def _ingest_raw() -> None:
    """Extract CSV → raw.data_jobs → staging.stg_job_postings + GX."""
    from pipeline import run_ingest
    run_ingest(_CSV_PATH)


def _create_marts_schema() -> None:
    """Apply marts DDL so every table exists with correct structure."""
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )
    conn.autocommit = True
    with open(_SQL_SCHEMA) as fh:
        ddl = fh.read()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.close()
    log.info("marts_schema.sql applied successfully")


# ------------------------------------------------------------------ #
# DAG                                                                 #
# ------------------------------------------------------------------ #

with DAG(
    dag_id="data_jobs_pipeline",
    description="ELT: CSV → raw → staging → marts 3NF model",
    schedule=None,          # triggered manually only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["elt", "data_jobs"],
) as dag:

    ingest_raw = PythonOperator(
        task_id="ingest_raw",
        python_callable=_ingest_raw,
    )

    create_marts_schema = PythonOperator(
        task_id="create_marts_schema",
        python_callable=_create_marts_schema,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt run --no-use-colors "
            f"--project-dir {_DBT_DIR} "
            f"--profiles-dir {_DBT_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test --no-use-colors "
            f"--project-dir {_DBT_DIR} "
            f"--profiles-dir {_DBT_DIR}"
        ),
    )

    ingest_raw >> create_marts_schema >> dbt_run >> dbt_test
