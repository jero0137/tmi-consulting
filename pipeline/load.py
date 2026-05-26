import logging
import os
from pathlib import Path
from typing import ClassVar, Optional

import pandas as pd
import psycopg2
import psycopg2.extensions
from psycopg2.extras import Json, execute_values

from pipeline.utils import retry_with_backoff

logger = logging.getLogger(__name__)

_SCHEMA_FILE = Path(__file__).parent.parent / "sql" / "schema.sql"

_INSERT_SQL = """
INSERT INTO raw.data_jobs (
    job_title_short, job_title, job_location, job_via,
    job_schedule_type, job_work_from_home, search_location, job_posted_date,
    job_no_degree_mention, job_health_insurance, job_country, salary_rate,
    salary_year_avg, salary_hour_avg, company_name, job_skills, job_type_skills
) VALUES %s
"""


class DatabaseConnection:
    """Singleton that holds a single psycopg2 connection for the process lifetime.

    Calling get_connection() after the connection drops will transparently
    re-establish it. Use close() for explicit cleanup.
    """

    _instance: ClassVar[Optional["DatabaseConnection"]] = None
    _conn: Optional[psycopg2.extensions.connection]

    def __new__(cls) -> "DatabaseConnection":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._conn = None
        return cls._instance

    @retry_with_backoff(
        max_retries=3,
        base_delay=2.0,
        exceptions=(psycopg2.OperationalError,),
    )
    def get_connection(self) -> psycopg2.extensions.connection:
        """Return the active connection, re-establishing it if closed or broken."""
        if self._conn is None or self._conn.closed:
            logger.info("Opening new database connection to '%s'", os.environ["POSTGRES_DB"])
            self._conn = psycopg2.connect(
                host=os.environ["POSTGRES_HOST"],
                port=int(os.environ["POSTGRES_PORT"]),
                dbname=os.environ["POSTGRES_DB"],
                user=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"],
            )
            logger.info("Connected to '%s' on %s:%s", os.environ["POSTGRES_DB"],
                        os.environ["POSTGRES_HOST"], os.environ["POSTGRES_PORT"])
        return self._conn

    def close(self) -> None:
        """Close the connection and reset the singleton so the next call reconnects."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("Database connection closed")
        self._conn = None


def create_raw_table(conn: psycopg2.extensions.connection) -> None:
    """Run sql/schema.sql to create raw.data_jobs if it does not already exist."""
    ddl = _SCHEMA_FILE.read_text()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    logger.info("raw.data_jobs table is ready")


def load_to_postgres(df: pd.DataFrame, conn: psycopg2.extensions.connection) -> None:
    """Truncate raw.data_jobs and bulk-insert all rows from the DataFrame.

    Uses execute_values for efficient batch insertion.
    Idempotent: truncates before each load so re-runs are safe.
    """
    rows = [
        (
            row.job_title_short or None,
            row.job_title or None,
            row.job_location or None,
            row.job_via or None,
            row.job_schedule_type or None,
            row.job_work_from_home if pd.notna(row.job_work_from_home) else None,
            row.search_location or None,
            row.job_posted_date.to_pydatetime() if pd.notna(row.job_posted_date) else None,
            row.job_no_degree_mention if pd.notna(row.job_no_degree_mention) else None,
            row.job_health_insurance if pd.notna(row.job_health_insurance) else None,
            row.job_country or None,
            row.salary_rate or None,
            float(row.salary_year_avg) if pd.notna(row.salary_year_avg) else None,
            float(row.salary_hour_avg) if pd.notna(row.salary_hour_avg) else None,
            row.company_name or None,
            row.job_skills if row.job_skills else None,
            Json(row.job_type_skills) if row.job_type_skills else None,
        )
        for row in df.itertuples(index=False)
    ]

    with conn.cursor() as cur:
        cur.execute("TRUNCATE raw.data_jobs RESTART IDENTITY;")
        execute_values(cur, _INSERT_SQL, rows)
    conn.commit()
    logger.info("Loaded %d rows into raw.data_jobs", len(rows))
