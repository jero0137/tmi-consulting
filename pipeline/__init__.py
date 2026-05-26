import logging

from dotenv import load_dotenv

from pipeline.extract import extract
from pipeline.load import DatabaseConnection, create_raw_table, load_to_postgres
from pipeline.transform import transform_and_load_staging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


def run_ingest(csv_path: str) -> None:
    """Orchestrate the full extract → raw → staging pipeline.

    Loads env vars, opens a singleton DB connection, ensures raw.data_jobs
    exists, extracts the CSV, bulk-inserts into raw, then applies cleansing
    rules and writes to staging.stg_job_postings. Idempotent end to end.
    """
    load_dotenv()

    db = DatabaseConnection()
    try:
        conn = db.get_connection()
        create_raw_table(conn)
        df = extract(csv_path)
        load_to_postgres(df, conn)
        transform_and_load_staging(conn)
        logger.info("Ingestion pipeline completed successfully")
    except Exception as exc:
        logger.exception("Ingestion pipeline failed: %s", exc)
        raise
    finally:
        db.close()
