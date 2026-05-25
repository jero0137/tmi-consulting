import logging
from pathlib import Path

import pandas as pd

from pipeline.utils import parse_job_skills, parse_job_type_skills

logger = logging.getLogger(__name__)

_BOOL_MAP: dict[str, bool] = {"True": True, "False": False}
_BOOL_COLS: list[str] = ["job_work_from_home", "job_no_degree_mention", "job_health_insurance"]
_NUMERIC_COLS: list[str] = ["salary_year_avg", "salary_hour_avg"]


def extract(filepath: str) -> pd.DataFrame:
    """Read the jobs CSV and return a typed DataFrame ready for loading.

    - Parses job_skills into list[str] and job_type_skills into dict.
    - Casts boolean columns from "True"/"False" strings to Python bool.
    - Casts job_posted_date to datetime and salary columns to float.
    - All other columns are kept as raw text; empty cells become empty strings.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")

    logger.info("Reading CSV from '%s'", path)
    # dtype=str + keep_default_na=False so every cell is a string, never NaN
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    logger.info("Read %d rows, %d columns", len(df), len(df.columns))

    df["job_skills"] = df["job_skills"].apply(parse_job_skills)
    df["job_type_skills"] = df["job_type_skills"].apply(parse_job_type_skills)

    for col in _BOOL_COLS:
        df[col] = df[col].map(_BOOL_MAP)

    df["job_posted_date"] = pd.to_datetime(df["job_posted_date"], errors="coerce")

    for col in _NUMERIC_COLS:
        df[col] = pd.to_numeric(df[col].replace("", None), errors="coerce")

    return df
