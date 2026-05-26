"""Read raw.data_jobs, apply cleansing rules, write to staging.stg_job_postings.

Designed to be idempotent: TRUNCATE before INSERT so re-runs produce the same
result. Language detection is the slowest step, so the unique-value cache and
lingua's batched parallel API are critical to keep wall time reasonable.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import psycopg2.extensions
from psycopg2.extras import Json, execute_values

from pipeline.clean import (
    _ARABIC_RE,
    _CHINESE_RE,
    _CYRILLIC_RE,
    _JAPANESE_RE,
    _KOREAN_RE,
    _get_lingua_detector,
    clean_company_name,
    normalize_schedule_type,
    normalize_string,
    parse_job_location,
    reconcile_country,
)

logger = logging.getLogger(__name__)

_DDL_FILE = Path(__file__).parent.parent / "sql" / "staging_schema.sql"

_INSERT_SQL = """
INSERT INTO staging.stg_job_postings (
    id, job_title_short, job_title, job_title_lang, job_title_lang_confidence,
    job_location, location_city, location_state, location_country,
    location_is_remote, location_format, job_via, job_schedule_type,
    job_work_from_home, search_location, country_final, job_posted_date,
    job_no_degree_mention, job_health_insurance, job_country, salary_rate,
    salary_year_avg, salary_hour_avg, company_name, job_skills, job_type_skills,
    loaded_at, cleaned_at
) VALUES %s
"""


def _na_to_none(value):
    """Convert pandas NaN/NaT to None; leave everything else alone."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _build_language_cache(titles: List[str]) -> Dict[str, Tuple[str, float]]:
    """Map each unique title to (iso_code, confidence).

    Splits inputs by script: non-Latin scripts are tagged from the regex check
    alone; the remaining Latin-alphabet strings go through one batched lingua
    call so the detector iterates over its language models only once.
    """
    cache: Dict[str, Tuple[str, float]] = {}
    latin: List[str] = []

    for title in titles:
        if not isinstance(title, str):
            continue
        stripped = title.strip()
        if len(stripped) < 3:
            cache[title] = ("unknown", 0.0)
            continue
        if _CYRILLIC_RE.search(stripped):
            cache[title] = ("cyrillic", 1.0)
        elif _CHINESE_RE.search(stripped):
            cache[title] = ("zh", 1.0)
        elif _JAPANESE_RE.search(stripped):
            cache[title] = ("ja", 1.0)
        elif _KOREAN_RE.search(stripped):
            cache[title] = ("ko", 1.0)
        elif _ARABIC_RE.search(stripped):
            cache[title] = ("ar", 1.0)
        else:
            latin.append(title)

    if not latin:
        return cache

    logger.info("Running lingua on %d unique Latin-script titles", len(latin))
    detector = _get_lingua_detector()
    stripped_latin = [t.strip() for t in latin]
    batch_results = detector.compute_language_confidence_values_in_parallel(stripped_latin)

    for original, confidences in zip(latin, batch_results):
        if not confidences:
            cache[original] = ("unknown", 0.0)
            continue
        top = confidences[0]
        cache[original] = (
            top.language.iso_code_639_1.name.lower(),
            float(top.value),
        )

    return cache


def create_staging_table(conn: psycopg2.extensions.connection) -> None:
    """Create the staging schema and stg_job_postings table if they don't exist."""
    ddl = _DDL_FILE.read_text()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    logger.info("staging.stg_job_postings table is ready")


def _row_to_tuple(
    row, lang_cache: Dict[str, Tuple[str, float]], cleaned_at: datetime
) -> tuple:
    """Apply all cleansing rules to a single raw row and return the INSERT tuple."""
    job_title_short = normalize_string(_na_to_none(row.job_title_short))
    job_title = normalize_string(_na_to_none(row.job_title))

    lang, lang_conf = lang_cache.get(job_title, ("unknown", 0.0)) if job_title else (None, None)

    job_location_raw = normalize_string(_na_to_none(row.job_location))
    city, state, country, is_remote, fmt = parse_job_location(job_location_raw)

    job_via = normalize_string(_na_to_none(row.job_via))
    schedule_type = normalize_schedule_type(_na_to_none(row.job_schedule_type))
    work_from_home = _na_to_none(row.job_work_from_home)
    search_location = normalize_string(_na_to_none(row.search_location))
    job_country = normalize_string(_na_to_none(row.job_country))
    country_final = reconcile_country(job_country, search_location)

    posted_date = _na_to_none(row.job_posted_date)
    if hasattr(posted_date, "to_pydatetime"):
        posted_date = posted_date.to_pydatetime()

    no_degree = _na_to_none(row.job_no_degree_mention)
    health_ins = _na_to_none(row.job_health_insurance)
    salary_rate = normalize_string(_na_to_none(row.salary_rate))
    salary_year = float(row.salary_year_avg) if pd.notna(row.salary_year_avg) else None
    salary_hour = float(row.salary_hour_avg) if pd.notna(row.salary_hour_avg) else None
    company_name = clean_company_name(_na_to_none(row.company_name))

    skills = row.job_skills if isinstance(row.job_skills, list) and row.job_skills else None
    type_skills_val = _na_to_none(row.job_type_skills)
    type_skills = Json(type_skills_val) if type_skills_val else None

    loaded_at = _na_to_none(row.loaded_at)
    if hasattr(loaded_at, "to_pydatetime"):
        loaded_at = loaded_at.to_pydatetime()

    return (
        int(row.id),
        job_title_short,
        job_title,
        lang,
        lang_conf,
        job_location_raw,
        city,
        state,
        country,
        is_remote,
        fmt,
        job_via,
        schedule_type,
        work_from_home,
        search_location,
        country_final,
        posted_date,
        no_degree,
        health_ins,
        job_country,
        salary_rate,
        salary_year,
        salary_hour,
        company_name,
        skills,
        type_skills,
        loaded_at,
        cleaned_at,
    )


def transform_and_load_staging(conn: psycopg2.extensions.connection) -> None:
    """Read raw.data_jobs, clean every row, and bulk-insert into staging.stg_job_postings."""
    create_staging_table(conn)

    logger.info("Reading raw.data_jobs")
    df = pd.read_sql("SELECT * FROM raw.data_jobs ORDER BY id", conn)
    logger.info("Read %d rows from raw.data_jobs", len(df))

    unique_titles = [t for t in df["job_title"].dropna().unique().tolist() if isinstance(t, str)]
    logger.info("Detecting language for %d unique job_title values", len(unique_titles))
    lang_cache = _build_language_cache(unique_titles)

    cleaned_at = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [_row_to_tuple(r, lang_cache, cleaned_at) for r in df.itertuples(index=False)]

    with conn.cursor() as cur:
        cur.execute("TRUNCATE staging.stg_job_postings;")
        execute_values(cur, _INSERT_SQL, rows, page_size=5000)
    conn.commit()
    logger.info("Loaded %d rows into staging.stg_job_postings", len(rows))
