# Data Jobs ELT Pipeline

An end-to-end ELT pipeline that ingests job postings data from a CSV file, loads it into PostgreSQL, cleanses it through a staging layer, and transforms it into a normalized 3NF relational model using dbt. The full pipeline is orchestrated with Apache Airflow and can be triggered on demand.

---

## Table of Contents

1. [Architecture & Design Decisions](#1-architecture--design-decisions)
2. [Project Structure](#2-project-structure)
3. [Execution Instructions](#3-execution-instructions)
4. [Testing Guide](#4-testing-guide)
5. [Continuous Integration (GitHub Actions)](#5-continuous-integration-github-actions)


---

## 1. Architecture & Design Decisions

### 1.1 ER Diagram

You can find the ER Diagram here [Diagrams](./Diagrams/er_diagram.png)

### 1.2 ELT over ETL

The pipeline follows an **ELT (Extract → Load → Transform)** pattern instead of the traditional ETL approach. The key distinction is that raw data is loaded into the database *before* any transformation is applied.

This decision was driven by the following reasons:

- **Raw data is preserved permanently.** Loading the CSV as-is into `raw.data_jobs` guarantees that the original source is always available for reprocessing and it makes more easy to analyze the raw data.
- **Transformations leverage the database engine.** dbt runs SQL transformations directly inside PostgreSQL, which is significantly more efficient than transforming large datasets in application memory before loading.
- **Separation of concerns.** Ingestion (Python) and transformation (dbt) are independent steps with clear boundaries. A failure in transformation never corrupts the raw data.
- **Iterative refinement.** Transformation rules can be updated and models rebuilt at any time without touching the ingestion layer.

### 1.3 Three-Layer Architecture: raw → staging → marts

The database is organized into three schemas, each with a distinct responsibility:

```
data_jobs.csv
      │
      │ Python — Extract & Load
      ▼
raw.data_jobs               ← Exact copy of the CSV. Never modified.
      │
      │ Python — Cleansing & Enrichment
      ▼
staging.stg_job_postings    ← Cleansed, typed, enriched. Still one row per job.
      │
      │ dbt — 3NF Transformation
      ▼
marts.companies             ← Deduplicated dimension tables
marts.locations
marts.job_schedules
marts.skills
marts.job_postings          ← Fact table with FK references to dimensions
marts.job_skills            ← Bridge table (job ↔ skill many-to-many)
```

**Why three layers instead of two?**

This layered approach makes the pipeline transparent at every stage: if a mart value looks wrong, you can compare it against the staging row, and then against the raw source record.

### 1.4 Python for Ingestion and Staging Cleansing

Python was chosen for the ingestion and staging steps for the following reasons:

- **Semi-structured column parsing.** Nested data structures add significant complexity to data quality handling, and Python provides the flexibility needed to address that complexity reliably.
- **Language detection.** Job titles are detected for language using `lingua-language-detector` to populate `job_title_lang` and `job_title_lang_confidence`. This enrichment requires a machine-learning model not available in SQL.
- **Flexibility for cleansing rules.** Business rules such as salary normalization, company name trimming, and schedule type standardization are easier to express, test, and iterate on in Python than in SQL stored procedures.
- **Data quality validation.** Great Expectations is integrated directly into the Python pipeline. After the staging table is populated, a suite of expectations is run against `staging.stg_job_postings`. If any expectation fails, the pipeline raises a `DataQualityError` and halts before dbt runs — preventing dirty data from reaching the marts layer.


### 1.5 Apache Airflow for Orchestration

Apache Airflow 2.9.1 was chosen to orchestrate the pipeline because:

- **Multi-phase pipeline coordination.** The pipeline has four distinct phases (ingestion, DDL, dbt run, dbt test) with strict ordering requirements. Airflow's DAG model makes these dependencies explicit and enforced.
- **Separation of orchestration from pipeline logic.** The DAG file is a thin wrapper that calls pipeline code. Business logic stays in `pipeline/` and `dbt/`; Airflow only manages execution order and failure handling.
- **Industry standard.** Airflow is the most widely adopted orchestrator for data engineering workflows, making the pipeline maintainable by any data team.

### 1.6 uv for Dependency Management

`uv` was chosen over pip/Poetry/PDM as the Python package manager because:

- **Speed.** Written in Rust, uv resolves and installs dependencies 10–100× faster than pip, which matters during Docker image builds and CI runs.
- **Lightweight.** Unlike Poetry or PDM, uv has no dependency on a separate build backend and does not require global installation hooks.

---

## 2. Project Structure

```
data_jobs_pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml                  # GitHub Actions: ruff lint + pytest
├── airflow/
│   ├── dags/
│   │   └── data_jobs_dag.py        # Airflow DAG (on-demand, 4 tasks)
│   └── logs/
├── dbt/
│   ├── models/
│   │   └── marts/
│   │       ├── schema.yml          # Source declaration + all dbt tests
│   │       ├── companies.sql
│   │       ├── locations.sql
│   │       ├── job_schedules.sql
│   │       ├── skills.sql
│   │       ├── job_postings.sql
│   │       └── job_skills.sql
│   ├── tests/                      # Custom SQL data tests
│   ├── dbt_project.yml
│   └── profiles.yml
├── pipeline/
│   ├── __init__.py                 # run_ingest() orchestrator
│   ├── __main__.py                 # CLI entry point
│   ├── extract.py                  # CSV reader
│   ├── load.py                     # PostgreSQL bulk loader
│   ├── transform.py                # Staging cleansing rules
│   ├── quality.py                  # Great Expectations validation
│   └── utils.py                    # parse_job_skills, parse_job_type_skills
├── sql/
│   ├── schema.sql                  # raw.data_jobs DDL
│   ├── staging_schema.sql          # staging.stg_job_postings DDL
│   └── marts_schema.sql            # marts tables DDL (PKs, FKs, indexes)
├── tests/
│   ├── test_utils.py               # pytest — parsing functions
│   ├── test_clean.py               # pytest — cleansing rules
│   └── test_transform.py           # pytest — staging transforms
├── data/
│   └── raw/
│       └── data_jobs.csv
├── init-db.sql                     # Schema creation + user grants (DB startup)
├── Dockerfile.airflow              # Airflow image with dbt + pipeline deps
├── docker-compose.yml
├── pyproject.toml
├── .env.example
└── diagram.dbml                    # ERD source (dbdiagram.io)
```

---

## 3. Execution Instructions

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (includes Docker Compose)
- [uv](https://docs.astral.sh/uv/getting-started/installation/) — only needed for local runs outside Docker
- Git

### 3.1 Clone the repository

```bash
git clone <repository-url>
cd data_jobs_pipeline
```

### 3.2 Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in the required values:

```dotenv
# PostgreSQL — data warehouse
POSTGRES_USER=pipeline_user
POSTGRES_PASSWORD=<choose a password>
POSTGRES_DB=tmidb
POSTGRES_PORT=5432

# PostgreSQL — Airflow metadata
AIRFLOW_POSTGRES_USER=airflow
AIRFLOW_POSTGRES_PASSWORD=<choose a password>
AIRFLOW_POSTGRES_DB=airflow_metadata

# Airflow
AIRFLOW__CORE__FERNET_KEY=<generate: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
AIRFLOW__WEBSERVER__SECRET_KEY=<any random string>
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=<choose a password>
AIRFLOW_ADMIN_EMAIL=admin@example.com
```

### 3.3 Place the source data file

Ensure the CSV is at:

```
data/raw/data_jobs.csv
```

### 3.4 Build the Docker image and start services

The first build installs dbt and all pipeline dependencies into the Airflow image. This takes a few minutes once and is cached on subsequent starts.

```bash
docker compose build
docker compose up -d
```

Wait until all containers are healthy (typically 60–90 seconds):

```bash
docker compose ps
```

| Service | Port | Description |
|---|---|---|
| `postgres_data` | `5432` | Data warehouse |
| `postgres_airflow` | — | Airflow metadata DB |
| `airflow-webserver` | `8080` | Airflow UI |
| `airflow-scheduler` | — | DAG scheduler |

### 3.5 Run the pipeline via Airflow (recommended)

1. Open **http://localhost:8080** and log in with the credentials from `.env`
2. Find the DAG named **`data_jobs_pipeline`**
3. Click **▶ Trigger DAG**
4. Monitor each task in the Grid or Graph view

The four tasks run in sequence:

| Task | Approx. duration | What it does |
|---|---|---|
| `ingest_raw` | 10–20 min | Loads 785 K rows to raw, cleanses into staging, runs GX validation |
| `create_marts_schema` | < 1 s | Applies DDL — creates mart tables with PKs, FKs, and indexes |
| `dbt_run` | 5–10 min | Builds all 6 mart models |
| `dbt_test` | 2–5 min | Runs 35 data tests |

To trigger from the command line instead:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger data_jobs_pipeline
```

### 3.6 Run the pipeline locally (without Airflow)

Requires `uv` and direct access to PostgreSQL on `localhost:5432`.

```bash
set -a && source <(sed 's/\r//' .env | grep -E '^[A-Z]') && set +a \
  && uv run python -m pipeline \
  && PGPASSWORD=$POSTGRES_PASSWORD psql \
       -h $POSTGRES_HOST -p $POSTGRES_PORT \
       -U $POSTGRES_USER -d $POSTGRES_DB \
       -f sql/marts_schema.sql \
  && uv run dbt run  --project-dir dbt --profiles-dir dbt \
  && uv run dbt test --project-dir dbt --profiles-dir dbt
```

### 3.7 Shut down

```bash
docker compose down       # stop containers, keep data volumes
docker compose down -v    # stop containers and delete all data
```

---

## 4. Testing Guide

The project has two independent test suites: **pytest** for Python unit tests and **dbt tests** for data quality on the mart tables.

### 4.1 Python unit tests (pytest)

Unit tests cover the parsing utilities in `pipeline/utils.py` and the cleansing rules in `pipeline/transform.py`. They run without a database connection — all I/O is mocked.

**Run locally:**

```bash
uv run pytest tests/ -v
```

**Run inside the Airflow container:**

```bash
docker compose exec airflow-scheduler pytest /opt/airflow/tests/ -v
```

**Coverage:**

| File | What is tested |
|---|---|
| `tests/test_utils.py` | `parse_job_skills()` — valid list, null, empty string, malformed input |
| `tests/test_utils.py` | `parse_job_type_skills()` — valid dict, null, empty string, malformed input |
| `tests/test_clean.py` | Salary normalization, boolean coercion, company name trimming |
| `tests/test_transform.py` | Staging cleansing rules end-to-end |

Expected output:

```
tests/test_utils.py ........    PASSED
tests/test_clean.py ........    PASSED
tests/test_transform.py .....   PASSED
```

### 4.2 Great Expectations (automated — runs inside `ingest_raw`)

GX runs automatically as the last step of Python ingestion. It validates `staging.stg_job_postings` before dbt is allowed to run:

| Column | Expectation |
|---|---|
| `id` | Not null, unique |
| `cleaned_at` | Not null |
| `location_is_remote` | Not null |
| `job_title_lang_confidence` | Between 0.0 and 1.0 |
| `location_format` | In `{remote, country_only, city_country, city_state_country}` |
| `salary_year_avg` | ≥ 0 when not null |
| `salary_hour_avg` | ≥ 0 when not null |
| Table | At least 1 row |

If any expectation fails, a `DataQualityError` is raised and the pipeline stops.

### 4.3 dbt tests

dbt tests run as the `dbt_test` task in Airflow but can also be executed independently after any `dbt run`.

**Run locally:**

```bash
set -a && source <(sed 's/\r//' .env | grep -E '^[A-Z]') && set +a \
  && uv run dbt test --project-dir dbt --profiles-dir dbt
```

**Run inside the Airflow container:**

```bash
docker compose exec airflow-scheduler \
  dbt test --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt
```

**Test inventory (35 total):**

| Model | Tests |
|---|---|
| `companies` | `not_null`, `unique` on `company_id`; `not_null` on `company_name` |
| `locations` | `not_null`, `unique` on `location_id`; `not_null` on `is_remote` |
| `job_schedules` | `not_null`, `unique` on `schedule_id`; `not_null` on `schedule_type` |
| `skills` | `not_null`, `unique` on `skill_id`; `not_null` on `skill_name`, `skill_category`; `accepted_values` on `skill_category` |
| `job_postings` | `not_null`, `unique` on `job_id`; `not_null` + `relationships` on `company_id`, `location_id`, `schedule_id`; `accepted_values` on `salary_rate` |
| `job_skills` | `not_null` on `job_id`, `skill_id`; `relationships` to `job_postings` and `skills` |
| Custom SQL | `test_salary_year_avg_positive`, `test_salary_hour_avg_positive`, `test_job_posted_date_not_future`, `test_job_skills_no_duplicates` |


---

## 5. Continuous Integration (GitHub Actions)

This repository uses **GitHub Actions** for CI.

- Workflow file: `.github/workflows/ci.yml`
- Triggered on every push to `main` and every pull request targeting `main`
- Runs the same quality gates used locally:
  - `uv run ruff check .`
  - `uv run ruff format --check .`
  - `uv run pytest -v`

CI ensures linting, formatting, and tests pass before changes are merged.

