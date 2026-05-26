import csv

import pandas as pd
import pytest

from pipeline.extract import extract


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_csv(tmp_path: pytest.TempPathFactory) -> str:
    """Write a minimal two-row CSV to a temp file and return its path."""
    rows = [
        {
            "job_title_short": "Data Engineer",
            "job_title": "Senior Data Engineer",
            "job_location": "New York, NY",
            "job_via": "via LinkedIn",
            "job_schedule_type": "Full-time",
            "job_work_from_home": "True",
            "search_location": "United States",
            "job_posted_date": "2023-06-16 13:44:15",
            "job_no_degree_mention": "False",
            "job_health_insurance": "True",
            "job_country": "United States",
            "salary_rate": "year",
            "salary_year_avg": "120000.0",
            "salary_hour_avg": "",
            "company_name": "Acme Corp",
            "job_skills": "['python', 'sql']",
            "job_type_skills": "{'programming': ['python', 'sql']}",
        },
        {
            "job_title_short": "Data Analyst",
            "job_title": "Data Analyst",
            "job_location": "Remote",
            "job_via": "via Indeed",
            "job_schedule_type": "Full-time",
            "job_work_from_home": "False",
            "search_location": "United States",
            "job_posted_date": "2023-01-14 13:18:07",
            "job_no_degree_mention": "True",
            "job_health_insurance": "False",
            "job_country": "United States",
            "salary_rate": "",
            "salary_year_avg": "",
            "salary_hour_avg": "25.0",
            "company_name": "Corp B",
            "job_skills": "",
            "job_type_skills": "",
        },
    ]

    path = tmp_path / "test_jobs.csv"
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    return str(path)


# ---------------------------------------------------------------------------
# Basic shape
# ---------------------------------------------------------------------------


def test_extract_returns_dataframe(sample_csv: str) -> None:
    df = extract(sample_csv)
    assert isinstance(df, pd.DataFrame)


def test_extract_row_count(sample_csv: str) -> None:
    df = extract(sample_csv)
    assert len(df) == 2


def test_extract_column_names(sample_csv: str) -> None:
    df = extract(sample_csv)
    expected = {
        "job_title_short",
        "job_title",
        "job_location",
        "job_via",
        "job_schedule_type",
        "job_work_from_home",
        "search_location",
        "job_posted_date",
        "job_no_degree_mention",
        "job_health_insurance",
        "job_country",
        "salary_rate",
        "salary_year_avg",
        "salary_hour_avg",
        "company_name",
        "job_skills",
        "job_type_skills",
    }
    assert expected.issubset(set(df.columns))


# ---------------------------------------------------------------------------
# Boolean casting
# ---------------------------------------------------------------------------


def test_boolean_true_is_cast(sample_csv: str) -> None:
    df = extract(sample_csv)
    # pandas stores booleans as np.True_ — use == not `is`
    assert df["job_work_from_home"].iloc[0] == True  # noqa: E712


def test_boolean_false_is_cast(sample_csv: str) -> None:
    df = extract(sample_csv)
    assert df["job_work_from_home"].iloc[1] == False  # noqa: E712


def test_boolean_empty_becomes_nan(tmp_path: pytest.TempPathFactory) -> None:
    path = tmp_path / "empty_bool.csv"
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "job_title_short",
                "job_title",
                "job_location",
                "job_via",
                "job_schedule_type",
                "job_work_from_home",
                "search_location",
                "job_posted_date",
                "job_no_degree_mention",
                "job_health_insurance",
                "job_country",
                "salary_rate",
                "salary_year_avg",
                "salary_hour_avg",
                "company_name",
                "job_skills",
                "job_type_skills",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                k: ""
                for k in [
                    "job_title_short",
                    "job_title",
                    "job_location",
                    "job_via",
                    "job_schedule_type",
                    "job_work_from_home",
                    "search_location",
                    "job_posted_date",
                    "job_no_degree_mention",
                    "job_health_insurance",
                    "job_country",
                    "salary_rate",
                    "salary_year_avg",
                    "salary_hour_avg",
                    "company_name",
                    "job_skills",
                    "job_type_skills",
                ]
            }
        )

    df = extract(str(path))
    assert pd.isna(df["job_work_from_home"].iloc[0])


# ---------------------------------------------------------------------------
# Date casting
# ---------------------------------------------------------------------------


def test_date_column_is_datetime(sample_csv: str) -> None:
    df = extract(sample_csv)
    assert pd.api.types.is_datetime64_any_dtype(df["job_posted_date"])


def test_date_invalid_becomes_nat(tmp_path: pytest.TempPathFactory) -> None:
    path = tmp_path / "bad_date.csv"
    cols = [
        "job_title_short",
        "job_title",
        "job_location",
        "job_via",
        "job_schedule_type",
        "job_work_from_home",
        "search_location",
        "job_posted_date",
        "job_no_degree_mention",
        "job_health_insurance",
        "job_country",
        "salary_rate",
        "salary_year_avg",
        "salary_hour_avg",
        "company_name",
        "job_skills",
        "job_type_skills",
    ]
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols)
        writer.writeheader()
        row = {k: "" for k in cols}
        row["job_posted_date"] = "not-a-date"
        writer.writerow(row)

    df = extract(str(path))
    assert pd.isna(df["job_posted_date"].iloc[0])


# ---------------------------------------------------------------------------
# Numeric casting
# ---------------------------------------------------------------------------


def test_salary_year_avg_is_numeric(sample_csv: str) -> None:
    df = extract(sample_csv)
    assert df["salary_year_avg"].iloc[0] == 120000.0


def test_salary_year_avg_empty_becomes_nan(sample_csv: str) -> None:
    df = extract(sample_csv)
    assert pd.isna(df["salary_year_avg"].iloc[1])


def test_salary_hour_avg_is_numeric(sample_csv: str) -> None:
    df = extract(sample_csv)
    assert df["salary_hour_avg"].iloc[1] == 25.0


def test_salary_hour_avg_empty_becomes_nan(sample_csv: str) -> None:
    df = extract(sample_csv)
    assert pd.isna(df["salary_hour_avg"].iloc[0])


# ---------------------------------------------------------------------------
# Semi-structured column parsing
# ---------------------------------------------------------------------------


def test_job_skills_parsed_to_list(sample_csv: str) -> None:
    df = extract(sample_csv)
    assert df["job_skills"].iloc[0] == ["python", "sql"]


def test_job_skills_empty_becomes_empty_list(sample_csv: str) -> None:
    df = extract(sample_csv)
    assert df["job_skills"].iloc[1] == []


def test_job_type_skills_parsed_to_dict(sample_csv: str) -> None:
    df = extract(sample_csv)
    assert df["job_type_skills"].iloc[0] == {"programming": ["python", "sql"]}


def test_job_type_skills_empty_becomes_empty_dict(sample_csv: str) -> None:
    df = extract(sample_csv)
    assert df["job_type_skills"].iloc[1] == {}


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_file_not_found_raises() -> None:
    with pytest.raises(FileNotFoundError):
        extract("/nonexistent/path/data_jobs.csv")
