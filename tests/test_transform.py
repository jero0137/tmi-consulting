"""Unit tests for pipeline/transform.py.

Database and lingua calls are always mocked — these are pure unit tests, not
integration tests. The real DB interaction is covered by running the pipeline
manually or in an Airflow DAG end-to-end test.
"""

import math
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import numpy as np
import pandas as pd
import pytest
from psycopg2.extras import Json

from pipeline.transform import (
    _build_language_cache,
    _na_to_none,
    _row_to_tuple,
    create_staging_table,
    transform_and_load_staging,
)

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

CLEANED_AT = datetime(2026, 1, 1, 0, 0, 0)


def _make_row(**kwargs) -> SimpleNamespace:
    """Build a minimal raw-row namespace with sensible defaults."""
    defaults = dict(
        id=1,
        job_title_short="Data Engineer",
        job_title="Senior Data Engineer",
        job_location="Paris, France",
        job_via="via LinkedIn",
        job_schedule_type="Full-time",
        job_work_from_home=False,
        search_location="France",
        job_posted_date=pd.Timestamp("2024-03-15"),
        job_no_degree_mention=False,
        job_health_insurance=True,
        job_country="France",
        salary_rate="year",
        salary_year_avg=90000.0,
        salary_hour_avg=float("nan"),
        company_name="Acme Corp",
        job_skills=["python", "sql"],
        job_type_skills={"programming": ["python", "sql"]},
        loaded_at=pd.Timestamp("2024-03-15 12:00:00"),
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _make_mock_lingua_result(lang_name: str, confidence: float):
    """Build a mock lingua ConfidenceValue-like object."""
    iso_mock = MagicMock()
    iso_mock.name = lang_name.upper()
    lang_mock = MagicMock()
    lang_mock.iso_code_639_1 = iso_mock
    cv = MagicMock()
    cv.language = lang_mock
    cv.value = confidence
    return cv


# ---------------------------------------------------------------------------
# _na_to_none
# ---------------------------------------------------------------------------


class TestNaToNone:
    def test_none_returns_none(self):
        assert _na_to_none(None) is None

    def test_float_nan_returns_none(self):
        assert _na_to_none(float("nan")) is None

    def test_numpy_nan_returns_none(self):
        assert _na_to_none(np.nan) is None

    def test_pandas_nat_returns_none(self):
        assert _na_to_none(pd.NaT) is None

    def test_string_is_preserved(self):
        assert _na_to_none("hello") == "hello"

    def test_integer_is_preserved(self):
        assert _na_to_none(42) == 42

    def test_float_value_is_preserved(self):
        assert _na_to_none(3.14) == 3.14

    def test_list_is_preserved(self):
        # pd.isna raises TypeError on a list; _na_to_none must not crash
        val = [1, 2, 3]
        assert _na_to_none(val) is val

    def test_dict_is_preserved(self):
        val = {"key": "value"}
        assert _na_to_none(val) is val

    def test_boolean_false_preserved(self):
        assert _na_to_none(False) is False

    def test_boolean_true_preserved(self):
        assert _na_to_none(True) is True

    def test_zero_integer_preserved(self):
        assert _na_to_none(0) == 0


# ---------------------------------------------------------------------------
# _build_language_cache
# ---------------------------------------------------------------------------


class TestBuildLanguageCache:
    def test_empty_list_returns_empty_cache(self):
        cache = _build_language_cache([])
        assert cache == {}

    def test_non_string_inputs_skipped(self):
        cache = _build_language_cache([None, 42, 3.14])  # type: ignore[list-item]
        assert cache == {}

    def test_short_title_marked_unknown(self):
        cache = _build_language_cache(["ab"])
        assert cache["ab"] == ("unknown", 0.0)

    def test_cyrillic_detected_without_lingua(self):
        cache = _build_language_cache(["Аналитик данных"])
        assert cache["Аналитик данных"] == ("cyrillic", 1.0)

    def test_chinese_detected_without_lingua(self):
        cache = _build_language_cache(["数据工程师"])
        assert cache["数据工程师"] == ("zh", 1.0)

    def test_japanese_detected_without_lingua(self):
        cache = _build_language_cache(["データエンジニア"])
        assert cache["データエンジニア"] == ("ja", 1.0)

    def test_korean_detected_without_lingua(self):
        cache = _build_language_cache(["데이터 엔지니어"])
        assert cache["데이터 엔지니어"] == ("ko", 1.0)

    def test_arabic_detected_without_lingua(self):
        cache = _build_language_cache(["مهندس بيانات"])
        assert cache["مهندس بيانات"] == ("ar", 1.0)

    def test_latin_title_goes_to_lingua_batch(self):
        cv = _make_mock_lingua_result("en", 0.97)
        detector = MagicMock()
        detector.compute_language_confidence_values_in_parallel.return_value = [[cv]]

        with patch("pipeline.transform._get_lingua_detector", return_value=detector):
            cache = _build_language_cache(["Data Engineer"])

        assert cache["Data Engineer"] == ("en", 0.97)
        detector.compute_language_confidence_values_in_parallel.assert_called_once_with(
            ["Data Engineer"]
        )

    def test_empty_lingua_result_marked_unknown(self):
        detector = MagicMock()
        detector.compute_language_confidence_values_in_parallel.return_value = [[]]

        with patch("pipeline.transform._get_lingua_detector", return_value=detector):
            cache = _build_language_cache(["Some text"])

        assert cache["Some text"] == ("unknown", 0.0)

    def test_mixed_scripts_only_latin_sent_to_lingua(self):
        cv = _make_mock_lingua_result("de", 0.91)
        detector = MagicMock()
        detector.compute_language_confidence_values_in_parallel.return_value = [[cv]]

        titles = ["Аналитик данных", "Dateningenieur"]
        with patch("pipeline.transform._get_lingua_detector", return_value=detector):
            cache = _build_language_cache(titles)

        assert cache["Аналитик данных"] == ("cyrillic", 1.0)
        assert cache["Dateningenieur"] == ("de", 0.91)
        # Only the Latin title was sent to lingua
        detector.compute_language_confidence_values_in_parallel.assert_called_once_with(
            ["Dateningenieur"]
        )

    def test_no_lingua_call_when_all_non_latin(self):
        detector = MagicMock()
        with patch("pipeline.transform._get_lingua_detector", return_value=detector):
            cache = _build_language_cache(["数据工程师", "Аналитик"])

        detector.compute_language_confidence_values_in_parallel.assert_not_called()
        assert cache["数据工程师"] == ("zh", 1.0)


# ---------------------------------------------------------------------------
# create_staging_table
# ---------------------------------------------------------------------------


class TestCreateStagingTable:
    def test_executes_ddl_and_commits(self, tmp_path):
        ddl_content = "CREATE SCHEMA IF NOT EXISTS staging;"
        ddl_file = tmp_path / "staging_schema.sql"
        ddl_file.write_text(ddl_content)

        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("pipeline.transform._DDL_FILE", ddl_file):
            create_staging_table(mock_conn)

        mock_cur.execute.assert_called_once_with(ddl_content)
        mock_conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# _row_to_tuple
# ---------------------------------------------------------------------------


class TestRowToTuple:
    def test_tuple_length(self):
        row = _make_row()
        cache = {"Senior Data Engineer": ("en", 0.99)}
        result = _row_to_tuple(row, cache, CLEANED_AT)
        assert len(result) == 28

    def test_id_is_int(self):
        row = _make_row(id=7)
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[0] == 7
        assert isinstance(result[0], int)

    def test_job_title_cleaned_and_lang_resolved(self):
        row = _make_row(job_title="Senior Data Engineer")
        cache = {"Senior Data Engineer": ("en", 0.99)}
        result = _row_to_tuple(row, cache, CLEANED_AT)
        assert result[2] == "Senior Data Engineer"  # job_title
        assert result[3] == "en"                    # job_title_lang
        assert result[4] == 0.99                    # job_title_lang_confidence

    def test_null_job_title_gives_null_lang(self):
        row = _make_row(job_title=float("nan"))
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[2] is None   # job_title
        assert result[3] is None   # job_title_lang
        assert result[4] is None   # job_title_lang_confidence

    def test_job_title_not_in_cache_returns_unknown(self):
        row = _make_row(job_title="Uncached Title Here")
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[3] == "unknown"
        assert result[4] == 0.0

    def test_location_parsed_correctly(self):
        row = _make_row(job_location="Karachi, Pakistan")
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[5] == "Karachi, Pakistan"  # raw
        assert result[6] == "Karachi"            # city
        assert result[7] is None                 # state
        assert result[8] == "Pakistan"           # country
        assert result[9] is False                # is_remote
        assert result[10] == "city_country"      # format

    def test_anywhere_location_sets_remote_flag(self):
        row = _make_row(job_location="Anywhere")
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[5] == "Anywhere"   # original preserved
        assert result[6] is None         # city
        assert result[9] is True         # is_remote
        assert result[10] == "remote"    # format

    def test_schedule_type_lowercased(self):
        row = _make_row(job_schedule_type="Full-time and Internship")
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[12] == "full-time and internship"

    def test_company_name_hash_stripped(self):
        row = _make_row(company_name="#TeamGoHealth")
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[23] == "TeamGoHealth"

    def test_salary_range_company_becomes_none(self):
        row = _make_row(company_name="$150K – $199.5K")
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[23] is None

    def test_nan_salary_year_becomes_none(self):
        row = _make_row(salary_year_avg=float("nan"))
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[21] is None

    def test_nan_salary_hour_becomes_none(self):
        row = _make_row(salary_hour_avg=float("nan"))
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[22] is None

    def test_valid_salary_preserved_as_float(self):
        row = _make_row(salary_year_avg=90000.0, salary_hour_avg=45.5)
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[21] == 90000.0
        assert result[22] == 45.5

    def test_empty_job_skills_becomes_none(self):
        row = _make_row(job_skills=[])
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[24] is None

    def test_nan_job_skills_becomes_none(self):
        row = _make_row(job_skills=float("nan"))
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[24] is None

    def test_valid_job_skills_preserved(self):
        row = _make_row(job_skills=["python", "sql"])
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[24] == ["python", "sql"]

    def test_job_type_skills_wrapped_in_json(self):
        payload = {"programming": ["python"]}
        row = _make_row(job_type_skills=payload)
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert isinstance(result[25], Json)
        assert result[25].adapted == payload

    def test_null_job_type_skills_is_none(self):
        row = _make_row(job_type_skills=None)
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[25] is None

    def test_pandas_timestamp_converted_to_datetime(self):
        ts = pd.Timestamp("2024-03-15 09:30:00")
        row = _make_row(job_posted_date=ts, loaded_at=ts)
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert isinstance(result[16], datetime)  # job_posted_date
        assert isinstance(result[26], datetime)  # loaded_at

    def test_country_final_reconciled(self):
        row = _make_row(job_country="France", search_location="France, Paris")
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[15] == "France"  # job_country wins

    def test_country_final_falls_back_to_search_location(self):
        row = _make_row(job_country=float("nan"), search_location="Germany")
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[15] == "Germany"

    def test_cleaned_at_is_last_element(self):
        row = _make_row()
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[27] == CLEANED_AT

    def test_whitespace_job_title_short_stripped(self):
        row = _make_row(job_title_short="  Data Engineer  ")
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[1] == "Data Engineer"

    def test_empty_job_via_becomes_none(self):
        row = _make_row(job_via="")
        result = _row_to_tuple(row, {}, CLEANED_AT)
        assert result[11] is None


# ---------------------------------------------------------------------------
# transform_and_load_staging (integration of all pieces, DB mocked)
# ---------------------------------------------------------------------------


class TestTransformAndLoadStaging:
    def _make_test_df(self):
        return pd.DataFrame(
            [
                {
                    "id": 1,
                    "job_title_short": "Data Engineer",
                    "job_title": "Senior Data Engineer",
                    "job_location": "Paris, France",
                    "job_via": "via LinkedIn",
                    "job_schedule_type": "Full-time",
                    "job_work_from_home": False,
                    "search_location": "France",
                    "job_posted_date": pd.Timestamp("2024-03-15"),
                    "job_no_degree_mention": False,
                    "job_health_insurance": True,
                    "job_country": "France",
                    "salary_rate": "year",
                    "salary_year_avg": 90000.0,
                    "salary_hour_avg": float("nan"),
                    "company_name": "Acme Corp",
                    "job_skills": ["python", "sql"],
                    "job_type_skills": {"programming": ["python"]},
                    "loaded_at": pd.Timestamp("2024-03-15 12:00:00"),
                },
                {
                    "id": 2,
                    "job_title_short": "Data Scientist",
                    "job_title": "Data Scientist",
                    "job_location": "Anywhere",
                    "job_via": "via Indeed",
                    "job_schedule_type": "Full-time and Part-time",
                    "job_work_from_home": True,
                    "search_location": "United States",
                    "job_posted_date": pd.Timestamp("2024-03-16"),
                    "job_no_degree_mention": True,
                    "job_health_insurance": False,
                    "job_country": "United States",
                    "salary_rate": float("nan"),
                    "salary_year_avg": float("nan"),
                    "salary_hour_avg": float("nan"),
                    "company_name": "#TeamGoHealth",
                    "job_skills": [],
                    "job_type_skills": None,
                    "loaded_at": pd.Timestamp("2024-03-16 08:00:00"),
                },
            ]
        )

    def test_truncate_and_insert_called(self):
        df = self._make_test_df()
        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        cv = _make_mock_lingua_result("en", 0.97)
        detector = MagicMock()
        detector.compute_language_confidence_values_in_parallel.return_value = [[cv], [cv]]

        with (
            patch("pipeline.transform.pd.read_sql", return_value=df),
            patch("pipeline.transform._get_lingua_detector", return_value=detector),
            patch("pipeline.transform.create_staging_table"),
            patch("pipeline.transform.execute_values") as mock_ev,
        ):
            transform_and_load_staging(mock_conn)

        mock_cur.execute.assert_called_once_with("TRUNCATE staging.stg_job_postings;")
        mock_ev.assert_called_once()
        mock_conn.commit.assert_called_once()

    def test_correct_row_count_inserted(self):
        df = self._make_test_df()
        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        captured_rows = []

        def capture_execute_values(cur, sql, rows, page_size=None):
            captured_rows.extend(rows)

        cv = _make_mock_lingua_result("en", 0.97)
        detector = MagicMock()
        detector.compute_language_confidence_values_in_parallel.return_value = [[cv], [cv]]

        with (
            patch("pipeline.transform.pd.read_sql", return_value=df),
            patch("pipeline.transform._get_lingua_detector", return_value=detector),
            patch("pipeline.transform.create_staging_table"),
            patch("pipeline.transform.execute_values", side_effect=capture_execute_values),
        ):
            transform_and_load_staging(mock_conn)

        assert len(captured_rows) == 2

    def test_anywhere_row_has_remote_flag(self):
        df = self._make_test_df()
        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        captured_rows = []

        def capture_execute_values(cur, sql, rows, page_size=None):
            captured_rows.extend(rows)

        cv = _make_mock_lingua_result("en", 0.97)
        detector = MagicMock()
        detector.compute_language_confidence_values_in_parallel.return_value = [[cv], [cv]]

        with (
            patch("pipeline.transform.pd.read_sql", return_value=df),
            patch("pipeline.transform._get_lingua_detector", return_value=detector),
            patch("pipeline.transform.create_staging_table"),
            patch("pipeline.transform.execute_values", side_effect=capture_execute_values),
        ):
            transform_and_load_staging(mock_conn)

        # row[1] is id=2 (Anywhere row); is_remote is index 9
        anywhere_row = captured_rows[1]
        assert anywhere_row[9] is True   # location_is_remote
        assert anywhere_row[10] == "remote"

    def test_hash_company_cleaned_in_output(self):
        df = self._make_test_df()
        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cur)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        captured_rows = []

        def capture_execute_values(cur, sql, rows, page_size=None):
            captured_rows.extend(rows)

        cv = _make_mock_lingua_result("en", 0.97)
        detector = MagicMock()
        detector.compute_language_confidence_values_in_parallel.return_value = [[cv], [cv]]

        with (
            patch("pipeline.transform.pd.read_sql", return_value=df),
            patch("pipeline.transform._get_lingua_detector", return_value=detector),
            patch("pipeline.transform.create_staging_table"),
            patch("pipeline.transform.execute_values", side_effect=capture_execute_values),
        ):
            transform_and_load_staging(mock_conn)

        # row[1] is id=2 (#TeamGoHealth row); company_name is index 23
        assert captured_rows[1][23] == "TeamGoHealth"
