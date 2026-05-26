from unittest.mock import MagicMock, patch

import great_expectations as gx
import pandas as pd
import pytest

from pipeline.quality import DataQualityError, _build_suite, validate_staging


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _make_valid_df(n: int = 2) -> pd.DataFrame:
    """Return a minimal DataFrame that satisfies every expectation in _build_suite."""
    return pd.DataFrame(
        {
            "id": list(range(1, n + 1)),
            "cleaned_at": [pd.Timestamp("2026-01-01")] * n,
            "location_is_remote": [False] * n,
            "job_title_lang_confidence": [0.95] * n,
            "location_format": ["city_country"] * n,
            "salary_year_avg": [100_000.0] * n,
            "salary_hour_avg": [None] * n,
        }
    )


# ---------------------------------------------------------------------------
# _build_suite
# ---------------------------------------------------------------------------


class TestBuildSuite:
    def test_returns_expectation_suite(self):
        assert isinstance(_build_suite(), gx.ExpectationSuite)

    def test_suite_name(self):
        assert _build_suite().name == "stg_job_postings"

    def test_expectation_count(self):
        assert len(_build_suite().expectations) == 9

    def test_checks_id_not_null(self):
        exps = _build_suite().expectations
        assert any(isinstance(e, gx.expectations.ExpectColumnValuesToNotBeNull) for e in exps)

    def test_checks_id_unique(self):
        exps = _build_suite().expectations
        assert any(isinstance(e, gx.expectations.ExpectColumnValuesToBeUnique) for e in exps)

    def test_checks_row_count(self):
        exps = _build_suite().expectations
        assert any(isinstance(e, gx.expectations.ExpectTableRowCountToBeBetween) for e in exps)

    def test_checks_confidence_between(self):
        exps = _build_suite().expectations
        assert any(isinstance(e, gx.expectations.ExpectColumnValuesToBeBetween) for e in exps)

    def test_checks_location_format_in_set(self):
        exps = _build_suite().expectations
        assert any(isinstance(e, gx.expectations.ExpectColumnValuesToBeInSet) for e in exps)


# ---------------------------------------------------------------------------
# validate_staging — happy path
# ---------------------------------------------------------------------------


class TestValidateStagingPasses:
    @patch("pipeline.quality.pd.read_sql")
    def test_valid_data_does_not_raise(self, mock_read):
        mock_read.return_value = _make_valid_df()
        validate_staging(MagicMock())  # must not raise

    @patch("pipeline.quality.pd.read_sql")
    def test_null_location_format_allowed(self, mock_read):
        # parse_job_location returns format=None for unparseable input
        df = _make_valid_df(1)
        df.loc[0, "location_format"] = None
        mock_read.return_value = df
        validate_staging(MagicMock())

    @patch("pipeline.quality.pd.read_sql")
    def test_null_confidence_allowed(self, mock_read):
        # confidence is None when job_title is None
        df = _make_valid_df(1)
        df.loc[0, "job_title_lang_confidence"] = None
        mock_read.return_value = df
        validate_staging(MagicMock())

    @patch("pipeline.quality.pd.read_sql")
    def test_null_salary_allowed(self, mock_read):
        # most rows have no salary data
        df = _make_valid_df(1)
        df.loc[0, "salary_year_avg"] = None
        mock_read.return_value = df
        validate_staging(MagicMock())

    @patch("pipeline.quality.pd.read_sql")
    def test_all_valid_location_formats_accepted(self, mock_read):
        formats = ["remote", "country_only", "city_country", "city_state_country"]
        df = _make_valid_df(len(formats))
        df["location_format"] = formats
        mock_read.return_value = df
        validate_staging(MagicMock())


# ---------------------------------------------------------------------------
# validate_staging — failure cases
# ---------------------------------------------------------------------------


class TestValidateStagingFails:
    @patch("pipeline.quality.pd.read_sql")
    def test_null_id_raises(self, mock_read):
        df = _make_valid_df(2)
        df.loc[0, "id"] = None
        mock_read.return_value = df
        with pytest.raises(DataQualityError):
            validate_staging(MagicMock())

    @patch("pipeline.quality.pd.read_sql")
    def test_duplicate_id_raises(self, mock_read):
        df = _make_valid_df(2)
        df.loc[1, "id"] = 1  # collision with row 0
        mock_read.return_value = df
        with pytest.raises(DataQualityError):
            validate_staging(MagicMock())

    @patch("pipeline.quality.pd.read_sql")
    def test_null_cleaned_at_raises(self, mock_read):
        df = _make_valid_df(1)
        df["cleaned_at"] = df["cleaned_at"].astype(object)
        df.loc[0, "cleaned_at"] = None
        mock_read.return_value = df
        with pytest.raises(DataQualityError):
            validate_staging(MagicMock())

    @patch("pipeline.quality.pd.read_sql")
    def test_null_location_is_remote_raises(self, mock_read):
        df = _make_valid_df(1)
        df["location_is_remote"] = df["location_is_remote"].astype(object)
        df.loc[0, "location_is_remote"] = None
        mock_read.return_value = df
        with pytest.raises(DataQualityError):
            validate_staging(MagicMock())

    @patch("pipeline.quality.pd.read_sql")
    def test_confidence_above_one_raises(self, mock_read):
        df = _make_valid_df(1)
        df.loc[0, "job_title_lang_confidence"] = 1.5
        mock_read.return_value = df
        with pytest.raises(DataQualityError):
            validate_staging(MagicMock())

    @patch("pipeline.quality.pd.read_sql")
    def test_confidence_below_zero_raises(self, mock_read):
        df = _make_valid_df(1)
        df.loc[0, "job_title_lang_confidence"] = -0.1
        mock_read.return_value = df
        with pytest.raises(DataQualityError):
            validate_staging(MagicMock())

    @patch("pipeline.quality.pd.read_sql")
    def test_invalid_location_format_raises(self, mock_read):
        df = _make_valid_df(1)
        df.loc[0, "location_format"] = "unknown_format"
        mock_read.return_value = df
        with pytest.raises(DataQualityError):
            validate_staging(MagicMock())

    @patch("pipeline.quality.pd.read_sql")
    def test_negative_salary_year_raises(self, mock_read):
        df = _make_valid_df(1)
        df.loc[0, "salary_year_avg"] = -1_000.0
        mock_read.return_value = df
        with pytest.raises(DataQualityError):
            validate_staging(MagicMock())

    @patch("pipeline.quality.pd.read_sql")
    def test_negative_salary_hour_raises(self, mock_read):
        df = _make_valid_df(1)
        df["salary_hour_avg"] = df["salary_hour_avg"].astype(float)
        df.loc[0, "salary_hour_avg"] = -5.0
        mock_read.return_value = df
        with pytest.raises(DataQualityError):
            validate_staging(MagicMock())

    @patch("pipeline.quality.pd.read_sql")
    def test_empty_table_raises(self, mock_read):
        mock_read.return_value = pd.DataFrame(
            {
                "id": pd.Series([], dtype="Int64"),
                "cleaned_at": pd.Series([], dtype="datetime64[ns]"),
                "location_is_remote": pd.Series([], dtype=bool),
                "job_title_lang_confidence": pd.Series([], dtype=float),
                "location_format": pd.Series([], dtype=object),
                "salary_year_avg": pd.Series([], dtype=float),
                "salary_hour_avg": pd.Series([], dtype=float),
            }
        )
        with pytest.raises(DataQualityError):
            validate_staging(MagicMock())

    @patch("pipeline.quality.pd.read_sql")
    def test_error_message_mentions_failed_count(self, mock_read):
        df = _make_valid_df(2)
        df.loc[0, "id"] = None
        df.loc[1, "id"] = 1  # null + duplicate — two failures on id
        mock_read.return_value = df
        with pytest.raises(DataQualityError, match=r"\d+ expectation"):
            validate_staging(MagicMock())
