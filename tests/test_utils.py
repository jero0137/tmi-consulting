import time
from unittest.mock import patch

import pytest

from pipeline.utils import parse_job_skills, parse_job_type_skills, retry_with_backoff


# ---------------------------------------------------------------------------
# parse_job_skills
# ---------------------------------------------------------------------------


class TestParseJobSkills:
    def test_valid_list(self):
        assert parse_job_skills("['python', 'sql', 'tableau']") == ["python", "sql", "tableau"]

    def test_single_item(self):
        assert parse_job_skills("['python']") == ["python"]

    def test_empty_list_literal(self):
        assert parse_job_skills("[]") == []

    def test_empty_string(self):
        assert parse_job_skills("") == []

    def test_none_value(self):
        assert parse_job_skills(None) == []  # type: ignore[arg-type]

    def test_whitespace_only(self):
        assert parse_job_skills("   ") == []

    def test_malformed_input(self):
        assert parse_job_skills("not a list at all") == []

    def test_dict_instead_of_list(self):
        # Should return [] when the literal evaluates to a dict, not a list
        assert parse_job_skills("{'key': 'value'}") == []

    def test_leading_trailing_whitespace_is_stripped(self):
        assert parse_job_skills("  ['python', 'sql']  ") == ["python", "sql"]


# ---------------------------------------------------------------------------
# parse_job_type_skills
# ---------------------------------------------------------------------------


class TestParseJobTypeSkills:
    def test_valid_dict(self):
        result = parse_job_type_skills("{'programming': ['python', 'sql']}")
        assert result == {"programming": ["python", "sql"]}

    def test_nested_dict(self):
        raw = "{'analyst_tools': ['tableau'], 'programming': ['python', 'sql', 'nosql']}"
        result = parse_job_type_skills(raw)
        assert result == {"analyst_tools": ["tableau"], "programming": ["python", "sql", "nosql"]}

    def test_empty_dict_literal(self):
        assert parse_job_type_skills("{}") == {}

    def test_empty_string(self):
        assert parse_job_type_skills("") == {}

    def test_none_value(self):
        assert parse_job_type_skills(None) == {}  # type: ignore[arg-type]

    def test_whitespace_only(self):
        assert parse_job_type_skills("   ") == {}

    def test_malformed_input(self):
        assert parse_job_type_skills("not a dict at all") == {}

    def test_list_instead_of_dict(self):
        # Should return {} when the literal evaluates to a list, not a dict
        assert parse_job_type_skills("['python', 'sql']") == {}

    def test_leading_trailing_whitespace_is_stripped(self):
        result = parse_job_type_skills("  {'programming': ['python']}  ")
        assert result == {"programming": ["python"]}


# ---------------------------------------------------------------------------
# retry_with_backoff
# ---------------------------------------------------------------------------


class TestRetryWithBackoff:
    def test_returns_value_on_first_success(self):
        @retry_with_backoff(max_retries=3, base_delay=0.01)
        def func() -> str:
            return "ok"

        assert func() == "ok"

    def test_succeeds_after_retries(self):
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01, exceptions=(ValueError,))
        def func() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("transient")
            return "ok"

        with patch("time.sleep"):
            result = func()

        assert result == "ok"
        assert call_count == 3

    def test_raises_after_all_retries_exhausted(self):
        @retry_with_backoff(max_retries=3, base_delay=0.01, exceptions=(ValueError,))
        def func() -> None:
            raise ValueError("always fails")

        with patch("time.sleep"), pytest.raises(ValueError, match="always fails"):
            func()

    def test_does_not_retry_on_unexpected_exception(self):
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01, exceptions=(ValueError,))
        def func() -> None:
            nonlocal call_count
            call_count += 1
            raise TypeError("unexpected")

        with pytest.raises(TypeError):
            func()

        assert call_count == 1

    def test_backoff_delay_grows_exponentially(self):
        sleep_calls: list[float] = []

        @retry_with_backoff(max_retries=4, base_delay=1.0, exceptions=(RuntimeError,))
        def func() -> None:
            raise RuntimeError("fail")

        with patch("time.sleep", side_effect=lambda s: sleep_calls.append(s)):
            with pytest.raises(RuntimeError):
                func()

        # delays should be 1.0, 2.0, 4.0 (3 sleeps for 4 attempts)
        assert sleep_calls == [1.0, 2.0, 4.0]

    def test_backoff_delay_is_capped_at_max_delay(self):
        sleep_calls: list[float] = []

        @retry_with_backoff(max_retries=4, base_delay=10.0, max_delay=15.0, exceptions=(RuntimeError,))
        def func() -> None:
            raise RuntimeError("fail")

        with patch("time.sleep", side_effect=lambda s: sleep_calls.append(s)):
            with pytest.raises(RuntimeError):
                func()

        assert all(s <= 15.0 for s in sleep_calls)

    def test_preserves_function_name_and_docstring(self):
        @retry_with_backoff(max_retries=2, base_delay=0.01)
        def my_function() -> None:
            """My docstring."""

        assert my_function.__name__ == "my_function"
        assert my_function.__doc__ == "My docstring."
