import ast
import functools
import logging
import time
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def parse_job_skills(value: str) -> list[str]:
    """Parse the job_skills CSV column from its string representation.

    Expects values like "['python', 'sql', 'tableau']".
    Returns an empty list for null, empty, or malformed input.
    """
    if not value or not isinstance(value, str) or not value.strip():
        return []
    try:
        result = ast.literal_eval(value.strip())
        return result if isinstance(result, list) else []
    except (ValueError, SyntaxError):
        logger.warning("Could not parse job_skills: %r", value)
        return []


def parse_job_type_skills(value: str) -> dict:
    """Parse the job_type_skills CSV column from its string representation.

    Expects values like "{'programming': ['python', 'sql']}".
    Returns an empty dict for null, empty, or malformed input.
    """
    if not value or not isinstance(value, str) or not value.strip():
        return {}
    try:
        result = ast.literal_eval(value.strip())
        return result if isinstance(result, dict) else {}
    except (ValueError, SyntaxError):
        logger.warning("Could not parse job_type_skills: %r", value)
        return {}


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[F], F]:
    """Decorator that retries a function with exponential backoff on failure.

    Waits base_delay * 2^(attempt-1) seconds between attempts, capped at max_delay.
    Re-raises the last exception after all retries are exhausted.
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            _logger = logging.getLogger(func.__module__)
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt == max_retries:
                        _logger.error(
                            "All %d attempts failed for '%s': %s",
                            max_retries, func.__name__, exc,
                        )
                        raise
                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    _logger.warning(
                        "Attempt %d/%d for '%s' failed: %s — retrying in %.1fs",
                        attempt, max_retries, func.__name__, exc, delay,
                    )
                    time.sleep(delay)
        return wrapper  # type: ignore[return-value]
    return decorator
