"""Cleansing rules applied when moving rows from raw.data_jobs to staging.stg_job_postings.

Each rule is a small, pure function so it can be unit-tested in isolation.
The transform module orchestrates them and handles batching / DB I/O.
"""

import re
import unicodedata
from typing import Optional, Tuple


def _ascii_fold(text: str) -> str:
    """Strip accents/diacritics for case- and accent-insensitive comparison."""
    return "".join(
        c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c)
    ).lower()


# Salary-range pattern used to detect company_name values that are actually
# salary text like "$150K – $199.5K" or "$176K – $234K". Matches "$"-prefixed
# numbers (with optional decimals and K/M suffix) on both sides of a dash.
_SALARY_RANGE_RE = re.compile(
    r"^\$\s*\d+(?:\.\d+)?\s*[KkMm]?\s*[\-–—]\s*\$?\s*\d+(?:\.\d+)?\s*[KkMm]?$"
)
_LEADING_HASH_RE = re.compile(r"^#+")
_EDGE_QUOTES_RE = re.compile(r'^"+|"+$')

# Numeric prefixes that are internal codes, not part of the company name.
# (0110) IBM  →  strip the parenthesised code.
_LEADING_PARENS_CODE_RE = re.compile(r"^\(\d+\)\s+")
# 00002 Citibank  →  strip leading-zero numbers (clearly not part of the name).
# 12542 Citicorp  →  strip 5-or-more-digit numbers (internal entity IDs).
# Does NOT strip "1872 Consulting", "24 Seven Talent", etc. (≤4 digits, no leading zero).
_LEADING_NUM_CODE_RE = re.compile(r"^(?:0\d+|\d{5,})\s+")
# Leading dashes (with optional surrounding spaces): "-  - Si-Ware"  →  "Si-Ware".
_LEADING_DASHES_RE = re.compile(r"^[-\s]+")

# Strings that look like data artifacts rather than company names.
# "5 reviews", "20 reviews"  →  "Not Identified".
_REVIEWS_RE = re.compile(r"^\d+\s+reviews?$", re.IGNORECASE)
# Pure digit strings of any length: "3677", "99"  →  "Not Identified".
_PURE_DIGITS_RE = re.compile(r"^\d+$")
# Digit string + 1–2 letters that is long enough to be a code: "201000200M".
# Keeps short names like "3M" (len 2) and "24S" (len 3) which are real companies.
_DIGIT_CODE_RE = re.compile(r"^\d+[A-Za-z]{1,2}$")

# Unicode-script ranges for alphabet-based language detection (Rule 4, Step 1).
_CYRILLIC_RE = re.compile(r"[Ѐ-ӿ]")
_CHINESE_RE = re.compile(r"[一-鿿]")
_JAPANESE_RE = re.compile(r"[぀-ヿ]")
_KOREAN_RE = re.compile(r"[가-힯]")
_ARABIC_RE = re.compile(r"[؀-ۿ]")

_REMOTE_TOKENS: set[str] = {"anywhere", "remote", "remoto"}

_LINGUA_DETECTOR = None


def normalize_string(value: Optional[str]) -> Optional[str]:
    """Strip whitespace and convert empty strings to None. Non-strings pass through."""
    if not isinstance(value, str):
        return value if value is not None else None
    stripped = value.strip()
    return stripped or None


def clean_company_name(name: Optional[str]) -> Optional[str]:
    """Apply Rule 1 to company_name in order.

    1.  Strip leading '#' characters.
    2.  Return None if the value is a salary range like "$150K – $199.5K".
    3.  Strip surrounding double quotes.
    4.  strip() whitespace.
    5.  Strip parenthesised numeric code prefix: "(0110) IBM" → "IBM".
    6.  Strip leading-zero or 5+-digit numeric prefix: "027 Parks" → "Parks",
        "12542 Citicorp" → "Citicorp".  Does NOT strip "1872 Consulting" etc.
    7.  Strip leading dashes (and surrounding spaces): "- - Si-Ware" → "Si-Ware".
    8.  strip() whitespace again.
    9.  "Not Identified" if the remaining string is not a real company name:
        • matches "N reviews" (e.g. "5 reviews")
        • is a pure digit string (e.g. "3677", "99")
        • is a long digit+letter code (e.g. "201000200M", len ≥ 5)
    10. Empty string → None.
    """
    if not isinstance(name, str):
        return None

    # 1. strip leading '#'
    cleaned = _LEADING_HASH_RE.sub("", name)

    # 2. salary range → None
    if _SALARY_RANGE_RE.match(cleaned.strip()):
        return None

    # 3–4. strip surrounding quotes and whitespace
    cleaned = _EDGE_QUOTES_RE.sub("", cleaned).strip()

    # 5. strip "(NNN) " prefix
    cleaned = _LEADING_PARENS_CODE_RE.sub("", cleaned).strip()

    # 6. strip leading-zero or long numeric prefix
    cleaned = _LEADING_NUM_CODE_RE.sub("", cleaned).strip()

    # 7–8. strip leading dashes and re-strip whitespace
    cleaned = _LEADING_DASHES_RE.sub("", cleaned).strip()

    if not cleaned:
        return None

    # 9. detect non-company strings
    if _REVIEWS_RE.match(cleaned):
        return "Not Identified"
    if _PURE_DIGITS_RE.match(cleaned):
        return "Not Identified"
    if _DIGIT_CODE_RE.match(cleaned) and len(cleaned) >= 5:
        return "Not Identified"

    return cleaned


def parse_job_location(
    value: Optional[str],
) -> Tuple[Optional[str], Optional[str], Optional[str], bool, Optional[str]]:
    """Rule 2: parse a raw job_location string into derived columns.

    Returns a 5-tuple (city, state, country, is_remote, format) where format is
    one of "remote", "country_only", "city_country", "city_state_country" or None.
    The 3-part case ignores a state that duplicates the city (e.g. "Bogota, Bogota, Colombia").
    """
    if not isinstance(value, str):
        return None, None, None, False, None

    trimmed = value.strip()
    if not trimmed:
        return None, None, None, False, None

    if trimmed.lower() in _REMOTE_TOKENS:
        return None, None, None, True, "remote"

    parts = [p.strip() for p in trimmed.split(",")]
    parts = [p for p in parts if p]

    if not parts:
        return None, None, None, False, None
    if len(parts) == 1:
        return None, None, parts[0], False, "country_only"
    if len(parts) == 2:
        return parts[0], None, parts[1], False, "city_country"

    city = parts[0]
    state = parts[1] if _ascii_fold(parts[0]) != _ascii_fold(parts[1]) else None
    country = parts[-1]
    return city, state, country, False, "city_state_country"


def normalize_schedule_type(value: Optional[str]) -> Optional[str]:
    """Rule 3: lowercase + strip, preserving combined types like 'Full-time and Internship'."""
    if not isinstance(value, str):
        return None
    cleaned = value.strip().lower()
    return cleaned or None


def reconcile_country(
    job_country: Optional[str], search_location: Optional[str]
) -> Optional[str]:
    """Rule 5: prefer job_country (system-generated); fall back to search_location."""
    jc = normalize_string(job_country)
    if jc:
        return jc
    return normalize_string(search_location)


def _get_lingua_detector():
    """Build the lingua detector lazily; loading all models is expensive."""
    global _LINGUA_DETECTOR
    if _LINGUA_DETECTOR is None:
        from lingua import LanguageDetectorBuilder

        _LINGUA_DETECTOR = (
            LanguageDetectorBuilder.from_all_languages()
            .with_preloaded_language_models()
            .build()
        )
    return _LINGUA_DETECTOR


def _lingua_top_confidence(text: str) -> Tuple[str, float]:
    """Run lingua on a single string and return (iso_code, confidence)."""
    detector = _get_lingua_detector()
    try:
        results = detector.compute_language_confidence_values(text)
    except Exception:
        return "unknown", 0.0
    if not results:
        return "unknown", 0.0
    top = results[0]
    return top.language.iso_code_639_1.name.lower(), float(top.value)


def detect_language(text: Optional[str]) -> Tuple[str, float]:
    """Rule 4: two-step language detection.

    Step 1 — Unicode-script check. If the string contains characters from one
    of the configured non-Latin ranges, return that script with confidence 1.0.

    Step 2 — Latin alphabet. Delegate to lingua-language-detector and return
    the top match. Strings shorter than 3 characters are reported as 'unknown'
    with confidence 0.0 since lingua is unreliable on very short input.
    """
    if not isinstance(text, str):
        return "unknown", 0.0

    stripped = text.strip()
    if len(stripped) < 3:
        return "unknown", 0.0

    if _CYRILLIC_RE.search(stripped):
        return "cyrillic", 1.0
    if _CHINESE_RE.search(stripped):
        return "zh", 1.0
    if _JAPANESE_RE.search(stripped):
        return "ja", 1.0
    if _KOREAN_RE.search(stripped):
        return "ko", 1.0
    if _ARABIC_RE.search(stripped):
        return "ar", 1.0

    return _lingua_top_confidence(stripped)
