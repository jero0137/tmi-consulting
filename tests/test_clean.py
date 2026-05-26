from unittest.mock import patch


from pipeline.clean import (
    clean_company_name,
    detect_language,
    normalize_schedule_type,
    normalize_string,
    parse_job_location,
    reconcile_country,
)


# ---------------------------------------------------------------------------
# normalize_string
# ---------------------------------------------------------------------------


class TestNormalizeString:
    def test_strips_leading_trailing_whitespace(self):
        assert normalize_string("  hello  ") == "hello"

    def test_empty_string_returns_none(self):
        assert normalize_string("") is None

    def test_whitespace_only_returns_none(self):
        assert normalize_string("   ") is None

    def test_none_passthrough(self):
        assert normalize_string(None) is None

    def test_non_string_int_passthrough(self):
        assert normalize_string(42) == 42  # type: ignore[arg-type]

    def test_already_clean_string_unchanged(self):
        assert normalize_string("Data Engineer") == "Data Engineer"


# ---------------------------------------------------------------------------
# clean_company_name
# ---------------------------------------------------------------------------


class TestCleanCompanyName:
    def test_strips_leading_hash(self):
        assert clean_company_name("#TeamGoHealth") == "TeamGoHealth"

    def test_strips_multiple_leading_hashes(self):
        assert clean_company_name("##OpenToWork Recruiting") == "OpenToWork Recruiting"

    def test_hash_mid_string_preserved(self):
        assert clean_company_name("Acme #1 Corp") == "Acme #1 Corp"

    def test_salary_range_em_dash_returns_none(self):
        assert clean_company_name("$150K – $199.5K") is None

    def test_salary_range_hyphen_returns_none(self):
        assert clean_company_name("$176K - $234K") is None

    def test_salary_range_with_decimals_returns_none(self):
        assert clean_company_name("$206K – $275.5K") is None

    def test_strips_surrounding_double_quotes(self):
        assert clean_company_name('"Dbank"') == "Dbank"

    def test_strips_surrounding_double_quotes_complex(self):
        assert clean_company_name('"KELKOO"') == "KELKOO"

    def test_preserves_non_latin_company_name(self):
        # Cyrillic company name — not a special character, should be preserved
        assert clean_company_name('"Hamkorbank" АТБ') == 'Hamkorbank" АТБ'

    def test_strips_whitespace(self):
        assert clean_company_name("  Acme Corp  ") == "Acme Corp"

    def test_empty_string_returns_none(self):
        assert clean_company_name("") is None

    def test_only_hashes_returns_none(self):
        assert clean_company_name("###") is None

    def test_only_quotes_returns_none(self):
        assert clean_company_name('""') is None

    def test_none_returns_none(self):
        assert clean_company_name(None) is None

    def test_int_returns_none(self):
        assert clean_company_name(42) is None  # type: ignore[arg-type]

    def test_hash_then_salary_range_returns_none(self):
        # After stripping "#", the remainder looks like a salary range
        assert clean_company_name("#$150K – $199.5K") is None

    # --- parenthesised numeric code prefix ---

    def test_strips_parens_code_prefix(self):
        assert clean_company_name("(0110) Companhia IBM Portuguesa, S.A.") == (
            "Companhia IBM Portuguesa, S.A."
        )

    def test_strips_parens_code_prefix_ibm(self):
        assert clean_company_name(
            "(0147) International Business Machines Corporation"
        ) == ("International Business Machines Corporation")

    # --- leading-zero and 5+-digit numeric code prefix ---

    def test_strips_leading_zero_number_prefix(self):
        assert (
            clean_company_name("027 Parks Culture and Sport")
            == "Parks Culture and Sport"
        )

    def test_strips_leading_zero_long_number(self):
        assert clean_company_name("00002 Citibank, N.A.") == "Citibank, N.A."

    def test_strips_five_digit_number_prefix(self):
        assert clean_company_name("12542 Citicorp Services India Private Limited") == (
            "Citicorp Services India Private Limited"
        )

    def test_preserves_four_digit_company_name(self):
        # "1872 Consulting" is a real company — must NOT strip "1872"
        assert clean_company_name("1872 Consulting") == "1872 Consulting"

    def test_preserves_two_digit_company_name(self):
        assert clean_company_name("24 Seven Talent") == "24 Seven Talent"

    def test_preserves_three_digit_company_name(self):
        assert clean_company_name("500 Global") == "500 Global"

    # --- leading dashes ---

    def test_strips_double_dash_prefix(self):
        assert clean_company_name("-  - Si-Ware Systems") == "Si-Ware Systems"

    def test_strips_single_dash_prefix_no_space(self):
        assert clean_company_name("-WorkEthix") == "WorkEthix"

    def test_strips_single_dash_prefix_with_space(self):
        assert clean_company_name("- INTM Groupe") == "INTM Groupe"

    def test_only_dashes_returns_none(self):
        assert clean_company_name("- -") is None

    # --- not-a-company → "Not Identified" ---

    def test_reviews_pattern_not_identified(self):
        assert clean_company_name("20 reviews") == "Not Identified"

    def test_reviews_singular_not_identified(self):
        assert clean_company_name("5 review") == "Not Identified"

    def test_reviews_case_insensitive_not_identified(self):
        assert clean_company_name("100 REVIEWS") == "Not Identified"

    def test_pure_digits_not_identified(self):
        assert clean_company_name("3677") == "Not Identified"

    def test_pure_digits_two_chars_not_identified(self):
        assert clean_company_name("99") == "Not Identified"

    def test_long_digit_code_not_identified(self):
        assert clean_company_name("201000200M") == "Not Identified"

    def test_short_digit_letter_company_kept(self):
        # "3M", "2K", "7N" are real companies — two chars, not flagged
        assert clean_company_name("3M") == "3M"

    def test_two_char_digit_letter_kept(self):
        assert clean_company_name("2K") == "2K"

    def test_three_char_digit_letter_kept(self):
        # "24S" appears 16 times in the dataset and is treated as a real company
        assert clean_company_name("24S") == "24S"


# ---------------------------------------------------------------------------
# parse_job_location
# ---------------------------------------------------------------------------


class TestParseJobLocation:
    # --- remote variants ---

    def test_anywhere_is_remote(self):
        assert parse_job_location("Anywhere") == (None, None, None, True, "remote")

    def test_anywhere_case_insensitive(self):
        assert parse_job_location("ANYWHERE") == (None, None, None, True, "remote")

    def test_remote_is_remote(self):
        assert parse_job_location("Remote") == (None, None, None, True, "remote")

    def test_remoto_is_remote(self):
        assert parse_job_location("Remoto") == (None, None, None, True, "remote")

    # --- country only ---

    def test_single_part_is_country_only(self):
        assert parse_job_location("Chile") == (
            None,
            None,
            "Chile",
            False,
            "country_only",
        )

    def test_single_part_singapore(self):
        assert parse_job_location("Singapore") == (
            None,
            None,
            "Singapore",
            False,
            "country_only",
        )

    # --- city, country ---

    def test_two_parts_city_country(self):
        assert parse_job_location("Karachi, Pakistan") == (
            "Karachi",
            None,
            "Pakistan",
            False,
            "city_country",
        )

    def test_two_parts_paris(self):
        assert parse_job_location("Paris, France") == (
            "Paris",
            None,
            "France",
            False,
            "city_country",
        )

    # --- city, state, country ---

    def test_three_parts_city_state_country(self):
        assert parse_job_location("Jalisco del Refugio, Jalisco, Mexico") == (
            "Jalisco del Refugio",
            "Jalisco",
            "Mexico",
            False,
            "city_state_country",
        )

    def test_three_parts_bengaluru(self):
        city, state, country, is_remote, fmt = parse_job_location(
            "Bengaluru, Karnataka, India"
        )
        assert city == "Bengaluru"
        assert state == "Karnataka"
        assert country == "India"
        assert is_remote is False
        assert fmt == "city_state_country"

    def test_duplicate_city_state_accent_insensitive(self):
        # "Bogotá, Bogota, Colombia" — city and state are the same after accent folding
        city, state, country, is_remote, fmt = parse_job_location(
            "Bogotá, Bogota, Colombia"
        )
        assert city == "Bogotá"
        assert state is None
        assert country == "Colombia"
        assert fmt == "city_state_country"

    def test_duplicate_city_state_exact_match(self):
        # Plain duplicate without accents
        city, state, country, is_remote, fmt = parse_job_location("Lima, Lima, Peru")
        assert city == "Lima"
        assert state is None
        assert country == "Peru"

    def test_non_duplicate_state_preserved(self):
        city, state, country, is_remote, fmt = parse_job_location(
            "Colombia, Huila, Colombia"
        )
        assert city == "Colombia"
        assert state == "Huila"
        assert country == "Colombia"

    # --- edge cases ---

    def test_none_returns_nulls(self):
        assert parse_job_location(None) == (None, None, None, False, None)

    def test_empty_string_returns_nulls(self):
        assert parse_job_location("") == (None, None, None, False, None)

    def test_whitespace_only_returns_nulls(self):
        assert parse_job_location("   ") == (None, None, None, False, None)

    def test_strips_part_whitespace(self):
        city, state, country, _, fmt = parse_job_location("  New York  ,  NY  ")
        assert city == "New York"
        assert country == "NY"
        assert fmt == "city_country"


# ---------------------------------------------------------------------------
# normalize_schedule_type
# ---------------------------------------------------------------------------


class TestNormalizeScheduleType:
    def test_full_time_lowercase(self):
        assert normalize_schedule_type("Full-time") == "full-time"

    def test_combined_type_preserved_and_lowercased(self):
        assert (
            normalize_schedule_type("Full-time and Internship")
            == "full-time and internship"
        )

    def test_complex_combined_type(self):
        assert (
            normalize_schedule_type("Full-time, Temp work, and Internship")
            == "full-time, temp work, and internship"
        )

    def test_strips_whitespace(self):
        assert normalize_schedule_type("  Part-time  ") == "part-time"

    def test_none_returns_none(self):
        assert normalize_schedule_type(None) is None

    def test_empty_string_returns_none(self):
        assert normalize_schedule_type("") is None

    def test_already_lowercase_unchanged(self):
        assert normalize_schedule_type("contractor") == "contractor"


# ---------------------------------------------------------------------------
# reconcile_country
# ---------------------------------------------------------------------------


class TestReconcileCountry:
    def test_uses_job_country_when_present(self):
        assert reconcile_country("Chile", "United States") == "Chile"

    def test_falls_back_to_search_location_when_job_country_is_none(self):
        assert reconcile_country(None, "United States") == "United States"

    def test_falls_back_when_job_country_is_empty_string(self):
        assert reconcile_country("", "Brazil") == "Brazil"

    def test_falls_back_when_job_country_is_whitespace(self):
        assert reconcile_country("   ", "Brazil") == "Brazil"

    def test_both_none_returns_none(self):
        assert reconcile_country(None, None) is None

    def test_both_empty_returns_none(self):
        assert reconcile_country("", "") is None

    def test_strips_whitespace_from_result(self):
        assert reconcile_country("  Chile  ", "Argentina") == "Chile"


# ---------------------------------------------------------------------------
# detect_language
# ---------------------------------------------------------------------------


class TestDetectLanguage:
    # --- non-string / short input ---

    def test_none_returns_unknown(self):
        assert detect_language(None) == ("unknown", 0.0)

    def test_empty_string_returns_unknown(self):
        assert detect_language("") == ("unknown", 0.0)

    def test_two_chars_returns_unknown(self):
        assert detect_language("ab") == ("unknown", 0.0)

    def test_whitespace_only_returns_unknown(self):
        assert detect_language("   ") == ("unknown", 0.0)

    # --- Step 1: unicode script detection (no lingua needed) ---

    def test_cyrillic_text(self):
        lang, conf = detect_language("Аналитик данных")
        assert lang == "cyrillic"
        assert conf == 1.0

    def test_chinese_text(self):
        lang, conf = detect_language("数据工程师")
        assert lang == "zh"
        assert conf == 1.0

    def test_japanese_text(self):
        lang, conf = detect_language("データエンジニア")
        assert lang == "ja"
        assert conf == 1.0

    def test_korean_text(self):
        lang, conf = detect_language("데이터 엔지니어")
        assert lang == "ko"
        assert conf == 1.0

    def test_arabic_text(self):
        lang, conf = detect_language("مهندس بيانات")
        assert lang == "ar"
        assert conf == 1.0

    # --- Step 2: Latin text routed to lingua (mocked) ---

    def test_latin_text_delegates_to_lingua(self):
        with patch(
            "pipeline.clean._lingua_top_confidence", return_value=("en", 0.98)
        ) as mock_fn:
            lang, conf = detect_language("Data Engineer")
        mock_fn.assert_called_once_with("Data Engineer")
        assert lang == "en"
        assert conf == 0.98

    def test_lingua_internal_error_returns_unknown(self):
        # _lingua_top_confidence catches its own exceptions and returns unknown/0.0
        with patch(
            "pipeline.clean._lingua_top_confidence", return_value=("unknown", 0.0)
        ):
            lang, conf = detect_language("Some latin text")
        assert lang == "unknown"
        assert conf == 0.0

    def test_non_string_int_returns_unknown(self):
        assert detect_language(42) == ("unknown", 0.0)  # type: ignore[arg-type]
