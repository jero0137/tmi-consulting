"""Data quality validation for staging.stg_job_postings using Great Expectations.

Called after every transform_and_load_staging run. Raises DataQualityError if
any expectation fails so the pipeline halts with a clear diagnostic message.
"""

import logging

import great_expectations as gx
import pandas as pd
import psycopg2.extensions

logger = logging.getLogger(__name__)

_SUITE_NAME = "stg_job_postings"
_DATASOURCE_NAME = "staging_pandas_ds"
_ASSET_NAME = "stg_job_postings_asset"

_VALID_LOCATION_FORMATS = [
    "remote",
    "country_only",
    "city_country",
    "city_state_country",
]


class DataQualityError(Exception):
    """Raised when staging data fails one or more GX expectations."""


def _build_suite() -> gx.ExpectationSuite:
    """Return the GX ExpectationSuite for staging.stg_job_postings.

    Covers:
    - Primary key integrity (id not-null + unique)
    - Mandatory pipeline audit timestamp (cleaned_at)
    - Non-null boolean flag (location_is_remote, DDL: NOT NULL DEFAULT FALSE)
    - Language confidence bounded to [0, 1]
    - location_format restricted to the controlled vocabulary produced by parse_job_location
    - Salary columns non-negative when present
    - Table is not empty after the load

    Expectations are passed in the constructor (not via add_expectation) so the
    suite can be built without an active GX data context — useful for unit tests
    and for calling _build_suite() before the context is initialised.
    """
    return gx.ExpectationSuite(
        name=_SUITE_NAME,
        expectations=[
            gx.expectations.ExpectColumnValuesToNotBeNull(column="id"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="id"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="cleaned_at"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="location_is_remote"),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="job_title_lang_confidence",
                min_value=0.0,
                max_value=1.0,
            ),
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="location_format",
                value_set=_VALID_LOCATION_FORMATS,
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="salary_year_avg",
                min_value=0,
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="salary_hour_avg",
                min_value=0,
            ),
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
        ],
    )


def validate_staging(conn: psycopg2.extensions.connection) -> None:
    """Run GX expectations against the full staging.stg_job_postings table.

    Reads the table into a pandas DataFrame, builds an ephemeral GX context,
    and evaluates all expectations. Logs PASS/FAIL per expectation, then
    raises DataQualityError if the overall validation fails.
    """
    logger.info("Reading staging.stg_job_postings for data quality validation")
    df = pd.read_sql("SELECT * FROM staging.stg_job_postings", conn)
    logger.info("Validating %d rows with Great Expectations", len(df))

    context = gx.get_context(mode="ephemeral")
    ds = context.data_sources.add_pandas(_DATASOURCE_NAME)
    asset = ds.add_dataframe_asset(_ASSET_NAME)
    batch_def = asset.add_batch_definition_whole_dataframe("full_batch")

    suite = _build_suite()
    context.suites.add(suite)

    vd = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="stg_job_postings_validation",
            data=batch_def,
            suite=suite,
        )
    )

    result = vd.run(batch_parameters={"dataframe": df})
    _log_results(result)

    if not result.success:
        failed = sum(1 for r in result.results if not r.success)
        raise DataQualityError(
            f"Staging data quality check failed: {failed} expectation(s) did not pass — "
            "see logs above for details"
        )

    logger.info(
        "Data quality validation passed — all %d expectations met", len(result.results)
    )


def _log_results(result: gx.core.ExpectationSuiteValidationResult) -> None:
    """Log each expectation result at INFO (pass) or WARNING (fail) level."""
    for r in result.results:
        exp_type = r.expectation_config.type
        col = r.expectation_config.kwargs.get("column", "(table)")
        if r.success:
            logger.info("  PASS  %-55s column=%s", exp_type, col)
        else:
            logger.warning("  FAIL  %-55s column=%s | %s", exp_type, col, r.result)
