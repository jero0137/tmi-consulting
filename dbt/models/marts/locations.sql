-- Deduplicate on (city, country, is_remote).
-- location_city and country_final come from parse_job_location / reconcile_country
-- in Python; location_is_remote is already TRUE for "Anywhere"/"Remote" strings.
-- ROW_NUMBER() PARTITION handles NULLs correctly (NULLS are equal within a partition).
WITH ranked AS (
    SELECT
        job_location        AS raw_location,
        location_city       AS city,
        country_final       AS country,
        location_is_remote  AS is_remote,
        ROW_NUMBER() OVER (
            PARTITION BY location_city, country_final, location_is_remote
            ORDER BY job_location NULLS LAST
        ) AS rn
    FROM {{ source('staging', 'stg_job_postings') }}
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY country NULLS LAST, city NULLS LAST, is_remote
    )           AS location_id,
    raw_location,
    city,
    country,
    is_remote,
    NOW() AT TIME ZONE 'America/Bogota' AS creation_date,
    NOW() AT TIME ZONE 'America/Bogota' AS update_date
FROM ranked
WHERE rn = 1
