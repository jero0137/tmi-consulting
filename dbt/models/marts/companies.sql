-- Deduplicate case-insensitively so "IBM" and "ibm" map to a single row.
-- clean_company_name (Python) already trimmed and normalised the names, so
-- only a LOWER() comparison is needed here.
WITH deduped AS (
    SELECT DISTINCT ON (LOWER(company_name))
        company_name
    FROM {{ source('staging', 'stg_job_postings') }}
    WHERE company_name IS NOT NULL
    ORDER BY LOWER(company_name), company_name
)

SELECT
    ROW_NUMBER() OVER (ORDER BY LOWER(company_name)) AS company_id,
    company_name,
    NOW() AT TIME ZONE 'America/Bogota' AS creation_date,
    NOW() AT TIME ZONE 'America/Bogota' AS update_date
FROM deduped
