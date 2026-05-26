-- Unique schedule types. normalize_schedule_type (Python) already lowercased
-- and trimmed these values, so no further transformation is needed.
SELECT
    ROW_NUMBER() OVER (ORDER BY schedule_type) AS schedule_id,
    schedule_type,
    NOW() AT TIME ZONE 'America/Bogota' AS creation_date,
    NOW() AT TIME ZONE 'America/Bogota' AS update_date
FROM (
    SELECT DISTINCT job_schedule_type AS schedule_type
    FROM {{ source('staging', 'stg_job_postings') }}
    WHERE job_schedule_type IS NOT NULL
) s
