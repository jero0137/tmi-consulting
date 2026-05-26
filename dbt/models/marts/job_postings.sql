{{
    config(
        post_hook=[
            "ALTER TABLE {{ this }} ADD CONSTRAINT pk_job_postings          PRIMARY KEY (job_id)",
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_job_postings_company  FOREIGN KEY (company_id)  REFERENCES {{ ref('companies') }}    (company_id)",
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_job_postings_location FOREIGN KEY (location_id) REFERENCES {{ ref('locations') }}    (location_id)",
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_job_postings_schedule FOREIGN KEY (schedule_id) REFERENCES {{ ref('job_schedules') }} (schedule_id)",
            "CREATE INDEX idx_job_postings_company_id   ON {{ this }} (company_id)",
            "CREATE INDEX idx_job_postings_location_id  ON {{ this }} (location_id)",
            "CREATE INDEX idx_job_postings_schedule_id  ON {{ this }} (schedule_id)",
            "CREATE INDEX idx_job_postings_posted_date  ON {{ this }} (job_posted_date)"
        ]
    )
}}

-- Most cleansing happened in Python (trim, salary normalisation, language
-- detection, etc.). This model resolves the dim FKs and enforces NOT-NULL
-- requirements on the join keys. Rows with NULL company_name or NULL
-- job_schedule_type are excluded naturally by the INNER JOINs.
WITH source AS (
    SELECT
        id,
        job_title,
        job_title_short,
        job_via,
        job_posted_date,
        COALESCE(job_work_from_home,    FALSE) AS work_from_home,
        COALESCE(job_no_degree_mention, FALSE) AS no_degree_mention,
        COALESCE(job_health_insurance,  FALSE) AS health_insurance,
        salary_rate,
        salary_year_avg,
        salary_hour_avg,
        company_name,
        location_city,
        country_final,
        location_is_remote,
        job_schedule_type
    FROM {{ source('staging', 'stg_job_postings') }}
)

SELECT
    s.id                        AS job_id,
    s.job_title,
    s.job_title_short,
    s.job_via,
    s.job_posted_date,
    s.work_from_home,
    s.no_degree_mention,
    s.health_insurance,
    s.salary_rate,
    s.salary_year_avg,
    s.salary_hour_avg,
    c.company_id,
    l.location_id,
    sch.schedule_id,
    NOW() AT TIME ZONE 'America/Bogota' AS creation_date,
    NOW() AT TIME ZONE 'America/Bogota' AS update_date
FROM source s
JOIN {{ ref('companies') }}     c
    ON LOWER(c.company_name) = LOWER(s.company_name)
JOIN {{ ref('locations') }}     l
    ON l.city      IS NOT DISTINCT FROM s.location_city
   AND l.country   IS NOT DISTINCT FROM s.country_final
   AND l.is_remote = s.location_is_remote
JOIN {{ ref('job_schedules') }} sch
    ON sch.schedule_type = s.job_schedule_type
