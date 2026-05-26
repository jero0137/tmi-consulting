{{
    config(
        pre_hook="SET LOCAL work_mem = '512MB'",
        post_hook=[
            "ALTER TABLE {{ this }} ADD CONSTRAINT pk_job_skills       PRIMARY KEY (job_id, skill_id)",
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_job_skills_job   FOREIGN KEY (job_id)   REFERENCES {{ ref('job_postings') }} (job_id)",
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_job_skills_skill FOREIGN KEY (skill_id) REFERENCES {{ ref('skills') }}      (skill_id)",
            "CREATE INDEX idx_job_skills_skill_id ON {{ this }} (skill_id)"
        ]
    )
}}

-- Bridge table: one row per (job, skill) pair.
-- Split into three CTEs so the planner can apply each optimisation independently:
--   1. valid_jobs  — filter staging to only rows that became job_postings
--                    (uses the PK index on job_postings.job_id from its post-hook).
--   2. unnested    — UNNEST after filtering, and normalise the name once upfront.
--   3. pairs       — hash-join against the 252-row skills dim, then DISTINCT.
WITH valid_jobs AS (
    SELECT stg.id AS job_id, stg.job_skills
    FROM {{ source('staging', 'stg_job_postings') }} stg
    INNER JOIN {{ ref('job_postings') }}             jp  ON jp.job_id = stg.id
    WHERE stg.job_skills IS NOT NULL
),

unnested AS (
    SELECT
        vj.job_id,
        LOWER(TRIM(t.skill_name)) AS skill_name
    FROM valid_jobs                              vj
    CROSS JOIN LATERAL UNNEST(vj.job_skills)    AS t(skill_name)
),

pairs AS (
    SELECT DISTINCT
        u.job_id,
        sk.skill_id
    FROM unnested               u
    JOIN {{ ref('skills') }}    sk  ON sk.skill_name = u.skill_name
)

SELECT
    job_id,
    skill_id,
    NOW() AT TIME ZONE 'America/Bogota' AS creation_date,
    NOW() AT TIME ZONE 'America/Bogota' AS update_date
FROM pairs
