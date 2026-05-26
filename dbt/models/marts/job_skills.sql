{{
    config(
        post_hook=[
            "ALTER TABLE {{ this }} ADD CONSTRAINT pk_job_skills       PRIMARY KEY (job_id, skill_id)",
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_job_skills_job   FOREIGN KEY (job_id)   REFERENCES {{ ref('job_postings') }} (job_id)",
            "ALTER TABLE {{ this }} ADD CONSTRAINT fk_job_skills_skill FOREIGN KEY (skill_id) REFERENCES {{ ref('skills') }}      (skill_id)",
            "CREATE INDEX idx_job_skills_skill_id ON {{ this }} (skill_id)"
        ]
    )
}}

-- Bridge table: one row per (job, skill) pair.
-- UNNEST expands the TEXT[] job_skills array; we then resolve the skill_id
-- from the skills dim by lowercase name match. DISTINCT guards against any
-- duplicate skill names that survived the Python parsing step.
WITH pairs AS (
    SELECT DISTINCT
        jp.job_id,
        sk.skill_id
    FROM {{ source('staging', 'stg_job_postings') }}    stg
    JOIN {{ ref('job_postings') }}                       jp
        ON stg.id = jp.job_id
    CROSS JOIN LATERAL UNNEST(stg.job_skills)            AS t(skill_name)
    JOIN {{ ref('skills') }}                             sk
        ON LOWER(TRIM(t.skill_name)) = sk.skill_name
    WHERE stg.job_skills IS NOT NULL
)

SELECT
    job_id,
    skill_id,
    NOW() AT TIME ZONE 'America/Bogota' AS creation_date,
    NOW() AT TIME ZONE 'America/Bogota' AS update_date
FROM pairs
