-- Skill names come pre-parsed as TEXT[] by the Python ingestion; categories
-- come from the JSONB job_type_skills column. For skills that appear under
-- multiple categories across postings, pick the most frequent one as canonical.
-- Skills with no category mapping fall back to 'other'.
WITH raw_skills AS (
    SELECT
        LOWER(TRIM(skill_name)) AS skill_name,
        job_type_skills
    FROM {{ source('staging', 'stg_job_postings') }}
    CROSS JOIN LATERAL UNNEST(job_skills) AS t(skill_name)
    WHERE job_skills IS NOT NULL
),

skills_with_category AS (
    SELECT
        rs.skill_name,
        COALESCE(cat.category_key, 'other') AS skill_category
    FROM raw_skills rs
    LEFT JOIN LATERAL (
        SELECT jt.key AS category_key
        FROM jsonb_each(rs.job_type_skills) AS jt(key, val)
        WHERE rs.skill_name = ANY(
            ARRAY(SELECT jsonb_array_elements_text(jt.val))
        )
        LIMIT 1
    ) cat ON true
),

category_counts AS (
    SELECT skill_name, skill_category, COUNT(*) AS cnt
    FROM skills_with_category
    GROUP BY skill_name, skill_category
),

ranked AS (
    SELECT
        skill_name,
        skill_category,
        ROW_NUMBER() OVER (
            PARTITION BY skill_name
            ORDER BY cnt DESC, skill_category
        ) AS rn
    FROM category_counts
),

deduped AS (
    SELECT skill_name, skill_category FROM ranked WHERE rn = 1
)

SELECT
    ROW_NUMBER() OVER (ORDER BY skill_name) AS skill_id,
    skill_name,
    skill_category,
    NOW() AT TIME ZONE 'America/Bogota' AS creation_date,
    NOW() AT TIME ZONE 'America/Bogota' AS update_date
FROM deduped
