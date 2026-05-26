-- Fail if any (job_id, skill_id) pair appears more than once.
-- The bridge table PK is the composite key; duplicates indicate an upstream bug.
SELECT job_id, skill_id, COUNT(*) AS cnt
FROM {{ ref('job_skills') }}
GROUP BY job_id, skill_id
HAVING COUNT(*) > 1
