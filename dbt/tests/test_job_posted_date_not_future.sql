-- Fail if any job posting has a future posted_date (data freshness guard).
SELECT job_id
FROM {{ ref('job_postings') }}
WHERE job_posted_date > CURRENT_TIMESTAMP
