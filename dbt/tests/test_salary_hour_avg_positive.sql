-- Fail if any non-null hourly salary is zero or negative.
SELECT job_id
FROM {{ ref('job_postings') }}
WHERE salary_hour_avg IS NOT NULL
  AND salary_hour_avg <= 0
