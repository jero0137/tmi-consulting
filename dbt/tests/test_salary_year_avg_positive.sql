-- Fail if any non-null yearly salary is zero or negative.
SELECT job_id
FROM {{ ref('job_postings') }}
WHERE salary_year_avg IS NOT NULL
  AND salary_year_avg <= 0
