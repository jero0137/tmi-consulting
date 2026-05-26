-- Idempotent: drop before add so this script is safe to run on every pipeline execution.
-- Run AFTER dbt has built all mart tables.

-- ------------------------------------------------------------------ --
-- companies                                                           --
-- ------------------------------------------------------------------ --
ALTER TABLE marts.companies DROP CONSTRAINT IF EXISTS pk_companies      CASCADE;
ALTER TABLE marts.companies DROP CONSTRAINT IF EXISTS uq_companies_name CASCADE;
ALTER TABLE marts.companies ADD CONSTRAINT pk_companies      PRIMARY KEY (company_id);
ALTER TABLE marts.companies ADD CONSTRAINT uq_companies_name UNIQUE      (company_name);

-- ------------------------------------------------------------------ --
-- locations                                                           --
-- ------------------------------------------------------------------ --
ALTER TABLE marts.locations DROP CONSTRAINT IF EXISTS pk_locations CASCADE;
ALTER TABLE marts.locations ADD CONSTRAINT pk_locations PRIMARY KEY (location_id);

-- ------------------------------------------------------------------ --
-- job_schedules                                                       --
-- ------------------------------------------------------------------ --
ALTER TABLE marts.job_schedules DROP CONSTRAINT IF EXISTS pk_job_schedules      CASCADE;
ALTER TABLE marts.job_schedules DROP CONSTRAINT IF EXISTS uq_job_schedules_type CASCADE;
ALTER TABLE marts.job_schedules ADD CONSTRAINT pk_job_schedules      PRIMARY KEY (schedule_id);
ALTER TABLE marts.job_schedules ADD CONSTRAINT uq_job_schedules_type UNIQUE      (schedule_type);

-- ------------------------------------------------------------------ --
-- skills                                                              --
-- ------------------------------------------------------------------ --
ALTER TABLE marts.skills DROP CONSTRAINT IF EXISTS pk_skills      CASCADE;
ALTER TABLE marts.skills DROP CONSTRAINT IF EXISTS uq_skills_name CASCADE;
ALTER TABLE marts.skills ADD CONSTRAINT pk_skills      PRIMARY KEY (skill_id);
ALTER TABLE marts.skills ADD CONSTRAINT uq_skills_name UNIQUE      (skill_name);

-- ------------------------------------------------------------------ --
-- job_postings (FK refs require dim PKs to exist — order matters)    --
-- ------------------------------------------------------------------ --
ALTER TABLE marts.job_postings DROP CONSTRAINT IF EXISTS fk_job_postings_company  CASCADE;
ALTER TABLE marts.job_postings DROP CONSTRAINT IF EXISTS fk_job_postings_location CASCADE;
ALTER TABLE marts.job_postings DROP CONSTRAINT IF EXISTS fk_job_postings_schedule CASCADE;
ALTER TABLE marts.job_postings DROP CONSTRAINT IF EXISTS pk_job_postings          CASCADE;
DROP INDEX IF EXISTS idx_job_postings_company_id;
DROP INDEX IF EXISTS idx_job_postings_location_id;
DROP INDEX IF EXISTS idx_job_postings_schedule_id;
DROP INDEX IF EXISTS idx_job_postings_posted_date;
ALTER TABLE marts.job_postings ADD CONSTRAINT pk_job_postings          PRIMARY KEY (job_id);
ALTER TABLE marts.job_postings ADD CONSTRAINT fk_job_postings_company  FOREIGN KEY (company_id)  REFERENCES marts.companies(company_id);
ALTER TABLE marts.job_postings ADD CONSTRAINT fk_job_postings_location FOREIGN KEY (location_id) REFERENCES marts.locations(location_id);
ALTER TABLE marts.job_postings ADD CONSTRAINT fk_job_postings_schedule FOREIGN KEY (schedule_id) REFERENCES marts.job_schedules(schedule_id);
CREATE INDEX idx_job_postings_company_id  ON marts.job_postings (company_id);
CREATE INDEX idx_job_postings_location_id ON marts.job_postings (location_id);
CREATE INDEX idx_job_postings_schedule_id ON marts.job_postings (schedule_id);
CREATE INDEX idx_job_postings_posted_date ON marts.job_postings (job_posted_date);

-- ------------------------------------------------------------------ --
-- job_skills (FK refs require job_postings PK and skills PK)         --
-- ------------------------------------------------------------------ --
ALTER TABLE marts.job_skills DROP CONSTRAINT IF EXISTS fk_job_skills_skill CASCADE;
ALTER TABLE marts.job_skills DROP CONSTRAINT IF EXISTS fk_job_skills_job   CASCADE;
ALTER TABLE marts.job_skills DROP CONSTRAINT IF EXISTS pk_job_skills       CASCADE;
DROP INDEX IF EXISTS idx_job_skills_skill_id;
ALTER TABLE marts.job_skills ADD CONSTRAINT pk_job_skills       PRIMARY KEY (job_id, skill_id);
ALTER TABLE marts.job_skills ADD CONSTRAINT fk_job_skills_job   FOREIGN KEY (job_id)   REFERENCES marts.job_postings(job_id);
ALTER TABLE marts.job_skills ADD CONSTRAINT fk_job_skills_skill FOREIGN KEY (skill_id) REFERENCES marts.skills(skill_id);
CREATE INDEX idx_job_skills_skill_id ON marts.job_skills (skill_id);
