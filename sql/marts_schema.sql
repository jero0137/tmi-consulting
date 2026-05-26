-- DDL for all marts tables.
-- Run this before dbt so the target schema and tables exist with explicit
-- types, constraints, and indexes. dbt (materialized: table) drops and
-- recreates each table on every run, so constraints defined here are
-- re-applied by this script on the next pipeline execution.

CREATE SCHEMA IF NOT EXISTS marts;

-- ------------------------------------------------------------------ --
-- Dimension: companies                                                --
-- ------------------------------------------------------------------ --
CREATE TABLE IF NOT EXISTS marts.companies (
    company_id    BIGINT        NOT NULL,
    company_name  VARCHAR(255)  NOT NULL,
    creation_date TIMESTAMP     NOT NULL,
    update_date   TIMESTAMP     NOT NULL,

    CONSTRAINT pk_companies        PRIMARY KEY (company_id),
    CONSTRAINT uq_companies_name   UNIQUE      (company_name)
);

-- ------------------------------------------------------------------ --
-- Dimension: locations                                                --
-- ------------------------------------------------------------------ --
CREATE TABLE IF NOT EXISTS marts.locations (
    location_id   BIGINT        NOT NULL,
    raw_location  VARCHAR(255),
    city          VARCHAR(100),
    country       VARCHAR(100),
    is_remote     BOOLEAN       NOT NULL DEFAULT FALSE,
    creation_date TIMESTAMP     NOT NULL,
    update_date   TIMESTAMP     NOT NULL,

    CONSTRAINT pk_locations PRIMARY KEY (location_id)
);

-- ------------------------------------------------------------------ --
-- Dimension: job_schedules                                            --
-- ------------------------------------------------------------------ --
CREATE TABLE IF NOT EXISTS marts.job_schedules (
    schedule_id   BIGINT        NOT NULL,
    schedule_type VARCHAR(100)  NOT NULL,
    creation_date TIMESTAMP     NOT NULL,
    update_date   TIMESTAMP     NOT NULL,

    CONSTRAINT pk_job_schedules          PRIMARY KEY (schedule_id),
    CONSTRAINT uq_job_schedules_type     UNIQUE      (schedule_type)
);

-- ------------------------------------------------------------------ --
-- Dimension: skills                                                   --
-- ------------------------------------------------------------------ --
CREATE TABLE IF NOT EXISTS marts.skills (
    skill_id       BIGINT        NOT NULL,
    skill_name     VARCHAR(100)  NOT NULL,
    skill_category VARCHAR(50),
    creation_date  TIMESTAMP     NOT NULL,
    update_date    TIMESTAMP     NOT NULL,

    CONSTRAINT pk_skills          PRIMARY KEY (skill_id),
    CONSTRAINT uq_skills_name     UNIQUE      (skill_name)
);

-- ------------------------------------------------------------------ --
-- Fact: job_postings                                                  --
-- ------------------------------------------------------------------ --
CREATE TABLE IF NOT EXISTS marts.job_postings (
    job_id             BIGINT         NOT NULL,
    job_title          VARCHAR(255)   NOT NULL,
    job_title_short    VARCHAR(100)   NOT NULL,
    job_via            VARCHAR(100),
    job_posted_date    TIMESTAMP,
    work_from_home     BOOLEAN        NOT NULL DEFAULT FALSE,
    no_degree_mention  BOOLEAN        NOT NULL DEFAULT FALSE,
    health_insurance   BOOLEAN        NOT NULL DEFAULT FALSE,
    salary_rate        VARCHAR(10),
    salary_year_avg    NUMERIC(10,2),
    salary_hour_avg    NUMERIC(10,2),
    company_id         BIGINT         NOT NULL,
    location_id        BIGINT         NOT NULL,
    schedule_id        BIGINT         NOT NULL,
    creation_date      TIMESTAMP      NOT NULL,
    update_date        TIMESTAMP      NOT NULL,

    CONSTRAINT pk_job_postings              PRIMARY KEY (job_id),
    CONSTRAINT fk_job_postings_company      FOREIGN KEY (company_id)
        REFERENCES marts.companies(company_id),
    CONSTRAINT fk_job_postings_location     FOREIGN KEY (location_id)
        REFERENCES marts.locations(location_id),
    CONSTRAINT fk_job_postings_schedule     FOREIGN KEY (schedule_id)
        REFERENCES marts.job_schedules(schedule_id)
);

CREATE INDEX IF NOT EXISTS idx_job_postings_company_id
    ON marts.job_postings (company_id);

CREATE INDEX IF NOT EXISTS idx_job_postings_location_id
    ON marts.job_postings (location_id);

CREATE INDEX IF NOT EXISTS idx_job_postings_schedule_id
    ON marts.job_postings (schedule_id);

CREATE INDEX IF NOT EXISTS idx_job_postings_posted_date
    ON marts.job_postings (job_posted_date);

-- ------------------------------------------------------------------ --
-- Bridge: job_skills                                                  --
-- ------------------------------------------------------------------ --
CREATE TABLE IF NOT EXISTS marts.job_skills (
    job_id        BIGINT     NOT NULL,
    skill_id      BIGINT     NOT NULL,
    creation_date TIMESTAMP  NOT NULL,
    update_date   TIMESTAMP  NOT NULL,

    CONSTRAINT pk_job_skills         PRIMARY KEY (job_id, skill_id),
    CONSTRAINT fk_job_skills_job     FOREIGN KEY (job_id)
        REFERENCES marts.job_postings(job_id),
    CONSTRAINT fk_job_skills_skill   FOREIGN KEY (skill_id)
        REFERENCES marts.skills(skill_id)
);

CREATE INDEX IF NOT EXISTS idx_job_skills_skill_id
    ON marts.job_skills (skill_id);
