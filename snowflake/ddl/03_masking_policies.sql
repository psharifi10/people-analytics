-- Masking policies for PII columns (design doc §11.4).
-- Only PA_SENSITIVE_HR sees real values; everyone else sees the mask.

USE DATABASE PEOPLE_ANALYTICS;
USE SCHEMA CORE;

CREATE OR REPLACE MASKING POLICY mask_email AS (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('PA_PLATFORM_ADMIN', 'PA_SENSITIVE_HR', 'PA_AUDITOR')
            THEN val
        ELSE REGEXP_REPLACE(val, '(.{1}).*(@.*)', '\\1***\\2')
    END;

CREATE OR REPLACE MASKING POLICY mask_full_name AS (val VARCHAR) RETURNS VARCHAR ->
    CASE
        WHEN CURRENT_ROLE() IN ('PA_PLATFORM_ADMIN', 'PA_SENSITIVE_HR', 'PA_AUDITOR')
            THEN val
        ELSE LEFT(val, 1) || '***'
    END;

-- Apply (run after dbt has created the tables)
-- ALTER TABLE CORE.DIM_EMPLOYEE MODIFY COLUMN work_email      SET MASKING POLICY mask_email;
-- ALTER TABLE CORE.DIM_EMPLOYEE MODIFY COLUMN first_name      SET MASKING POLICY mask_full_name;
-- ALTER TABLE CORE.DIM_EMPLOYEE MODIFY COLUMN last_name       SET MASKING POLICY mask_full_name;
-- ALTER TABLE CORE.DIM_PERSON   MODIFY COLUMN personal_email  SET MASKING POLICY mask_email;
