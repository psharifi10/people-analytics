-- Right-to-erasure stored procedure (design doc §11.7).
-- Two-engineer approval gate is enforced upstream in the ticket workflow,
-- not in SQL. This proc is the "execute" step.

USE DATABASE PEOPLE_ANALYTICS;
USE SCHEMA CORE;

CREATE OR REPLACE PROCEDURE erase_person(p_person_external_id VARCHAR, p_ticket_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_employee_keys ARRAY;
    v_count INTEGER;
BEGIN
    -- 1. Resolve person → employee_keys (across all stints).
    SELECT ARRAY_AGG(employee_key) INTO :v_employee_keys
    FROM CORE.DIM_EMPLOYEE WHERE person_external_id = :p_person_external_id;

    IF (ARRAY_SIZE(:v_employee_keys) = 0) THEN
        RETURN 'No matching person found: ' || :p_person_external_id;
    END IF;

    -- 2. Tombstone in PII tables (replace with cryptographic placeholder).
    UPDATE CORE.DIM_PERSON SET
        first_name = 'ERASED',
        last_name = 'ERASED',
        personal_email = 'erased+' || person_key || '@example.invalid'
    WHERE person_external_id = :p_person_external_id;

    UPDATE CORE.DIM_EMPLOYEE SET
        first_name = 'ERASED',
        last_name = 'ERASED',
        work_email = 'erased+' || employee_key || '@example.invalid'
    WHERE person_external_id = :p_person_external_id;

    -- 3. Delete from RAW landings (re-ingestion guard handles future inserts).
    DELETE FROM RAW.HRIS_WORKDAY__WORKERS WHERE _source_record_id IN (
        SELECT worker_id FROM CORE.DIM_EMPLOYEE WHERE person_external_id = :p_person_external_id
    );
    DELETE FROM RAW.HRIS_WORKDAY__PERSONS WHERE _source_record_id = :p_person_external_id;

    -- 4. Audit row.
    INSERT INTO AUDIT.ERASURE_LOG (ticket_id, person_external_id, executed_at, executed_by)
    VALUES (:p_ticket_id, :p_person_external_id, CURRENT_TIMESTAMP(), CURRENT_USER());

    RETURN 'Erased: ' || :p_person_external_id || ' (ticket ' || :p_ticket_id || ')';
END;
$$;
