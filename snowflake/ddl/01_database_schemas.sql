-- Database, schemas, and sequences for the People Analytics platform.
-- Run as ACCOUNTADMIN once per environment.

CREATE DATABASE IF NOT EXISTS PEOPLE_ANALYTICS;

USE DATABASE PEOPLE_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Append-only landing zone. Owned by extractor service account.';

CREATE SCHEMA IF NOT EXISTS BASE
    COMMENT = 'Typed flatten of raw VARIANT payloads (dbt views).';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Renamed, deduped, latest-record stage layer (dbt views).';

CREATE SCHEMA IF NOT EXISTS CORE
    COMMENT = 'Identity, dimensions, facts (dbt tables).';

CREATE SCHEMA IF NOT EXISTS MARTS
    COMMENT = 'Business-ready reporting marts (dbt tables).';

CREATE SCHEMA IF NOT EXISTS SEMANTIC
    COMMENT = 'Semantic / metric definitions (dbt views).';

-- Restricted variants — see role grants in 02_roles.sql.
CREATE SCHEMA IF NOT EXISTS CORE_RESTRICTED
    COMMENT = 'PII / comp / sensitive HR data. Restricted access only.';

CREATE SCHEMA IF NOT EXISTS MARTS_RESTRICTED
    COMMENT = 'Restricted reporting marts (e.g., compensation analytics).';

CREATE SCHEMA IF NOT EXISTS AUDIT
    COMMENT = 'Operational audit trail: erasure logs, access reviews, data quality incidents.';

-- Erasure audit table (referenced by 04_erasure_procedure.sql).
CREATE TABLE IF NOT EXISTS AUDIT.ERASURE_LOG (
    erasure_id        NUMBER AUTOINCREMENT PRIMARY KEY,
    ticket_id         VARCHAR NOT NULL,
    person_external_id VARCHAR NOT NULL,
    executed_at       TIMESTAMP_NTZ NOT NULL,
    executed_by       VARCHAR NOT NULL
);
