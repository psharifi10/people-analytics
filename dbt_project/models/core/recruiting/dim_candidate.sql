{{ config(materialized='table') }}

-- Candidate dimension: one row per ATS candidate record.
-- Joins to dim_person via person_key when the candidate has a known
-- person_external_id (set when the candidate is hired and linked to HRIS).

with candidates as (
    select * from {{ ref('stg_ats__candidates') }}
)
select
    {{ surrogate_key(['candidate_id']) }}            as candidate_key,
    {{ surrogate_key(['person_external_id']) }}      as person_key,
    candidate_id,
    person_external_id,
    first_name,
    last_name,
    email,
    linkedin_url,
    candidate_source,
    source_updated_at,
    ingested_at_utc,
    ingested_run_id
from candidates
