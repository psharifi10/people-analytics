{{ config(materialized='table') }}

-- Fact: one row per offer extended to a candidate.
-- Links to application, candidate, requisition, and (if accepted) to employee.

with offers as (
    select * from {{ ref('stg_ats__offers') }}
)
select
    {{ surrogate_key(['offer_id']) }}                       as offer_key,
    {{ surrogate_key(['application_id']) }}                 as application_key,
    {{ surrogate_key(['candidate_id']) }}                   as candidate_key,
    {{ surrogate_key(['requisition_id']) }}                 as requisition_key,
    -- Link to dim_person for accepted offers (enables hire conversion joins).
    {{ surrogate_key(['accepted_person_external_id']) }}    as hired_person_key,
    offer_id,
    application_id,
    candidate_id,
    requisition_id,
    accepted_person_external_id,
    extended_at,
    responded_at,
    status,
    base_salary_amount,
    base_salary_currency,
    -- Days from offer extended to response (null if pending).
    case
        when responded_at is not null
        then datediff('day', extended_at, responded_at)
    end                                                    as days_to_respond,
    source_updated_at,
    ingested_at_utc,
    ingested_run_id
from offers
