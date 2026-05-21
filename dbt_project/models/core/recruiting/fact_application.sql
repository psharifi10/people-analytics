{{ config(materialized='table') }}

-- Fact: one row per application (candidate applying to a specific requisition).
-- Grain: application_id. Conformed to candidate, requisition, and person keys.

with apps as (
    select * from {{ ref('stg_ats__applications') }}
)
select
    {{ surrogate_key(['application_id']) }}          as application_key,
    {{ surrogate_key(['candidate_id']) }}            as candidate_key,
    {{ surrogate_key(['requisition_id']) }}          as requisition_key,
    application_id,
    candidate_id,
    requisition_id,
    submitted_at,
    current_stage,
    status,
    final_outcome_reason,
    application_source,
    is_winning,
    closed_at,
    -- Days in funnel: submitted to closed (null if still active).
    case
        when closed_at is not null
        then datediff('day', submitted_at, closed_at)
    end                                             as days_in_funnel,
    source_updated_at,
    ingested_at_utc,
    ingested_run_id
from apps
