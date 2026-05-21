{{ config(materialized='table') }}

-- Fact: one row per application x stage transition.
-- Computes time_in_stage_hours by looking at the next transition for the same
-- application (lead window). Last stage has null duration (still active or closed).

with events as (
    select * from {{ ref('stg_ats__stage_events') }}
),
with_duration as (
    select
        *,
        lead(transitioned_at) over (
            partition by application_id
            order by transitioned_at
        ) as next_transition_at
    from events
)
select
    {{ surrogate_key(['stage_event_id']) }}          as stage_history_key,
    {{ surrogate_key(['application_id']) }}          as application_key,
    {{ surrogate_key(['candidate_id']) }}            as candidate_key,
    {{ surrogate_key(['requisition_id']) }}          as requisition_key,
    stage_event_id,
    application_id,
    candidate_id,
    requisition_id,
    from_stage,
    to_stage,
    transitioned_at,
    next_transition_at,
    -- Hours in this stage (null for terminal stage).
    case
        when next_transition_at is not null
        then round(
            extract(epoch from (next_transition_at - transitioned_at)) / 3600.0,
            1
        )
    end                                             as hours_in_stage,
    source_updated_at,
    ingested_at_utc,
    ingested_run_id
from with_duration
