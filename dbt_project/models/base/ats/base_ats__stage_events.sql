{{ config(materialized='view') }}

with src as (
    select *
    from {{ source('raw_ats', 'ats_ashby__application_stage_events') }}
),
ranked as (
    select *,
        row_number() over (
            partition by _source_record_id
            order by _source_updated_at desc nulls last, _ingested_at_utc desc
        ) as rn
    from src
),
latest as (
    select * from ranked where rn = 1
)
select
    _source_record_id                                                          as stage_event_id,
    json_extract_string(raw_payload, '$.application_id')                       as application_id,
    json_extract_string(raw_payload, '$.candidate_id')                         as candidate_id,
    json_extract_string(raw_payload, '$.requisition_id')                       as requisition_id,
    json_extract_string(raw_payload, '$.from_stage')                           as from_stage,
    json_extract_string(raw_payload, '$.to_stage')                             as to_stage,
    cast(json_extract_string(raw_payload, '$.transitioned_at') as timestamp)   as transitioned_at,
    _source_updated_at                                                         as source_updated_at,
    _ingested_at_utc                                                           as ingested_at_utc,
    _run_id                                                                    as ingested_run_id
from latest
