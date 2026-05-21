{{ config(materialized='view') }}

with src as (
    select *
    from {{ source('raw_ats', 'ats_ashby__applications') }}
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
    _source_record_id                                                          as application_id,
    json_extract_string(raw_payload, '$.candidate_id')                         as candidate_id,
    json_extract_string(raw_payload, '$.requisition_id')                       as requisition_id,
    cast(json_extract_string(raw_payload, '$.submitted_at') as timestamp)      as submitted_at,
    json_extract_string(raw_payload, '$.current_stage')                        as current_stage,
    json_extract_string(raw_payload, '$.status')                               as status,
    json_extract_string(raw_payload, '$.final_outcome_reason')                 as final_outcome_reason,
    json_extract_string(raw_payload, '$.source')                               as application_source,
    cast(json_extract_string(raw_payload, '$.is_winning') as boolean)          as is_winning,
    try_cast(json_extract_string(raw_payload, '$.closed_at') as timestamp)     as closed_at,
    _source_updated_at                                                         as source_updated_at,
    _ingested_at_utc                                                           as ingested_at_utc,
    _run_id                                                                    as ingested_run_id
from latest
