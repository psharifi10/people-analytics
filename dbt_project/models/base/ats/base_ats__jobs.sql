{{ config(materialized='view') }}

with src as (
    select *
    from {{ source('raw_ats', 'ats_ashby__jobs') }}
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
    _source_record_id                                                     as requisition_id,
    json_extract_string(raw_payload, '$.job_title')                       as job_title,
    json_extract_string(raw_payload, '$.job_level')                       as job_level,
    json_extract_string(raw_payload, '$.department')                      as department,
    json_extract_string(raw_payload, '$.function')                        as function_name,
    cast(json_extract_string(raw_payload, '$.opened_at') as date)         as opened_at,
    try_cast(json_extract_string(raw_payload, '$.closed_at') as date)     as closed_at,
    json_extract_string(raw_payload, '$.status')                          as status,
    json_extract_string(raw_payload, '$.hired_worker_ids')                as hired_worker_ids_json,
    _source_updated_at                                                    as source_updated_at,
    _ingested_at_utc                                                      as ingested_at_utc,
    _run_id                                                               as ingested_run_id
from latest
