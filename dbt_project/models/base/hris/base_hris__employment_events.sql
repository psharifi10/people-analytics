{{ config(materialized='view') }}

with src as (
    select *
    from {{ source('raw_hris', 'hris_workday__employment_events') }}
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
    _source_record_id                                                       as event_id,
    json_extract_string(raw_payload, '$.worker_id')                         as worker_id,
    json_extract_string(raw_payload, '$.person_external_id')                as person_external_id,
    json_extract_string(raw_payload, '$.event_type')                        as event_type,
    cast(json_extract_string(raw_payload, '$.effective_date') as date)      as effective_date,
    json_extract_string(raw_payload, '$.department')                        as department,
    json_extract_string(raw_payload, '$.function')                          as function_name,
    json_extract_string(raw_payload, '$.job_title')                         as job_title,
    json_extract_string(raw_payload, '$.job_level')                         as job_level,
    json_extract_string(raw_payload, '$.manager_worker_id')                 as manager_worker_id,
    json_extract_string(raw_payload, '$.termination_reason')                as termination_reason,
    _source_updated_at                                                      as source_updated_at,
    _ingested_at_utc                                                        as ingested_at_utc,
    _run_id                                                                 as ingested_run_id
from latest
