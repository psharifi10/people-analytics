{{ config(materialized='view') }}

with src as (
    select *
    from {{ source('raw_hris', 'hris_workday__persons') }}
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
    _source_record_id                                                       as person_external_id,
    json_extract_string(raw_payload, '$.first_name')                        as first_name,
    json_extract_string(raw_payload, '$.last_name')                         as last_name,
    json_extract_string(raw_payload, '$.personal_email')                    as personal_email,
    json_extract_string(raw_payload, '$.worker_ids')                        as worker_ids_json,
    _source_updated_at                                                      as source_updated_at,
    _ingested_at_utc                                                        as ingested_at_utc,
    _run_id                                                                 as ingested_run_id
from latest
