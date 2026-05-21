{{ config(materialized='view') }}

with src as (
    select *
    from {{ source('raw_ats', 'ats_ashby__offers') }}
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
    _source_record_id                                                          as offer_id,
    json_extract_string(raw_payload, '$.application_id')                       as application_id,
    json_extract_string(raw_payload, '$.candidate_id')                         as candidate_id,
    json_extract_string(raw_payload, '$.requisition_id')                       as requisition_id,
    cast(json_extract_string(raw_payload, '$.extended_at') as timestamp)        as extended_at,
    try_cast(json_extract_string(raw_payload, '$.responded_at') as timestamp)   as responded_at,
    json_extract_string(raw_payload, '$.status')                               as status,
    try_cast(json_extract_string(raw_payload, '$.base_salary_amount') as double) as base_salary_amount,
    json_extract_string(raw_payload, '$.base_salary_currency')                 as base_salary_currency,
    json_extract_string(raw_payload, '$.accepted_person_external_id')           as accepted_person_external_id,
    _source_updated_at                                                         as source_updated_at,
    _ingested_at_utc                                                           as ingested_at_utc,
    _run_id                                                                    as ingested_run_id
from latest
