{{ config(materialized='view') }}

with src as (
    select *
    from {{ source('raw_hris', 'hris_workday__profile_versions') }}
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
    _source_record_id                                                       as profile_version_id,
    {{ json_field('raw_payload', 'worker_id') }}                            as worker_id,
    {{ json_field('raw_payload', 'person_external_id') }}                   as person_external_id,
    {{ json_field_cast('raw_payload', 'effective_date', 'date') }}          as effective_date,
    {{ json_field('raw_payload', 'department') }}                           as department,
    {{ json_field('raw_payload', 'function') }}                             as function_name,
    {{ json_field('raw_payload', 'job_title') }}                            as job_title,
    {{ json_field('raw_payload', 'job_level') }}                            as job_level,
    {{ json_field('raw_payload', 'manager_worker_id') }}                    as manager_worker_id,
    {{ json_field('raw_payload', 'worker_type') }}                          as worker_type,
    {{ json_field_cast('raw_payload', 'fte', 'double') }}                   as fte,
    {{ json_field('raw_payload', 'location_city') }}                        as location_city,
    {{ json_field('raw_payload', 'location_region') }}                      as location_region,
    {{ json_field('raw_payload', 'location_country') }}                     as location_country,
    {{ json_field('raw_payload', 'event_type') }}                           as event_type,
    _source_updated_at                                                      as source_updated_at,
    _ingested_at_utc                                                        as ingested_at_utc,
    _run_id                                                                 as ingested_run_id
from latest
