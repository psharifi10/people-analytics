{{ config(materialized='view') }}

with src as (
    select *
    from {{ source('raw_hris', 'hris_workday__workers') }}
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
    _source_record_id                                                        as worker_id,
    {{ json_field('raw_payload', 'person_external_id') }}                    as person_external_id,
    {{ json_field('raw_payload', 'first_name') }}                            as first_name,
    {{ json_field('raw_payload', 'last_name') }}                             as last_name,
    {{ json_field('raw_payload', 'work_email') }}                            as work_email,
    {{ json_field('raw_payload', 'personal_email') }}                        as personal_email,
    {{ json_field_cast('raw_payload', 'hire_date', 'date') }}                as hire_date,
    {{ json_field_try_cast('raw_payload', 'termination_date', 'date') }}     as termination_date,
    {{ json_field('raw_payload', 'termination_reason') }}                    as termination_reason,
    {{ json_field('raw_payload', 'current_department') }}                    as current_department,
    {{ json_field('raw_payload', 'current_function') }}                      as current_function,
    {{ json_field('raw_payload', 'current_job_title') }}                     as current_job_title,
    {{ json_field('raw_payload', 'current_job_level') }}                     as current_job_level,
    {{ json_field('raw_payload', 'current_manager_worker_id') }}             as current_manager_worker_id,
    {{ json_field('raw_payload', 'current_worker_type') }}                   as current_worker_type,
    {{ json_field_cast('raw_payload', 'current_fte', 'double') }}            as current_fte,
    {{ json_field('raw_payload', 'current_location_city') }}                 as current_location_city,
    {{ json_field('raw_payload', 'current_location_region') }}               as current_location_region,
    {{ json_field('raw_payload', 'current_location_country') }}              as current_location_country,
    {{ json_field_cast('raw_payload', 'is_active', 'boolean') }}             as is_active_in_source,
    _source_updated_at                                                       as source_updated_at,
    _ingested_at_utc                                                         as ingested_at_utc,
    _run_id                                                                  as ingested_run_id
from latest
