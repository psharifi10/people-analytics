{{ config(materialized='table') }}

{# SCD Type 2 driven by effective_date (NOT by source_updated_at).
   See design doc §8.1, this is the single most important non-obvious
   modelling choice in the platform. #}

with v as (
    select * from {{ ref('stg_hris__profile_versions') }}
),
ranked as (
    select *,
        lead(effective_date) over (
            partition by worker_id order by effective_date
        ) as next_effective_date,
        row_number() over (
            partition by worker_id order by effective_date
        ) as version_number
    from v
)
select
    {{ surrogate_key(['worker_id', 'effective_date']) }} as employee_profile_key,
    {{ surrogate_key(['worker_id']) }}                   as employee_key,
    worker_id,
    person_external_id,
    version_number,
    effective_date                                       as valid_from_date,
    next_effective_date                                  as valid_to_date,
    case when next_effective_date is null
         then true else false end                        as is_current,
    department,
    function_name,
    job_title,
    job_level,
    manager_worker_id,
    {{ surrogate_key(['manager_worker_id']) }}           as manager_employee_key,
    worker_type,
    fte,
    location_city,
    location_region,
    location_country,
    event_type                                           as version_event_type,
    source_updated_at                                    as recorded_at_utc,
    ingested_run_id
from ranked
