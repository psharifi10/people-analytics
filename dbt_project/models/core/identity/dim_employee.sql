{{ config(materialized='table') }}

with workers as (
    select * from {{ ref('stg_hris__workers') }}
)
select
    {{ surrogate_key(['worker_id']) }}             as employee_key,
    {{ surrogate_key(['person_external_id']) }}    as person_key,
    worker_id,
    person_external_id,
    first_name,
    last_name,
    work_email,
    hire_date,
    termination_date,
    termination_reason,
    case when termination_date is null then true else false end as is_currently_employed,
    current_department,
    current_function,
    current_job_title,
    current_job_level,
    current_manager_worker_id,
    case when current_manager_worker_id is not null
         then {{ surrogate_key(['current_manager_worker_id']) }}
    end                                                      as current_manager_employee_key,
    current_worker_type,
    current_fte,
    current_location_city,
    current_location_region,
    current_location_country,
    source_updated_at,
    ingested_at_utc,
    ingested_run_id
from workers
