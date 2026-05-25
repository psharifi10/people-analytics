{{ config(materialized='table') }}

with events as (
    select * from {{ ref('stg_hris__employment_events') }}
)
select
    {{ surrogate_key(['event_id']) }}              as employment_event_key,
    {{ surrogate_key(['worker_id']) }}             as employee_key,
    {{ surrogate_key(['person_external_id']) }}    as person_key,
    event_id,
    worker_id,
    person_external_id,
    event_type,
    effective_date,
    department,
    function_name,
    job_title,
    job_level,
    manager_worker_id,
    case when manager_worker_id is not null
         then {{ surrogate_key(['manager_worker_id']) }}
    end                                                  as manager_employee_key,
    termination_reason,
    source_updated_at                              as recorded_at_utc,
    ingested_run_id
from events
