{{ config(materialized='table') }}

{# Daily headcount snapshot built by joining the date spine to the SCD2
   profile history (point-in-time). One row per (snapshot_date, employee).
   Filtered to the employment episode so terminated employees drop out. #}

with bounds as (
    select
        cast('2022-01-01' as date)                                    as start_date,
        cast(coalesce(max(termination_date), current_date) as date)   as end_date
    from {{ ref('dim_employment_episode') }}
),
dates as (
    select d.date_day
    from {{ ref('dim_date') }} d, bounds b
    where d.date_day between b.start_date and b.end_date
),
profile as (
    select * from {{ ref('dim_employee_profile_scd') }}
),
episode as (
    select * from {{ ref('dim_employment_episode') }}
)
select
    d.date_day                                       as snapshot_date,
    p.employee_key,
    p.employee_profile_key,
    e.employment_episode_key,
    p.worker_id,
    p.person_external_id,
    p.department,
    p.function_name,
    p.job_title,
    p.job_level,
    p.manager_worker_id,
    p.manager_employee_key,
    p.worker_type,
    p.fte,
    p.location_country,
    e.episode_number,
    e.hire_date,
    e.termination_date
from dates d
inner join profile p
    on d.date_day >= p.valid_from_date
   and (p.valid_to_date is null or d.date_day < p.valid_to_date)
inner join episode e
    on p.employee_key = e.employee_key
   and d.date_day >= e.hire_date
   and (e.termination_date is null or d.date_day < e.termination_date)
