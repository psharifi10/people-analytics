{{ config(materialized='table') }}

{# An employment episode is a contiguous stint at the company. In our synth
   data each worker_id corresponds to exactly one episode; rehires create a
   second worker_id (and therefore a second episode) under the same person. #}

with workers as (
    select * from {{ ref('stg_hris__workers') }}
),
numbered as (
    select
        w.*,
        row_number() over (
            partition by person_external_id order by hire_date
        ) as episode_number
    from workers w
)
select
    {{ surrogate_key(['worker_id', 'hire_date']) }}    as employment_episode_key,
    {{ surrogate_key(['worker_id']) }}                 as employee_key,
    {{ surrogate_key(['person_external_id']) }}        as person_key,
    worker_id,
    person_external_id,
    episode_number,
    hire_date,
    termination_date,
    termination_reason,
    cast(
        coalesce(termination_date, current_date) - hire_date
    as integer)                                        as tenure_days,
    case when termination_date is null then true else false end as is_active_episode,
    ingested_run_id
from numbered
