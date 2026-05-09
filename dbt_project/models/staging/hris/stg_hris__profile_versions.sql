{{ config(materialized='view') }}

with v as (
    select * from {{ ref('base_hris__profile_versions') }}
),
deduped as (
    select *,
        row_number() over (
            partition by worker_id, effective_date
            order by source_updated_at desc nulls last, ingested_at_utc desc
        ) as rn
    from v
)
select
    profile_version_id,
    worker_id,
    person_external_id,
    effective_date,
    department,
    function_name,
    job_title,
    job_level,
    manager_worker_id,
    worker_type,
    fte,
    location_city,
    location_region,
    location_country,
    event_type,
    source_updated_at,
    ingested_at_utc,
    ingested_run_id
from deduped
where rn = 1
