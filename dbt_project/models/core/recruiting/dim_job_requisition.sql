{{ config(materialized='table') }}

-- Job requisition dimension: one row per opening / requisition.
-- Status: open, filled, cancelled, unfilled.

with jobs as (
    select * from {{ ref('stg_ats__jobs') }}
)
select
    {{ surrogate_key(['requisition_id']) }}          as requisition_key,
    requisition_id,
    job_title,
    job_level,
    department,
    function_name,
    opened_at,
    closed_at,
    status,
    hired_worker_ids_json,
    -- Days open: null if still open, otherwise closed - opened.
    case
        when closed_at is not null
        then datediff('day', opened_at, closed_at)
    end                                             as days_open,
    source_updated_at,
    ingested_at_utc,
    ingested_run_id
from jobs
