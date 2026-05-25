-- Custom test: SCD2 grain must be unique on (worker_id, effective_date).
-- A duplicate (worker_id, effective_date) pair means the staging dedup failed
-- or a source correction wasn't properly coalesced.
{{ config(severity='error') }}

select
    worker_id,
    valid_from_date,
    count(*) as row_count
from {{ ref('dim_employee_profile_scd') }}
group by worker_id, valid_from_date
having count(*) > 1
