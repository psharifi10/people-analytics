-- Custom test: every snapshot row must reference a real episode that
-- was active on that date.
select
    s.snapshot_date,
    s.employee_key
from {{ ref('fact_headcount_snapshot_daily') }} s
left join {{ ref('dim_employment_episode') }} e
    on s.employee_key = e.employee_key
   and s.snapshot_date >= e.hire_date
   and (e.termination_date is null or s.snapshot_date < e.termination_date)
where e.employment_episode_key is null
