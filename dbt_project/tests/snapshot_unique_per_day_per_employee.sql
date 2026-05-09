-- Replacement for dbt_utils.unique_combination_of_columns.
select snapshot_date, employee_key, count(*) as n
from {{ ref('fact_headcount_snapshot_daily') }}
group by snapshot_date, employee_key
having count(*) > 1
