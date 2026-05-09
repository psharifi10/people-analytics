{{ config(materialized='table') }}

{# Manager-chain flatten via recursive CTE, see design doc §7.4.
   Produces (snapshot_date, employee, ancestor_employee, depth) tuples
   for span-of-control / org-rollup analytics. Capped at 20 levels. #}

with recursive headcount as (
    select * from {{ ref('fact_headcount_snapshot_daily') }}
),
chain as (
    select
        snapshot_date,
        employee_key,
        manager_employee_key as ancestor_employee_key,
        1 as depth
    from headcount
    where manager_employee_key is not null

    union all

    select
        c.snapshot_date,
        c.employee_key,
        h.manager_employee_key as ancestor_employee_key,
        c.depth + 1            as depth
    from chain c
    join headcount h
        on c.ancestor_employee_key = h.employee_key
       and c.snapshot_date = h.snapshot_date
    where h.manager_employee_key is not null
      and c.depth < 20
)
select * from chain
