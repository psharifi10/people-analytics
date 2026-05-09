{{ config(materialized='table') }}

{# Single workforce reporting mart at daily grain.
   Combines snapshot-derived stocks (headcount, FTE) with event-derived
   flows (hires, terminations, promotions, transfers). #}

with snap as (
    select
        snapshot_date,
        count(*) as active_headcount,
        sum(fte) as total_fte,
        count(*) filter (where worker_type = 'full_time')   as full_time_headcount,
        count(*) filter (where worker_type = 'part_time')   as part_time_headcount,
        count(*) filter (where worker_type = 'contractor')  as contractor_headcount,
        count(*) filter (where worker_type = 'intern')      as intern_headcount,
        count(*) filter (where worker_type = 'contingent')  as contingent_headcount
    from {{ ref('fact_headcount_snapshot_daily') }}
    group by snapshot_date
),
ev as (
    select
        effective_date,
        count(*) filter (where event_type = 'hire')         as hires,
        count(*) filter (where event_type = 'rehire')       as rehires,
        count(*) filter (where event_type = 'termination')  as terminations,
        count(*) filter (where event_type = 'promotion')    as promotions,
        count(*) filter (where event_type = 'transfer')     as transfers
    from {{ ref('fact_employment_event') }}
    group by effective_date
)
select
    d.date_day,
    d.year_number,
    d.quarter_number,
    d.month_number,
    d.first_day_of_month,
    coalesce(s.active_headcount, 0)        as active_headcount,
    coalesce(s.total_fte, 0)               as total_fte,
    coalesce(s.full_time_headcount, 0)     as full_time_headcount,
    coalesce(s.part_time_headcount, 0)     as part_time_headcount,
    coalesce(s.contractor_headcount, 0)    as contractor_headcount,
    coalesce(s.intern_headcount, 0)        as intern_headcount,
    coalesce(s.contingent_headcount, 0)    as contingent_headcount,
    coalesce(e.hires, 0)                   as hires,
    coalesce(e.rehires, 0)                 as rehires,
    coalesce(e.terminations, 0)            as terminations,
    coalesce(e.promotions, 0)              as promotions,
    coalesce(e.transfers, 0)               as transfers,
    coalesce(e.hires, 0) + coalesce(e.rehires, 0)
        - coalesce(e.terminations, 0)      as net_headcount_change
from {{ ref('dim_date') }} d
left join snap s on s.snapshot_date = d.date_day
left join ev   e on e.effective_date = d.date_day
where d.date_day between cast('2022-01-01' as date) and current_date
order by d.date_day
