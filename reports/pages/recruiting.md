---
title: Recruiting Funnel
description: Application pipeline, stage conversion, and time-to-hire metrics
---

```sql funnel_stages
select
    to_stage as stage,
    count(*) as candidates
from core.fact_application_stage_history
group by 1
order by
    case to_stage
        when 'applied' then 1
        when 'recruiter_screen' then 2
        when 'hiring_manager_screen' then 3
        when 'technical_interview' then 4
        when 'final_round' then 5
        when 'offer' then 6
        when 'hired' then 7
    end
```

```sql monthly_applications
select
    date_trunc('month', event_date) as month,
    sum(applications_submitted) as applications,
    sum(hires) as hires,
    sum(offers_extended) as offers
from marts.mart_recruiting_funnel_daily
group by 1
having sum(applications_submitted) > 0
order by 1
```

```sql pipeline_by_dept
select
    department,
    sum(applications_submitted) as applications,
    sum(hires) as hires,
    sum(offers_extended) as offers,
    round(avg(avg_days_in_funnel), 1) as avg_days
from marts.mart_recruiting_funnel_daily
where applications_submitted > 0
group by 1
order by applications desc
```

```sql conversion_rates
select
    date_trunc('month', event_date) as month,
    round(sum(hires) * 100.0 / nullif(sum(applications_submitted), 0), 1) as hire_rate_pct,
    round(sum(offers_accepted) * 100.0 / nullif(sum(offers_extended), 0), 1) as offer_accept_pct
from marts.mart_recruiting_funnel_daily
where applications_submitted > 0
group by 1
order by 1
```

```sql time_to_hire_trend
select
    date_trunc('month', event_date) as month,
    round(avg(avg_days_in_funnel), 1) as avg_days_to_hire
from marts.mart_recruiting_funnel_daily
where avg_days_in_funnel is not null
group by 1
order by 1
```

```sql top_reqs
select
    r.title as requisition,
    r.department,
    r.status,
    count(distinct a.application_id) as applications,
    count(distinct case when a.current_stage = 'hired' then a.application_id end) as hires
from core.fact_application a
join core.dim_job_requisition r on a.job_requisition_id = r.job_requisition_id
group by 1, 2, 3
order by applications desc
limit 10
```

# Recruiting Funnel

## Stage Conversion

<FunnelChart
    data={funnel_stages}
    nameCol=stage
    valueCol=candidates
    showPercent=true
    title="Candidate Funnel (all time)"
/>

## Monthly Activity

<LineChart
    data={monthly_applications}
    x=month
    y={['applications', 'offers', 'hires']}
    title="Applications, Offers, and Hires by Month"
    yAxisTitle="Count"
/>

## Conversion Rates Over Time

<LineChart
    data={conversion_rates}
    x=month
    y={['hire_rate_pct', 'offer_accept_pct']}
    title="Hire Rate and Offer Acceptance Rate (%)"
    yAxisTitle="Percent"
/>

## Average Time to Hire

<LineChart
    data={time_to_hire_trend}
    x=month
    y=avg_days_to_hire
    title="Average Days in Funnel by Month"
    yAxisTitle="Days"
/>

## Pipeline by Department

<BarChart
    data={pipeline_by_dept}
    x=department
    y={['applications', 'offers', 'hires']}
    type=grouped
    title="Recruiting Volume by Department"
    swapXY=true
/>

## Top Requisitions by Volume

<DataTable
    data={top_reqs}
    rows=10
/>
