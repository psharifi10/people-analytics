---
title: Workforce Overview
description: Headcount trends, hiring, terminations, and workforce composition
---

```sql headcount_trend
select
    date_day,
    active_headcount,
    total_fte
from people_analytics.mart_workforce_metrics_daily
where active_headcount > 0
  and date_day = first_day_of_month
order by date_day
```

```sql hires_terms_monthly
select
    first_day_of_month as month,
    sum(hires) as hires,
    sum(terminations) as terminations,
    sum(net_headcount_change) as net_change
from people_analytics.mart_workforce_metrics_daily
where active_headcount > 0
group by 1
order by 1
```

```sql composition
select
    max(full_time_headcount) as full_time,
    max(part_time_headcount) as part_time,
    max(contractor_headcount) as contractors,
    max(intern_headcount) as interns,
    max(contingent_headcount) as contingent
from people_analytics.mart_workforce_metrics_daily
where date_day = (select max(date_day) from people_analytics.mart_workforce_metrics_daily where active_headcount > 0)
```

```sql dept_headcount
select
    e.current_department as department,
    count(*) as headcount
from people_analytics.dim_employee e
join people_analytics.dim_employment_episode ep on e.employee_key = ep.employee_key
where ep.is_active_episode = true
group by 1
order by headcount desc
```

```sql events_monthly
select
    first_day_of_month as month,
    sum(promotions) as promotions,
    sum(transfers) as transfers
from people_analytics.mart_workforce_metrics_daily
where active_headcount > 0
group by 1
having sum(promotions) + sum(transfers) > 0
order by 1
```

```sql summary_stats
select
    max(active_headcount) as peak_headcount,
    sum(hires) as total_hires,
    sum(terminations) as total_terminations,
    sum(promotions) as total_promotions
from people_analytics.mart_workforce_metrics_daily
where active_headcount > 0
```

# Workforce Overview

<BigValue
    data={summary_stats}
    value=peak_headcount
    title="Peak Headcount"
/>

<BigValue
    data={summary_stats}
    value=total_hires
    title="Total Hires"
/>

<BigValue
    data={summary_stats}
    value=total_terminations
    title="Total Terminations"
/>

<BigValue
    data={summary_stats}
    value=total_promotions
    title="Total Promotions"
/>

## Headcount Trend

<LineChart
    data={headcount_trend}
    x=date_day
    y={['active_headcount', 'total_fte']}
    title="Active Headcount and FTE (monthly snapshots)"
    yAxisTitle="Count"
/>

## Hires vs Terminations

<BarChart
    data={hires_terms_monthly}
    x=month
    y={['hires', 'terminations']}
    type=grouped
    title="Monthly Hires and Terminations"
    yAxisTitle="Count"
/>

## Net Headcount Change

<BarChart
    data={hires_terms_monthly}
    x=month
    y=net_change
    title="Net Headcount Change by Month"
    yAxisTitle="Net Change"
/>

## Workforce Composition

<BigValue
    data={composition}
    value=full_time
    title="Full-Time"
/>

<BigValue
    data={composition}
    value=part_time
    title="Part-Time"
/>

<BigValue
    data={composition}
    value=contractors
    title="Contractors"
/>

<BigValue
    data={composition}
    value=interns
    title="Interns"
/>

## Headcount by Department

<BarChart
    data={dept_headcount}
    x=department
    y=headcount
    title="Current Headcount by Department"
    swapXY=true
/>

## Promotions and Transfers

<LineChart
    data={events_monthly}
    x=month
    y={['promotions', 'transfers']}
    title="Monthly Promotions and Transfers"
    yAxisTitle="Count"
/>
