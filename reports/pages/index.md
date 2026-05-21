---
title: People Analytics
description: Workforce and recruiting analytics built on dbt + DuckDB
---

```sql headcount
select
    max(active_headcount) as current_headcount,
    max(total_fte) as current_fte,
    sum(hires) as total_hires,
    sum(terminations) as total_terms
from people_analytics.mart_workforce_metrics_daily
where date_day >= current_date - interval '365 days'
  and active_headcount > 0
```

```sql recruiting_summary
select
    sum(applications_submitted) as total_applications,
    sum(hires) as total_hires,
    round(avg(avg_days_in_funnel), 1) as avg_days_to_hire,
    round(avg(offer_acceptance_rate) * 100, 1) as offer_accept_pct
from people_analytics.mart_recruiting_funnel_daily
where event_date >= current_date - interval '365 days'
  and applications_submitted > 0
```

# People Analytics Platform

A complete workforce and recruiting analytics pipeline built with **Python extractors**, **dbt transformations**, and **DuckDB** as the analytical warehouse.

<BigValue
    data={headcount}
    value=current_headcount
    title="Active Headcount"
/>

<BigValue
    data={headcount}
    value=current_fte
    title="Total FTE"
    fmt="num1"
/>

<BigValue
    data={recruiting_summary}
    value=total_applications
    title="Applications (12mo)"
/>

<BigValue
    data={recruiting_summary}
    value=avg_days_to_hire
    title="Avg Days to Hire"
    fmt="num1"
/>

## Explore

- [Recruiting Funnel](/recruiting) - Application pipeline, conversion rates, time-to-hire
- [Workforce Overview](/workforce) - Headcount trends, hires, terminations, composition

---

*Data sourced from synthetic HRIS (Workday-shaped) and ATS (Ashby-shaped) extracts. Pipeline: raw landing with full lineage metadata, dbt transformations through base/staging/core/marts layers.*
