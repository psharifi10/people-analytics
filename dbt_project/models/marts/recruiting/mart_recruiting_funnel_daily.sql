{{ config(materialized='table') }}

-- Mart: daily recruiting funnel metrics.
-- Grain: one row per date x department x function.
-- Answers: how many applications, stage entries, offers, hires, rejections
-- occurred on each day? What are cumulative pipeline and conversion metrics?

with dates as (
    select date_day
    from {{ ref('dim_date') }}
),

-- Applications submitted per day.
app_events as (
    select
        cast(submitted_at as date)  as event_date,
        requisition_id,
        application_id,
        candidate_id,
        status,
        is_winning,
        days_in_funnel
    from {{ ref('fact_application') }}
),

-- Offers per day (by extended date).
offer_events as (
    select
        cast(extended_at as date)   as event_date,
        requisition_id,
        offer_id,
        status                      as offer_status,
        days_to_respond
    from {{ ref('fact_offer') }}
),

-- Accepted offers (hires) per day.
hire_events as (
    select
        cast(responded_at as date)  as event_date,
        requisition_id,
        offer_id
    from {{ ref('fact_offer') }}
    where status = 'accepted'
),

-- Requisition context for slicing.
reqs as (
    select
        requisition_id,
        department,
        function_name,
        job_level
    from {{ ref('dim_job_requisition') }}
),

-- Applications submitted aggregated.
apps_daily as (
    select
        ae.event_date,
        r.department,
        r.function_name,
        count(distinct ae.application_id)                                     as applications_submitted,
        count(distinct case when ae.status = 'hired' then ae.application_id end) as applications_hired,
        count(distinct case when ae.status = 'rejected' then ae.application_id end) as applications_rejected,
        avg(case when ae.days_in_funnel is not null then ae.days_in_funnel end) as avg_days_in_funnel
    from app_events ae
    join reqs r on ae.requisition_id = r.requisition_id
    group by 1, 2, 3
),

-- Offers extended aggregated.
offers_daily as (
    select
        oe.event_date,
        r.department,
        r.function_name,
        count(distinct oe.offer_id)                                           as offers_extended,
        count(distinct case when oe.offer_status = 'accepted' then oe.offer_id end) as offers_accepted,
        count(distinct case when oe.offer_status = 'declined' then oe.offer_id end) as offers_declined,
        avg(case when oe.days_to_respond is not null then oe.days_to_respond end) as avg_days_to_respond
    from offer_events oe
    join reqs r on oe.requisition_id = r.requisition_id
    group by 1, 2, 3
),

-- Hires (accepted offers) aggregated.
hires_daily as (
    select
        he.event_date,
        r.department,
        r.function_name,
        count(distinct he.offer_id) as hires
    from hire_events he
    join reqs r on he.requisition_id = r.requisition_id
    group by 1, 2, 3
),

-- Spine: every date x department x function that had any activity.
spine as (
    select distinct event_date, department, function_name from apps_daily
    union
    select distinct event_date, department, function_name from offers_daily
    union
    select distinct event_date, department, function_name from hires_daily
)

select
    s.event_date,
    s.department,
    s.function_name,
    coalesce(ad.applications_submitted, 0)  as applications_submitted,
    coalesce(ad.applications_hired, 0)      as applications_hired,
    coalesce(ad.applications_rejected, 0)   as applications_rejected,
    ad.avg_days_in_funnel,
    coalesce(od.offers_extended, 0)         as offers_extended,
    coalesce(od.offers_accepted, 0)         as offers_accepted,
    coalesce(od.offers_declined, 0)         as offers_declined,
    od.avg_days_to_respond,
    coalesce(hd.hires, 0)                  as hires,
    -- Offer acceptance rate (avoid div/0).
    case
        when coalesce(od.offers_extended, 0) > 0
        then round(
            coalesce(od.offers_accepted, 0) * 1.0 / od.offers_extended,
            3
        )
    end                                    as offer_acceptance_rate,
    -- Funnel conversion: hires / applications submitted.
    case
        when coalesce(ad.applications_submitted, 0) > 0
        then round(
            coalesce(hd.hires, 0) * 1.0 / ad.applications_submitted,
            4
        )
    end                                    as funnel_conversion_rate
from spine s
left join apps_daily ad
    on s.event_date = ad.event_date
    and s.department = ad.department
    and s.function_name = ad.function_name
left join offers_daily od
    on s.event_date = od.event_date
    and s.department = od.department
    and s.function_name = od.function_name
left join hires_daily hd
    on s.event_date = hd.event_date
    and s.department = hd.department
    and s.function_name = hd.function_name
