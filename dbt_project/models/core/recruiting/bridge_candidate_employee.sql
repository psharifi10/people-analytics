{{ config(materialized='table') }}

-- Bridge: links ATS candidates to HRIS employees via person_key.
-- Only populated for candidates who were hired (accepted offer with a
-- resolved person_external_id). Enables hire-conversion analytics that
-- span both ATS and HRIS dimensions.

with accepted_offers as (
    select
        candidate_id,
        requisition_id,
        application_id,
        offer_id,
        accepted_person_external_id,
        responded_at as hired_at
    from {{ ref('stg_ats__offers') }}
    where status = 'accepted'
      and accepted_person_external_id is not null
),
-- Find the matching employee record (there may be multiple worker_ids per
-- person if rehired; take the one whose hire_date is closest to offer accept).
employees as (
    select
        worker_id,
        person_external_id,
        hire_date
    from {{ ref('stg_hris__workers') }}
),
bridged as (
    select
        ao.candidate_id,
        ao.requisition_id,
        ao.application_id,
        ao.offer_id,
        ao.accepted_person_external_id as person_external_id,
        ao.hired_at,
        e.worker_id,
        e.hire_date,
        row_number() over (
            partition by ao.offer_id
            order by abs(datediff('day', e.hire_date, cast(ao.hired_at as date)))
        ) as rn
    from accepted_offers ao
    left join employees e
        on ao.accepted_person_external_id = e.person_external_id
)
select
    {{ surrogate_key(['offer_id', 'worker_id']) }}              as candidate_employee_bridge_key,
    {{ surrogate_key(['candidate_id']) }}                        as candidate_key,
    {{ surrogate_key(['worker_id']) }}                           as employee_key,
    {{ surrogate_key(['person_external_id']) }}                  as person_key,
    {{ surrogate_key(['requisition_id']) }}                      as requisition_key,
    {{ surrogate_key(['application_id']) }}                      as application_key,
    candidate_id,
    worker_id,
    person_external_id,
    requisition_id,
    application_id,
    offer_id,
    hired_at,
    hire_date,
    -- Days between offer acceptance and HRIS hire date (data quality signal).
    datediff('day', cast(hired_at as date), hire_date)          as days_offer_to_hris_start,
    'offer_acceptance'                                          as match_method,
    1.0                                                         as match_confidence
from bridged
where rn = 1
