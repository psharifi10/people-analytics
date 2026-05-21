select
    application_key,
    candidate_key,
    requisition_key,
    application_id,
    candidate_id,
    requisition_id,
    submitted_at,
    current_stage,
    status,
    application_source,
    is_winning,
    closed_at,
    days_in_funnel
from core.fact_application
