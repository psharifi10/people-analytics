select
    stage_event_id,
    application_id,
    candidate_id,
    requisition_id,
    from_stage,
    to_stage,
    transitioned_at,
    next_transition_at,
    hours_in_stage
from core.fact_application_stage_history
