select
    requisition_key,
    requisition_id,
    job_title,
    job_level,
    department,
    function_name,
    opened_at,
    closed_at,
    status,
    days_open
from core.dim_job_requisition
