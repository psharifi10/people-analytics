select
    employee_key,
    person_key,
    worker_id,
    first_name,
    last_name,
    hire_date,
    termination_date,
    is_currently_employed,
    current_department,
    current_function,
    current_job_title,
    current_job_level,
    current_worker_type,
    current_fte
from core.dim_employee
