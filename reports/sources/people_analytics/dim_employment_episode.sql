select
    employment_episode_key,
    employee_key,
    person_key,
    worker_id,
    episode_number,
    hire_date,
    termination_date,
    tenure_days,
    is_active_episode
from core.dim_employment_episode
