-- Active headcount must never be negative.
select date_day, active_headcount
from {{ ref('mart_workforce_metrics_daily') }}
where active_headcount < 0
