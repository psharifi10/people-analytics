-- MetricFlow time spine: one row per day.
-- Required by the dbt semantic layer for time-series metric calculations.
-- Range: 2020-01-01 to 2030-12-31 (covers historical + forward-looking planning).

{{
  config(
    materialized='table',
  )
}}

{% if target.type == 'duckdb' %}

select
    cast(unnest(generate_series(
        date '2020-01-01',
        date '2030-12-31',
        interval '1 day'
    )) as date) as date_day

{% else %}

-- Snowflake: use generator + dateadd
select
    dateadd(day, seq4(), '2020-01-01'::date) as date_day
from table(generator(rowcount => 4018))

{% endif %}
