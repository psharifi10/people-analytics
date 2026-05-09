{{ config(materialized='table') }}

with spine as (
    select
        unnest(
            generate_series(
                cast('{{ var("date_spine_start") }}' as date),
                cast('{{ var("date_spine_end") }}' as date),
                interval 1 day
            )
        ) as date_day_ts
)
select
    cast(date_day_ts as date)                            as date_day,
    extract('year'    from date_day_ts)::int             as year_number,
    extract('quarter' from date_day_ts)::int             as quarter_number,
    extract('month'   from date_day_ts)::int             as month_number,
    strftime(date_day_ts, '%B')                          as month_name,
    extract('day'     from date_day_ts)::int             as day_of_month,
    extract('dow'     from date_day_ts)::int             as day_of_week,
    strftime(date_day_ts, '%A')                          as day_name,
    extract('week'    from date_day_ts)::int             as iso_week,
    case when extract('dow' from date_day_ts) in (0, 6)
         then false else true end                        as is_weekday,
    cast(date_trunc('month',   date_day_ts) as date)     as first_day_of_month,
    cast(date_trunc('quarter', date_day_ts) as date)     as first_day_of_quarter,
    cast(date_trunc('year',    date_day_ts) as date)     as first_day_of_year
from spine
