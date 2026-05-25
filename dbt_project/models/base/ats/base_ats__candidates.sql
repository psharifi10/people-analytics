{{ config(materialized='view') }}

with src as (
    select *
    from {{ source('raw_ats', 'ats_ashby__candidates') }}
),
ranked as (
    select *,
        row_number() over (
            partition by _source_record_id
            order by _source_updated_at desc nulls last, _ingested_at_utc desc
        ) as rn
    from src
),
latest as (
    select * from ranked where rn = 1
)
select
    _source_record_id                                                     as candidate_id,
    {{ json_field('raw_payload', 'first_name') }}                         as first_name,
    {{ json_field('raw_payload', 'last_name') }}                          as last_name,
    {{ json_field('raw_payload', 'email') }}                              as email,
    {{ json_field('raw_payload', 'linkedin_url') }}                       as linkedin_url,
    {{ json_field('raw_payload', 'source') }}                             as candidate_source,
    {{ json_field('raw_payload', 'person_external_id') }}                 as person_external_id,
    _source_updated_at                                                    as source_updated_at,
    _ingested_at_utc                                                      as ingested_at_utc,
    _run_id                                                               as ingested_run_id
from latest
