{{ config(materialized='table') }}

with persons as (
    select * from {{ ref('stg_hris__persons') }}
)
select
    {{ surrogate_key(['person_external_id']) }} as person_key,
    person_external_id,
    first_name,
    last_name,
    personal_email,
    worker_ids_json,
    source_updated_at,
    ingested_at_utc,
    ingested_run_id
from persons
