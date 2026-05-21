{{ config(materialized='view') }}

select * from {{ ref('base_ats__stage_events') }}
