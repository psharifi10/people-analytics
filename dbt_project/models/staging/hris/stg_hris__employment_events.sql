{{ config(materialized='view') }}

select * from {{ ref('base_hris__employment_events') }}
