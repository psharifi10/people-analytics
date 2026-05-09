{{ config(materialized='view') }}

select * from {{ ref('base_hris__workers') }}
