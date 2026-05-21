{{ config(materialized='view') }}

select * from {{ ref('base_ats__jobs') }}
