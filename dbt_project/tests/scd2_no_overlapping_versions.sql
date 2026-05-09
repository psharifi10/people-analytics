-- Custom test: SCD2 history must have no overlapping validity ranges per employee.
{{ config(severity='error') }}

select
    a.employee_key,
    a.employee_profile_key as profile_a,
    b.employee_profile_key as profile_b,
    a.valid_from_date as a_from,
    a.valid_to_date as a_to,
    b.valid_from_date as b_from,
    b.valid_to_date as b_to
from {{ ref('dim_employee_profile_scd') }} a
inner join {{ ref('dim_employee_profile_scd') }} b
    on a.employee_key = b.employee_key
   and a.employee_profile_key < b.employee_profile_key
where a.valid_from_date < coalesce(b.valid_to_date, cast('9999-12-31' as date))
  and coalesce(a.valid_to_date, cast('9999-12-31' as date)) > b.valid_from_date
