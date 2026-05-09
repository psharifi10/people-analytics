{# As-of join helper (see design doc §8.3). Filters an effective-dated
   history relation to the row valid on `as_of_date`. #}
{% macro as_of(history_relation, as_of_date) %}
select *
from {{ history_relation }}
where valid_from_date <= cast('{{ as_of_date }}' as date)
  and (valid_to_date is null or valid_to_date > cast('{{ as_of_date }}' as date))
{% endmacro %}
