{# Hash-based surrogate key. Lightweight stand-in for dbt_utils.generate_surrogate_key
   so the project has zero external package dependencies. #}
{% macro surrogate_key(field_list) -%}
    md5(
        concat_ws(
            '|',
            {%- for f in field_list %}
            coalesce(cast({{ f }} as varchar), '_NULL_'){% if not loop.last %},{% endif %}
            {%- endfor %}
        )
    )
{%- endmacro %}
