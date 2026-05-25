{# Cross-database JSON field extraction.
   - DuckDB: json_extract_string(col, '$.field')
   - Snowflake: col:field::varchar
   Keeps base models portable across both targets. #}
{% macro json_field(column, path) -%}
    {%- if target.type == 'snowflake' -%}
        {{ column }}:{{ path }}::varchar
    {%- else -%}
        json_extract_string({{ column }}, '$.{{ path }}')
    {%- endif -%}
{%- endmacro %}

{# Cast variant: extracts then casts to a target type. #}
{% macro json_field_cast(column, path, cast_type) -%}
    {%- if target.type == 'snowflake' -%}
        {{ column }}:{{ path }}::{{ cast_type }}
    {%- else -%}
        cast(json_extract_string({{ column }}, '$.{{ path }}') as {{ cast_type }})
    {%- endif -%}
{%- endmacro %}

{# Try-cast variant: returns null on cast failure instead of error. #}
{% macro json_field_try_cast(column, path, cast_type) -%}
    {%- if target.type == 'snowflake' -%}
        try_cast({{ column }}:{{ path }}::varchar as {{ cast_type }})
    {%- else -%}
        try_cast(json_extract_string({{ column }}, '$.{{ path }}') as {{ cast_type }})
    {%- endif -%}
{%- endmacro %}
