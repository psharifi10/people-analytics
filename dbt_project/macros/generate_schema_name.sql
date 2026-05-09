{# Make schemas literal (no target-schema prefix) so we get clean
   `base`, `staging`, `core`, `marts` schemas instead of `main_base` etc. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
