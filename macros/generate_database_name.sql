-- Macro: override del default dbt para usar el nombre EXACTO de database (no prefijar con target).

{% macro generate_database_name(custom_database_name, node) -%}
    {%- if custom_database_name is not none -%}
        {{ custom_database_name | trim }}
    {%- else -%}
        {{ target.database | trim }}
    {%- endif -%}
{%- endmacro %}
