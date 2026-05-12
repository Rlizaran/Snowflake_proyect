-- Macro: layout de schema segun DBT_ENVIRONMENTS. DEV={target.schema}_{custom} (sandbox); PRO={custom} (limpio).

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set env = env_var('DBT_ENVIRONMENTS', 'DEV') | upper -%}
    {%- if env == 'PRO' -%}
        {%- if custom_schema_name is none -%}
            {{ target.schema | trim }}
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}
    {%- else -%}
        {%- if custom_schema_name is none -%}
            {{ target.schema | trim }}
        {%- else -%}
            {{ target.schema | trim }}_{{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
