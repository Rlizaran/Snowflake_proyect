-- macros/generate_database_name.sql
-- nombre EXACTO obligatorio

{% macro generate_database_name(
    custom_database_name, node
) -%}

{%- if custom_database_name is not none -%}
        {{ custom_database_name | trim }}
    {%- else -%}
        {{ target.database | trim }}
    {%- endif -%}

{%- endmacro %}