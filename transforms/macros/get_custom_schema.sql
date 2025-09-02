
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name }}

    {%- endif -%}

{%- endmacro %}

{% macro create_schema(relation, schema_name) %}
    CREATE SCHEMA IF NOT EXISTS {{ schema_name }}
    LOCATION 's3a://warehouse/{{ schema_name }}/'
{% endmacro %}
