{% macro generate_schema_name(custom_schema_name, node) -%}

    {#
        Override dbt's default schema naming behaviour.

        Default dbt behaviour appends the target schema as a prefix:
            dev_staging, dev_intermediate, dev_marts

        This macro produces clean schema names without the target prefix
        in dev, matching the production schema structure:
            staging, intermediate, marts, raw

        Usage:
        - Set schema in dbt_project.yml (e.g. +schema: staging)
        - This macro ensures the final schema is exactly 'staging' in all targets
    #}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
