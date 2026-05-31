-- Override dbt's default schema naming behaviour.
--
-- By default dbt concatenates the target schema with the custom schema:
--   target.schema = "dev"  +  custom = "staging"  →  "dev_staging"
--
-- That's annoying. This macro makes it use just the custom schema name directly:
--   custom = "staging"  →  "staging"
--   custom = "gold"     →  "gold"
--
-- If no custom schema is set, fall back to whatever the target schema is.

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
