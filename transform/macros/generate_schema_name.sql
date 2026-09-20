{#
  dbt's default prepends the target schema to custom ones, yielding
  `main_silver` / `main_gold`. The medallion layer names are meaningful on
  their own, and they are what downstream consumers will write in queries, so
  use them verbatim.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
