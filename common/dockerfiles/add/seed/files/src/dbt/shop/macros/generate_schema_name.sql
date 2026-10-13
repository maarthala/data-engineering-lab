{#- Schemas are named exactly as +schema says (silver, gold). Without this macro dbt would
    prefix them with the target schema: silver_silver, silver_gold. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}
