{#
  Map dbt custom schemas directly (no public_ prefix):
  staging models -> schema "silver", marts -> schema "gold", sources -> "bronze".
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name }}
    {%- endif -%}
{%- endmacro %}