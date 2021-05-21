{#
This macro enables to dynamically select a source from production or from the parameter settings.
The table parameter should be composed as schema_key.table_name. schema_key corresponds to the second part of the schema name (i.e. logs, staging, etc.).
It relies on a from_prod parameter which is a list of such table names. When the table parameter is in this from_prod list, the compilation will return the prod location.
#}

{%- macro dynamic_src(table) -%}
{%- set schema_key = table.split('.')[0] -%}
{%- set table_name = table.split('.')[1] -%}
{%- set schema_std = schema[schema_key] -%}
{%- set in_from_prod = False if from_prod is none else table in from_prod -%}
{{ 'analytics_' + schema_key if in_from_prod else schema_std }}.{{ '' if in_from_prod else user_prefix }}{{ table_name }}
{%- endmacro -%}
