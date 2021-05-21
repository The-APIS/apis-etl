{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT *

FROM {{ dynamic_src("logs.ethereum_methods") }}

LIMIT 1
