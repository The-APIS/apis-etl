{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT token_address
     , to_address AS holder_address
     , sum(value_double) AS in_value
     , count(1) AS in_transactions

FROM {{ dynamic_src("staging.stg_ethereum_token_transfers_cast") }}

GROUP BY 1,2
