{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT token_address
     , from_address AS holder_address
     , sum(value_double) AS out_value
     , count(1) AS out_transactions

FROM {{ dynamic_src("staging.stg_ethereum_token_transfers_cast") }}

GROUP BY 1,2
