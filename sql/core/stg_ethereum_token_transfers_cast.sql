{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT token_address
     , from_address
     , to_address
     , transaction_hash
     , log_index
     , block_timestamp
     , block_number
     , block_hash
     , value AS value_str
     , TRY_CAST(value AS double) AS value_double

FROM {{ dynamic_src("logs.bq_ethereum_token_transfers") }}
