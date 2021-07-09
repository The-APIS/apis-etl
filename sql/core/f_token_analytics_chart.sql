{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT token_address
     , TO_DATE(block_timestamp) AS transfer_dt
     , COUNT(1) AS n_transfers
     , COUNT(DISTINCT from_address) AS unique_senders
     , COUNT(DISTINCT to_address) AS unique_recievers
     , SUM(value_double) AS total_value

FROM {{ dynamic_src("staging.stg_ethereum_token_transfers_cast") }}

GROUP BY 1,2

ORDER BY 1,2
