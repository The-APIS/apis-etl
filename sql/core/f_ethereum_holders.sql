{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

WITH holder_ledger AS (
  SELECT token_address
       , holder_address
       , out_value
       , out_transactions
       , 0 AS in_value
       , 0 AS in_transactions
       , - out_value AS value
  FROM {{ dynamic_src("staging.stg_ethereum_token_transfer_out") }}

  UNION ALL

  SELECT token_address
       , holder_address
       , 0 AS out_value
       , 0 AS out_transactions
       , in_value
       , in_transactions
       , in_value AS value
  FROM {{ dynamic_src("staging.stg_ethereum_token_transfer_in") }}
)

SELECT token_address
     , holder_address
     , SUM(out_value) AS out_value
     , SUM(out_transactions) AS out_transactions
     , SUM(in_value) AS in_value
     , SUM(in_transactions) AS in_transactions
     , SUM(out_transactions) + SUM(in_transactions) AS total_transactions
     , SUM(value) AS balance

FROM holder_ledger

GROUP BY 1,2
