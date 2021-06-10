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

, pre_final AS (
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
)

SELECT pf.token_address
     , t.name AS token_name
     , pf.holder_address
     , pf.out_value
     , pf.out_transactions
     , pf.in_value
     , pf.in_transactions
     , pf.total_transactions
     , pf.balance
     , SUM(pf.balance) OVER (PARTITION BY pf.token_address) AS token_supply
     , pf.balance/(CASE WHEN token_supply = 0 THEN 1 ELSE token_supply END) AS holder_balance_perc
     , ROW_NUMBER() OVER (PARTITION BY pf.token_address ORDER BY pf.balance DESC) AS ranking

FROM pre_final pf

LEFT JOIN {{ dynamic_src("logs.bq_ethereum_tokens") }} t
  ON pf.token_address = t.address

WHERE pf.balance > 0
