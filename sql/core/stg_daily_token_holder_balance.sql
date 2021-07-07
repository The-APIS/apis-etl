{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

WITH tokens AS (
  SELECT address
       , 0 AS is_erc721
  FROM {{ dynamic_src("logs.top_500_tokens_erc20") }}

  UNION ALL

  SELECT address
       , 1 AS is_erc721
  FROM {{ dynamic_src("logs.top_500_tokens_erc721") }}

)

, transfer_out AS (
 SELECT c.token_address
      , c.from_address AS holder_address
      , TO_DATE(c.block_timestamp) AS dt
      , SUM(CASE WHEN t.is_erc721 = 1 THEN 1 ELSE c.value_double END) AS out_value

 FROM {{ dynamic_src("staging.stg_ethereum_token_transfers_cast") }} c

 INNER JOIN tokens t
  ON c.token_address = t.address

 GROUP BY 1,2,3
)

, transfer_in AS (
 SELECT c.token_address
      , c.to_address AS holder_address
      , TO_DATE(c.block_timestamp) AS dt
      , SUM(CASE WHEN t.is_erc721 = 1 THEN 1 ELSE c.value_double END) AS in_value

 FROM {{ dynamic_src("staging.stg_ethereum_token_transfers_cast") }} c

 INNER JOIN tokens t
  ON c.token_address = t.address

 GROUP BY 1,2,3
)

, b AS (
 SELECT token_address
      , holder_address
      , dt
      , out_value
      , 0 AS in_value
      , - out_value AS value
      , 1 AS is_out
      , 0 AS is_in

 FROM transfer_out

 UNION ALL

 SELECT token_address
      , holder_address
      , dt
      , 0 AS out_value
      , in_value
      , in_value AS value
      , 0 AS is_out
      , 1 AS is_in

 FROM transfer_in

)

SELECT DISTINCT token_address
     , holder_address
     , dt
     , SUM(value) OVER (PARTITION BY token_address, holder_address ORDER BY dt ASC) AS rolling_balance
     , LEAD(dt, 1, CURRENT_DATE) OVER (PARTITION BY token_address, holder_address ORDER BY dt ASC) AS next_dt

FROM b
