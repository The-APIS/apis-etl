{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

WITH transfer_out AS (
 SELECT token_address
      , from_address AS holder_address
      , TO_DATE(block_timestamp) AS dt
      , SUM(VALUE_DOUBLE) AS out_value

 FROM {{ dynamic_src("staging.stg_ethereum_token_transfers_cast") }}

 WHERE token_address IN (
   SELECT address
   FROM {{ dynamic_src("models.top_100_tokens") }}
 )

 GROUP BY 1,2,3
)

, transfer_in AS (
 SELECT token_address
      , to_address AS holder_address
      , TO_DATE(block_timestamp) AS dt
      , SUM(VALUE_DOUBLE) AS in_value

 FROM {{ dynamic_src("staging.stg_ethereum_token_transfers_cast") }}

 WHERE token_address IN (
   SELECT address
   FROM {{ dynamic_src("models.top_100_tokens") }}
 )

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
