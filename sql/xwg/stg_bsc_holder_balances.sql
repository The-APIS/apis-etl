{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

WITH tokens AS (
  SELECT address
       , 0 AS is_erc721
  FROM {{ dynamic_src("staging.stg_bsc_holders_erc20_tokens") }}

  UNION ALL

  SELECT address
       , 1 AS is_erc721
  FROM {{ dynamic_src("staging.stg_bsc_holders_erc721_tokens") }}

)

, transfers AS (
  SELECT c.token_address
       , c.from_address
       , c.to_address
       , b."TIMESTAMP" AS block_timestamp
       , TRY_CAST(c."VALUE" AS DOUBLE) AS value_double
       , t.is_erc721
    FROM {{ dynamic_src("logs.bsc_token_transfers") }} c
    JOIN {{ dynamic_src("logs.bsc_blocks") }} b
      ON c.BLOCK_NUMBER = b."NUMBER"
   INNER JOIN tokens t
      ON t.address = c.token_address
  )

, transfer_out AS (
  SELECT token_address
       , from_address AS holder_address
       , TO_DATE(block_timestamp) AS dt
       , SUM(CASE WHEN is_erc721 = 1 THEN 1 ELSE value_double END) AS out_value
    FROM transfers
   GROUP BY 1,2,3
)

, transfer_in AS (
 SELECT token_address
      , to_address AS holder_address
      , TO_DATE(block_timestamp) AS dt
      , SUM(CASE WHEN is_erc721 = 1 THEN 1 ELSE value_double END) AS in_value
   FROM transfers
  GROUP BY 1,2,3
)

, bals AS (
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

, balances_without_aggregates AS (
	SELECT DISTINCT token_address
       , b.dt
       , b.holder_address
       , SUM("VALUE") OVER (PARTITION BY b.token_address, b.holder_address ORDER BY b.dt ASC) AS rolling_balance
    FROM bals b
)

, internal_xwg_addresses AS (
	SELECT address AS holder_address
       , 1 as is_xwg_address
    FROM {{ dynamic_src("staging.stg_bsc_xwg_addresses_to_exclude")}}
)

SELECT b.token_address
     , b.holder_address
     , b.dt
     , LEAD(b.dt, 1, CURRENT_DATE) OVER (PARTITION BY b.token_address, b.holder_address ORDER BY b.dt ASC) AS next_dt
     , ROW_NUMBER() OVER (PARTITION BY b.token_address, b.holder_address ORDER BY b.dt DESC) dt_rank
     , b.rolling_balance
     , TRY_CAST(b.rolling_balance AS DOUBLE) * POWER(10 * 1.000, -18) AS rolling_balance_xwg
     , CASE WHEN i.is_xwg_address IS NULL THEN 0 ELSE 1 END AS is_xwg_address
  FROM balances_without_aggregates b
  LEFT JOIN internal_xwg_addresses i
  ON b.holder_address = i.holder_address
