{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

WITH nft AS (
     SELECT *
       FROM {{ dynamic_src("logs.bsc_token_transfers") }}
      INNER JOIN {{ dynamic_src("staging.stg_nft_sale_nft_tokens") }}
    ),

payment AS (
     SELECT *
       FROM {{ dynamic_src("logs.bsc_token_transfers") }}
      INNER JOIN {{ dynamic_src("staging.stg_nft_sale_payment_tokens") }}
    )

SELECT p.from_address AS buyer_address
     , n.from_address AS seller_address
     , p.token_address AS payment_token
     , p.value AS nft_price
     , TRY_CAST(p.value AS DOUBLE) * POWER(10 * 1.000, -18) AS nft_price_xwg_double
     , n.value AS nft_id
     , p.transaction_hash
     , p.block_number
     , b."TIMESTAMP"
  FROM nft n
  JOIN payment p
    ON n.transaction_hash = p.transaction_hash
  JOIN {{ dynamic_src("logs.bsc_blocks") }} b
    ON p.BLOCK_NUMBER = b."NUMBER";
