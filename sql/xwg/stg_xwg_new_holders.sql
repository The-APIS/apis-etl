{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

WITH earliest_purchase AS (
  SELECT buyer_address
       , MIN("TIMESTAMP") AS ts
    FROM {{ dynamic_src("models.f_bsc_nft_sales") }}
   GROUP BY 1
)

SELECT n.buyer_address as holder_address
     , n.nft_price_xwg_double AS price_paid
     , to_date(p.ts) as dt
  FROM earliest_purchase p
  JOIN {{ dynamic_src("models.f_bsc_nft_sales") }} n
    ON p.buyer_address = n.buyer_address
       AND p.ts = n."TIMESTAMP"
