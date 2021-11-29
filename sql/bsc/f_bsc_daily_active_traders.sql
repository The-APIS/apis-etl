{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

WITH active_traders AS (
	SELECT to_date("TIMESTAMP") AS dt
	     , buyer_address AS trader_address
	  FROM {{ dynamic_src("models.f_bsc_nft_sales") }}

	UNION ALL

	SELECT to_date("TIMESTAMP") AS dt
	     , seller_address AS trader_address
	  FROM {{ dynamic_src("models.f_bsc_nft_sales") }}
)

SELECT dt
     , COUNT(DISTINCT trader_address) AS traders_count
  FROM active_traders
 GROUP BY 1
 ORDER BY 1
