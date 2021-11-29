{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT SUM(nft_price_xwg_double) AS total_dreamcard_volume_xwg
  FROM {{ dynamic_src("models.f_bsc_nft_sales") }}
