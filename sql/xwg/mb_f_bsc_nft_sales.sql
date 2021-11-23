{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT buyer_address
     , seller_address
     , payment_token
     , nft_price
     , nft_price_xwg_double
     , nft_id
     , transaction_hash
     , block_number
     , "TIMESTAMP"
  FROM {{ dynamic_src("models.f_bsc_nft_sales") }}
