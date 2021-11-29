{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT SUM(rolling_balance_xwg) AS token_distribution
  FROM {{ dynamic_src("models.f_bsc_holder_balances") }}
 WHERE token_address = '0x6b23c89196deb721e6fd9726e6c76e4810a464bc'
   AND dt_rank = 1
   AND rolling_balance > 0
   AND is_xwg_address = 0
