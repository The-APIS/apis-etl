{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT COUNT(DISTINCT holder_address) AS total_dreamcard_holders
  FROM {{ dynamic_src("models.f_bsc_holder_balances") }}
 WHERE token_address = '0xe6965b4f189dbdb2bd65e60abaeb531b6fe9580b'
   AND dt_rank = 1
   AND rolling_balance > 0
