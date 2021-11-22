{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT token_address
     , holder_address
     , dt
     , rolling_balance
     , rolling_balance_xwg
     , percentage_distribution
     , next_dt
     , dt_rank
     , is_xwg_address
  FROM {{ dynamic_src("models.f_bsc_holder_balances") }}
