{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT h.token_address
     , h.holder_address
     , h.dt
     , h.rolling_balance
     , h.rolling_balance_xwg
     , 100 * DIV0(h.rolling_balance, s.circulating_supply) AS percentage_distribution
     , h.next_dt
     , h.dt_rank
     , h.is_xwg_address
  FROM {{ dynamic_src("staging.stg_holder_balances") }} h
  JOIN {{ dynamic_src("models.f_circulating_supply") }} s
    ON h.next_dt = s.dt AND h.token_address = s.token_address
