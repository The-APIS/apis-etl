{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT d.dt
     , b.token_address
     , SUM(b.rolling_balance) AS circulating_supply
     , circulating_supply * power(10 * 1.000, -18) AS circulating_supply_xwg

  FROM {{ dynamic_src("staging.stg_bsc_holder_balances") }} b

 RIGHT JOIN {{ dynamic_src("staging.stg_dates") }} d
    ON b.dt < d.dt AND d.dt <= b.next_dt

 WHERE rolling_balance > 0
   AND is_xwg_address = 0

 GROUP BY 1, 2
