{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT d.dt
     , b.token_address
	   , COUNT(DISTINCT b.holder_address) AS holders_count
  FROM {{ dynamic_src("models.f_holder_balances") }}  b
  JOIN {{ dynamic_src("staging.stg_dates") }} d
    ON b.dt < d.dt AND d.dt <= b.next_dt
 WHERE rolling_balance > 0
 GROUP BY d.dt, b.token_address
 ORDER BY dt DESC;
