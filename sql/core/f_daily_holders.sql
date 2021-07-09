{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

WITH holder_balance AS (
  SELECT d.dt
       , b.token_address
       , b.holder_address
       , b.ROLLING_BALANCE
       , b.next_dt

  FROM {{ dynamic_src("staging.stg_daily_token_holder_balance") }} b

  JOIN {{ dynamic_src("staging.stg_dates") }} d
   ON b.dt <= d.dt AND d.dt < b.next_dt
)

SELECT dt
     , token_address
     , COUNT(DISTINCT holder_address) AS n_holders
     , SUM(ROLLING_BALANCE) AS total_balance

FROM holder_balance

WHERE ROLLING_BALANCE > 0

GROUP BY 1,2
