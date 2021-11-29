{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT SUM(supply_delta) AS token_distribution
     , '1w' AS date_range
  FROM {{ dynamic_src("staging.stg_xwg_daily_token_supply_delta") }}
 WHERE dt >= DATEADD(week, -1, CURRENT_DATE)

UNION

SELECT SUM(supply_delta) AS token_distribution
     , '1m' AS start_of_date_range
  FROM {{ dynamic_src("staging.stg_xwg_daily_token_supply_delta") }}
 WHERE dt >= DATEADD(month, -1, CURRENT_DATE)

UNION

SELECT SUM(supply_delta) AS token_distribution
     , '3m' AS start_of_date_range
  FROM {{ dynamic_src("staging.stg_xwg_daily_token_supply_delta") }}
 WHERE dt >= DATEADD(month, -3, CURRENT_DATE)

UNION

SELECT SUM(supply_delta) AS token_distribution
     , '1y' AS start_of_date_range
  FROM {{ dynamic_src("staging.stg_xwg_daily_token_supply_delta") }}
 WHERE dt >= DATEADD(year, -1, CURRENT_DATE)

UNION

SELECT token_distribution
     , 'all-time' AS start_of_date_range
  FROM {{ dynamic_src("models.f_xwg_token_distribution_all_time") }}
