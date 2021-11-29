{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT COUNT(holder_address) AS new_holders
     , '1w' AS date_range
  FROM {{ dynamic_src("staging.stg_xwg_new_holders") }}
 WHERE dt >= DATEADD(week, -1, CURRENT_DATE)

UNION

SELECT COUNT(holder_address) AS new_holders
     , '1m' AS start_of_date_range
  FROM {{ dynamic_src("staging.stg_xwg_new_holders") }}
 WHERE dt >= DATEADD(month, -1, CURRENT_DATE)

UNION

SELECT COUNT(holder_address) AS new_holders
     , '3m' AS start_of_date_range
  FROM {{ dynamic_src("staging.stg_xwg_new_holders") }}
 WHERE dt >= DATEADD(month, -3, CURRENT_DATE)

UNION

SELECT COUNT(holder_address) AS new_holders
     , '1y' AS start_of_date_range
  FROM {{ dynamic_src("staging.stg_xwg_new_holders") }}
 WHERE dt >= DATEADD(year, -1, CURRENT_DATE)

UNION

SELECT COUNT(holder_address) AS new_holders
     , 'all-time' AS start_of_date_range
  FROM {{ dynamic_src("staging.stg_xwg_new_holders") }}
