{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

WITH dates AS (
SELECT dateadd(day, '-' || seq4(), to_date('2021-06-04')) as dt

FROM TABLE (generator(rowcount => 2048))

ORDER BY 1
)

SELECT dt

FROM dates
