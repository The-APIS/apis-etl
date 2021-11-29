{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT dt
     , traders_count
  FROM {{ dynamic_src("models.f_bsc_daily_active_traders") }}
