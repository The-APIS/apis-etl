{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT dt
     , token_address
	   , holders_count
  FROM {{ dynamic_src("models.f_bsc_daily_holders_count") }}
