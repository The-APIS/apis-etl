{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT dt
     , token_address
     , n_holders
     , total_balance

FROM {{ dynamic_src("models.f_daily_holders") }}
