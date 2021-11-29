{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT dt
     , token_address
     , circulating_supply
     , circulating_supply_xwg

  FROM {{ dynamic_src("models.f_bsc_circulating_supply") }}
