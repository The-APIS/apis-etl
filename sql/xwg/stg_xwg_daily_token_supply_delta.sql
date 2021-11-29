{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT dt
     , circulating_supply_xwg
     , circulating_supply_xwg
       - LAG(circulating_supply_xwg) OVER(ORDER BY dt ASC) AS supply_delta
  FROM {{ dynamic_src("models.f_bsc_circulating_supply") }}
 WHERE token_address = '0x6b23c89196deb721e6fd9726e6c76e4810a464bc'
