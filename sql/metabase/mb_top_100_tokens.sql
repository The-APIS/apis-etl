{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT address
     , ranking AS etherscan_ranking

FROM {{ dynamic_src("models.top_100_tokens") }}
