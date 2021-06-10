{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT COALESCE(t.symbol, t.address) AS symbol
     , t.name
     , t.block_timestamp
     , t.address
     , t.decimals
     , c.is_erc20
     , c.is_erc721
     , COUNT(DISTINCT h.holder_address) AS holders
     , SUM(h.balance) AS holder_value
     , SUM(h.total_transactions) AS total_transactions

FROM {{ dynamic_src("models.f_ethereum_holders") }} h

LEFT JOIN {{ dynamic_src("logs.bq_ethereum_tokens") }} t
  ON h.token_address = t.address

LEFT JOIN {{ dynamic_src("logs.bq_ethereum_contracts") }} c
  ON c.address = t.address

WHERE h.balance > 0

GROUP BY 1,2,3,4,5,6,7

ORDER BY 7 DESC
