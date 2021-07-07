{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT COALESCE(t.symbol, h.token_address) AS symbol
     , h.token_address AS address
     , t.name
     , COALESCE(t.block_timestamp, c.block_timestamp) AS block_timestamp
     , t.decimals
     , c.is_erc20
     , c.is_erc721
     , r.ranking AS etherscan_erc20_ranking
     , rc.ranking AS etherscan_erc721_ranking
     , COUNT(DISTINCT h.holder_address) AS holders
     , SUM(h.balance) AS holder_value
     , SUM(h.total_transactions) AS total_transactions

FROM {{ dynamic_src("models.f_ethereum_holders") }} h

LEFT JOIN {{ dynamic_src("logs.bq_ethereum_tokens") }} t
  ON h.token_address = t.address

LEFT JOIN {{ dynamic_src("logs.bq_ethereum_contracts") }} c
  ON h.token_address = c.address

LEFT JOIN {{ dynamic_src("logs.top_500_tokens_erc20") }} r
  ON h.token_address = r.address

LEFT JOIN {{ dynamic_src("logs.top_500_tokens_erc721") }} rc
  ON h.token_address = rc.address

WHERE h.balance > 0

GROUP BY 1,2,3,4,5,6,7,8,9

ORDER BY 7 DESC
