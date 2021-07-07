{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT symbol
     , name
     , block_timestamp
     , address
     , is_erc20
     , is_erc721
     , holders
     , holder_value
     , total_transactions
     , decimals
     , etherscan_erc20_ranking
     , etherscan_erc721_ranking

FROM {{ dynamic_src("models.f_ethereum_token_summary") }}
