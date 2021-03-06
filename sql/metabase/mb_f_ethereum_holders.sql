{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT token_address
     , holder_address
     , out_value
     , out_transactions
     , in_value
     , in_transactions
     , total_transactions
     , balance
     , token_name
     , holder_balance_perc
     , ranking

FROM {{ dynamic_src("models.f_ethereum_holders") }}
