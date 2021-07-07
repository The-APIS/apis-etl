{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT number
     , hash
     , parent_hash AS parentHash
     , nonce
     , sha3_uncles AS sha3Uncles
     , logs_bloom AS logsBloom
     , transactions_root AS transactionsRoot
     , state_root AS stateRoot
     , receipts_root AS receiptsRoot
     , miner
     , difficulty
     , total_difficulty AS totalDifficulty
     , extra_data AS extraData
     , size
     , gas_limit AS gasLimit
     , gas_used AS gasUsed
     , timestamp
     , NULL AS transactions
     , NULL AS uncles

FROM {{ dynamic_src("logs.bq_ethereum_blocks") }}
