{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT b.hash AS block_hash
  	 , b.parenthash AS parent_block_hash
  	 , b.timestamp AS block_ts
  	 , b.createdat AS block_created_ts
  	 , b.updatedat AS block_updated_ts
  	 , b.number AS block_number
  	 , b.nonce AS nonce
  	 , b.miner AS miner_address
  	 , b.difficulty
  	 , b.totaldifficulty AS total_difficulty
  	 , b.size AS block_size
  	 , b.gaslimit AS gas_limit
  	 , b.blockgasused AS block_gas_used
  	 , 100 * b.blockgasused/b.gaslimit AS percentage_gas_used

FROM {{ dynamic_src("logs.ethereum_blocks") }} b
