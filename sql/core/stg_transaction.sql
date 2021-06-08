{%- from 'sql/macros/dynamic_source.sql' import dynamic_src  -%}

SELECT tr.hash AS transaction_hash
  	 , tr.blockhash AS block_hash
  	 , tr.blocknumber AS block_number
  	 , tr.createdat AS transaction_created_ts
  	 , tr.updatedat AS transaction_updated_ts
  	 , tr.from_address
  	 , tr.to_address
  	 , tr.transactionindex AS transaction_index
  	 , tr.gasprice AS gas_price
  	 , tr.gasused AS gas_used
     , tr.gas
  	 , tr.nonce
  	 , tr.value AS transaction_value
  	 , tr.status AS transaction_status

FROM {{ dynamic_src("logs.ethereum_transactions") }} tr
