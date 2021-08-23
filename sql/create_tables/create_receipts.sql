USE SCHEMA {{ schema }};

CREATE TABLE IF NOT EXISTS {{ table }} (
    transaction_hash	VARCHAR(16777216)
  , transaction_index	NUMBER(38,0)
  , block_hash	VARCHAR(16777216)
  , block_number	NUMBER(38,0)
  , cumulative_gas_used	NUMBER(38,0)
  , gas_used	NUMBER(38,0)
  , contract_address VARCHAR(16777216)
  , root	VARCHAR(16777216)
  , status	NUMBER(38,0)
)
