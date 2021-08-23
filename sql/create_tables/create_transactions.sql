USE SCHEMA {{ schema }};

CREATE TABLE IF NOT EXISTS {{ table }} (
    hash VARCHAR(16777216)
  , nonce	NUMBER(38,0)
  , block_hash VARCHAR(16777216)
  , block_number NUMBER(38,0)
  , transaction_index	NUMBER(38,0)
  , from_address VARCHAR(16777216)
  , to_address VARCHAR(16777216)
  , value VARCHAR(16777216)
  , gas	NUMBER(38,0)
  , gas_price	NUMBER(38,0)
  , input	VARCHAR(16777216)
  , block_timestamp	NUMBER(38,0)
)
