USE SCHEMA {{ schema }};

CREATE TABLE IF NOT EXISTS {{ table }} (
    token_address VARCHAR(16777216)
  , from_address VARCHAR(16777216)
  , to_address VARCHAR(16777216)
  , value	VARCHAR(16777216)
  , transaction_hash VARCHAR(16777216)
  , log_index	NUMBER(38,0)
  , block_number NUMBER(38,0)
)
