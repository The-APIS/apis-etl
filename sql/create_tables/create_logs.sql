USE SCHEMA {{ schema }};

CREATE TABLE IF NOT EXISTS {{ table }} (
    log_index	NUMBER(38,0)
  , transaction_hash VARCHAR(16777216)
  , transaction_index	NUMBER(38,0)
  , block_hash VARCHAR(16777216)
  , block_number NUMBER(38,0)
  , address VARCHAR(16777216)
  , data VARCHAR(16777216)
  , topics VARCHAR(16777216)
)
