USE SCHEMA {{ schema }};

CREATE TABLE IF NOT EXISTS {{ table }} (
    address VARCHAR(16777216)
  , symbol VARCHAR(16777216)
  , name VARCHAR(16777216)
  , decimals NUMBER(38,0)
  , total_supply VARCHAR(16777216)
  , block_number NUMBER(38,0)
)
