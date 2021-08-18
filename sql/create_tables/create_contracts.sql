USE SCHEMA {{ schema }};

CREATE TABLE IF NOT EXISTS {{ table }} (
    address VARCHAR(16777216)
  , bytecode VARCHAR(16777216)
  , function_sighashes VARCHAR(16777216)
  , is_erc20	boolean
  , is_erc721	boolean
  , block_number NUMBER(38,0)
)
