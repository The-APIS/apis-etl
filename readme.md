# Quick Start

## Retrieving Snowflake Data via Docker Container

1. Clone the repository and change directory to the clone repo
2. Run `docker build . -t get_snowflake_data`
3. Run `docker run -e SAYN_CREDENTIAL_warehouse='your_snowflake_credentials' -v path_to_save_to_1:/Users/tim/f_ethereum_token_summary -v path_to_save_to_2:/Users/tim/f_ethereum_holders get_snowflake_data`

### Variable Explanation
`your_snowflake_credentials` : this should be changed to your snowflake credentials in JSON format

#### Credentials Structure
{
  "type": "snowflake"
  "account": "part before snowflakecomputing in the snowflake url e.g. account = abc100 if url = https://abc100.snowflakecomputing.com/"
  "user": "your username"
  "password": "your password"
  "database": "THEAPIS"
  "warehouse": "COMPUTE_WH"
  "role": "ETL"
}

`path_to_save_to_1`: the local path you want to save the files for `f_ethereum_token_summary` e.g. local_path/f_ethereum_token_summary

`path_to_save_to_2`: the local path you want to save the files for `f_ethereum_holders` e.g. local_path/f_ethereum_holders

## Running Locally

You will need to create a `settings.yaml` file, with the following structure:

Example settings.yaml
```
default_profile: test

profiles:
  prod:
    credentials:
      warehouse: snowflake

  test:
    credentials:
      warehouse: snowflake

    parameters:
      user_prefix: your_initials
      schema:
        logs: test_logs
        staging: test_staging
        models: test_models
        viz: test_viz
      from_prod:
        - logs.ethereum_contracts
        - logs.ethereum_transactions
        - logs.ethereum_methods
        - logs.ethereum_blocks
        - logs.bq_ethereum_token_transfers
        - logs.bq_ethereum_tokens
        - logs.bq_ethereum_contracts
        - staging.stg_ethereum_token_transfers_cast
        - models.top_100_tokens

credentials:
  snowflake:
    type: snowflake
    account: "part before snowflakecomputing in the snowflake url e.g. account = abc100 if url = https://abc100.snowflakecomputing.com/"
    user: "your username"
    password: "your password"
    database: THEAPIS
    warehouse: COMPUTE_WH
    role: ETL
```
