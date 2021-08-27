# Quick Start

## Scheduling Incremental Load Into Snowflake via Docker Container

1. Clone the repository and change directory to the cloned repo
2. Run `docker build . -t incremental_load`
3. Run `docker run -e SAYN_CREDENTIAL_warehouse='your_snowflake_credentials' incremental_load sayn run -t group:extract`

Note: For a quick single test run, command in part 3 should be change to this
`docker run -e SAYN_CREDENTIAL_warehouse='your_snowflake_credentials' SAYN_PARAMETER_is_test='true' SAYN_PARAMETER_schema='{"logs":"test_logs", "staging":"test_staging", "models":"test_models", "viz":"test_viz"}' incremental_load sayn run -t group:extract` 

## Retrieving Snowflake Data via Docker Container

1. Clone the repository and change directory to the cloned repo
2. Run `docker build . -t get_snowflake_data`
3. Run `docker run -e SAYN_CREDENTIAL_warehouse='your_snowflake_credentials' -v path_to_save_to:/app/data_downloads get_snowflake_data sayn run -t group:data_dump`


### Variable Explanation
`your_snowflake_credentials` : this should be changed to your snowflake credentials in JSON format

#### Credentials Structure
```
{
  "type": "snowflake"
  "account": "part before snowflakecomputing in the snowflake url e.g. account = abc100 if url = https://abc100.snowflakecomputing.com/"
  "user": "your username"
  "password": "your password"
  "database": "THEAPIS"
  "warehouse": "COMPUTE_WH"
  "role": "ETL"
}
```
`path_to_save_to` = local path to save the tables to

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
