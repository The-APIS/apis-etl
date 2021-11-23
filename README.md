# Quick Start

## BSC Incremental Load (via Docker container)

These steps will achieve the following:

* Extract and load the latest BSC blocks so that the snowflake tables are up to date.
* Update the analysis tables using this new data.
* Unload the analysis tables into a given location in JSON format.

To build and execute this task as a docker container:

1. Clone the repository and change directory to the cloned repo
2. Run
```
docker build . -f Dockerfile_bsc -t incremental_bsc_load
```
3. Run
```
docker run \
  -e SAYN_CREDENTIAL_warehouse='your_snowflake_credentials' \
  incremental_bsc_load \
  scripts/daily_bsc.sh
```

Note: For a single test run, command in part 3 should be changed to this. To optionally define the start and end blocks, refer to the `test_value` parameter, covered in [Additional information](#testvalues)

```
docker run \
  -e SAYN_CREDENTIAL_warehouse='your_snowflake_credentials' \
  -e SAYN_PARAMETER_is_test='true' \
  -e SAYN_PARAMETER_user_prefix=your_initials_ \
  # Optional: -e SAYN_PARAMETER_test_values='{"start_block":start, "end_block":end}' \
  -e SAYN_PARAMETER_schema='{"logs":"test_logs", "staging":"test_staging", "models":"test_models", "viz":"test_viz"}' \
  incremental_bsc_load \
  sayn run -t group:create_bsc_tables -t group:extract_bsc -t stg_dates -t group:xwg -t group:xwg_data_dump
```

## Scheduling Ethereum Incremental Load Into Snowflake via Docker Container

1. Clone the repository and change directory to the cloned repo
2. Run
```
docker build . -f Dockerfile_eth -t incremental_eth_load
```
3. Run
```
docker run \
  -e SAYN_CREDENTIAL_warehouse='your_snowflake_credentials' \
  -e SAYN_PARAMETER_blockchain='your_node_locations' \
  incremental_eth_load \
  sayn run -t group:create_eth_tables -t group:extract_eth -d
```

Note: For a quick single test run, command in part 3 should be change to this

```
docker run \
  -e SAYN_CREDENTIAL_warehouse='your_snowflake_credentials' \
  -e SAYN_PARAMETER_blockchain='your_node_locations' \
  -e SAYN_PARAMETER_is_test='true' \
  -e SAYN_PARAMETER_schema='{"logs":"test_logs", "staging":"test_staging", "models":"test_models", "viz":"test_viz"}' \
  incremental_eth_load \
  sayn run -t group:create_eth_tables -t group:extract_eth -d
```

## Data unload (via Docker container)

For generic blockchain data, do the following:

1. Clone the repository and change directory to the cloned repo
2. Run
```
docker build . -t get_snowflake_data
```
3. Run
```
docker run \
  -e SAYN_CREDENTIAL_warehouse='your_snowflake_credentials' \
  -v path_to_save_to:/app/data_downloads \
  get_snowflake_data \
  sayn run -t group:data_dump
```

For the XWG analysis data, the following should replace 3:
```
docker run \
  -e SAYN_CREDENTIAL_warehouse='your_snowflake_credentials' \
  -v path_to_save_to:/app/data_downloads \
  get_snowflake_data \
  sayn run -t group:xwg_data_dump

```

### Data unload variable Explanation

`path_to_save_to` = local path to save the tables to

`your_snowflake_credentials` = this should be changed to your snowflake credentials in JSON format

`your_node_locations` = this should be changed to a list of your node locations in JSON format

## Additional information

### Credentials Structure

Wherever `'your_snowflake_credentials'` is mentioned, it will expect a JSON object like this:

```
{
  "type": "snowflake",
  "account": "part before snowflakecomputing in the snowflake url e.g. account = abc100 if url = https://abc100.snowflakecomputing.com/",
  "user": "your username",
  "password": "your password",
  "database": "THEAPIS",
  "warehouse": "COMPUTE_WH",
  "role": "ETL"
}
```

## Blockchain Node Locations Structure
```
{
  "bsc": "https://bsc-dataseed.binance.org"
  "eth": "path to your local ethereum node"
}
```

When using a local node, the values above is expected to be a standard unix path (eg: `/var/ipc/ethereum.ipc`).

<a name="testvalues">

## Test Values Parameter (Optional)

</a>

Structure:

```
{
  "start_block": "your start block number for testing"
  "end_block": "your end block number for testing"
}
```

Note: start_block and end_block are integers not strings

This parameter can be modified to change start and end blocks of a test (not specifying this parameter will result in a default test)

You can add this parameter after the `docker run` command, e.g. `docker run -e SAYN_PARAMETER_test_values='your_test_values'...`

## Running Locally

You will need to create a `settings.yaml` file, with the following structure:

Example settings.yaml
```
default_profile: test

profiles:
  prod:
    credentials:
      warehouse: snowflake
    parameters:
      user_prefix: ""
      schema:
        logs: analytics_logs
        staging: analytics_staging
        models: analytics_models
        viz: analytics_viz

  test:
    credentials:
      warehouse: snowflake

    parameters:
      user_prefix: your_initials
      is_test: true
      test_values:
        start_block: your_test_start_block
        end_block: your_test_end_block
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
