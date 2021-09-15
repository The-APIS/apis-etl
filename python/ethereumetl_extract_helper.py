import subprocess

def get_end_block(blockchain_url):
    result = subprocess.run([ "geth"
                            , "attach"
                            , blockchain_url
                            , "--exec"
                            , ''"eth.blockNumber"''
                            ]
                            , capture_output=True
                            , text=True)
    return(int(result.stdout.strip('\n')))

def create_requisite_files(file_type, file_name, logger):
    logger.info(f"Extracting {file_type} for {file_name}")
    extract_sk = [ "ethereumetl"
                 , "extract_csv_column"
                 , "--input"
                 , "INPUT"
                 , "--output"
                 , f"data_downloads/{file_type}.txt"
                 , "--column"
                 , "PLACEHOLDER"
                 ]
    processes_to_run = []
    if file_type == "transaction_hashes":
        extract_sk[3] = f"data_downloads/transactions/{file_name}"
        extract_sk[7] = "hash"

    elif file_type == "contract_addresses":
        extract_sk[3] = f"data_downloads/receipts/{file_name}"
        extract_sk[7] = "contract_address"

    elif file_type == "token_addresses":
        first_task = extract_sk.copy()
        first_task[1] = "filter_items"
        first_task[3] = f"data_downloads/contracts/{file_name}"
        first_task[5] = "data_downloads/filtered_contracts.csv"
        first_task[6], first_task[7] = "-p" , ''"item['is_erc20'] or item['is_erc721']"''

        processes_to_run.append(first_task)

        extract_sk[1] = "extract_field"
        extract_sk[3] = "data_downloads/filtered_contracts.csv"
        extract_sk[5] = "data_downloads/token_addresses.txt"
        extract_sk[6], extract_sk[7] = "-f" , "address"

    processes_to_run.append(extract_sk)

    for process in processes_to_run:
        subprocess.run(process)

def create_put_query(table_name, schema, stage, current_directory, file_name, logger):
    logger.info(f"Putting {table_name} into snowflake for {file_name}")
    return (
    f'''
        USE SCHEMA {schema};

        PUT file://{current_directory}/data_downloads/{table_name}/{file_name} @{stage}/{table_name}/ auto_compress=true;
    '''
    )

def extract_table(table_name, blockchain_url, max_workers, file_name, logger):
    logger.info(f"Exporting {table_name} for {file_name}")
    base_subprocess = [ "ethereumetl"
                      , f"export_{table_name}"
                      , "--max-workers"
                      , f"{max_workers}"
                      , "--provider-uri"
                      , blockchain_url
                      ]
    if table_name == "blocks_and_transactions":
        start_block, stop_block = file_name.split("_")[2], file_name.split("_")[3].strip(".csv")
        additional_parts = [ "--start-block"
                           , start_block
                           , "--end-block"
                           , stop_block
                           , "--blocks-output"
                           , f"data_downloads/blocks/{file_name}"
                           , "--transactions-output"
                           , f"data_downloads/transactions/{file_name}"
                           ]
    elif table_name == "receipts_and_logs":
        additional_parts = [ "--transaction-hashes"
                           , "data_downloads/transaction_hashes.txt"
                           , "--receipts-output"
                           , f"data_downloads/receipts/{file_name}"
                           , "--logs-output"
                           , f"data_downloads/logs/{file_name}"
                           ]

    elif table_name == "contracts":
        additional_parts = [ "--contract-addresses"
                           , "data_downloads/contract_addresses.txt"
                           , "--output"
                           , f"data_downloads/contracts/{file_name}"
                           , "--batch-size"
                           , "1"
                           ]

    elif table_name == "tokens":
        additional_parts = [ "--token-addresses"
                           , "data_downloads/token_addresses.txt"
                           , "--output"
                           , f"data_downloads/tokens/{file_name}"
                           ]

    elif table_name == "token_transfers":
        base_subprocess = base_subprocess[:4]
        base_subprocess[1] = "extract_token_transfers"
        additional_parts = [ "--logs"
                           , f"data_downloads/logs/{file_name}"
                           , "--output"
                           , f"data_downloads/token_transfers/{file_name}"
                           ]

    # Run the command with additional parts
    base_subprocess.extend(additional_parts)
    subprocess.run(base_subprocess)
