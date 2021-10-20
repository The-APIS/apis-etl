import subprocess
import requests


def get_end_block(blockchain_jsonrpc):
    result = requests.post(
        blockchain_jsonrpc,
        headers={"Content-Type": "application/json"},
        json={"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 0},
    )
    return int(result.json()["result"], 16)


def create_requisite_files(file_type, file_name, logger):
    logger.info(f"Extracting {file_type} for {file_name}")
    extract_sk = [
        "ethereumetl",
        "extract_csv_column",
        "--input",
        "INPUT",
        "--output",
        f"data_downloads/{file_type}.txt",
        "--column",
        "PLACEHOLDER",
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
        first_task[6], first_task[7] = (
            "-p",
            "" "item['is_erc20'] or item['is_erc721']" "",
        )

        processes_to_run.append(first_task)

        extract_sk[1] = "extract_field"
        extract_sk[3] = "data_downloads/filtered_contracts.csv"
        extract_sk[5] = "data_downloads/token_addresses.txt"
        extract_sk[6], extract_sk[7] = "-f", "address"

    elif file_type == "geth_traces":
        extract_sk = extract_sk[:-2]
        extract_sk[1] = "extract_geth_traces"
        extract_sk[3] = f"data_downloads/geth_traces/{file_name.replace('.csv', '.json')}"
        extract_sk[5] = f"data_downloads/geth_traces/{file_name}"

    processes_to_run.append(extract_sk)

    for process in processes_to_run:
        subprocess.run(process)


def create_put_query(table_name, schema, stage, current_directory, file_name, logger):
    logger.info(f"Putting {table_name} into snowflake for {file_name}")
    return f"""
        USE SCHEMA {schema};

        PUT file://{current_directory}/data_downloads/{table_name}/{file_name} @{stage}/{table_name}/ auto_compress=true;
    """


def extract_table(table_name, blockchain_uri, max_workers, file_name, logger):
    if not blockchain_uri.startswith("https://"):
        blockchain_uri = "file://" + blockchain_uri
    logger.info(f"Exporting {table_name} for {file_name}")
    base_subprocess = [
        "ethereumetl",
        f"export_{table_name}",
        "--max-workers",
        f"{max_workers}",
        "--provider-uri",
        blockchain_uri,
    ]
    if table_name == "blocks_and_transactions":
        start_block, stop_block = (
            file_name.split("_")[2],
            file_name.split("_")[3].strip(".csv"),
        )
        additional_parts = [
            "--start-block",
            start_block,
            "--end-block",
            stop_block,
            "--blocks-output",
            f"data_downloads/blocks/{file_name}",
            "--transactions-output",
            f"data_downloads/transactions/{file_name}",
        ]
    elif table_name == "receipts_and_logs":
        additional_parts = [
            "--transaction-hashes",
            "data_downloads/transaction_hashes.txt",
            "--receipts-output",
            f"data_downloads/receipts/{file_name}",
            "--logs-output",
            f"data_downloads/logs/{file_name}",
        ]

    elif table_name == "contracts":
        additional_parts = [
            "--contract-addresses",
            "data_downloads/contract_addresses.txt",
            "--output",
            f"data_downloads/contracts/{file_name}",
            "--batch-size",
            "1",
        ]

    elif table_name == "tokens":
        additional_parts = [
            "--token-addresses",
            "data_downloads/token_addresses.txt",
            "--output",
            f"data_downloads/tokens/{file_name}",
        ]

    elif table_name == "token_transfers":
        base_subprocess = base_subprocess[:4]
        base_subprocess[1] = "extract_token_transfers"
        additional_parts = [
            "--logs",
            f"data_downloads/logs/{file_name}",
            "--output",
            f"data_downloads/token_transfers/{file_name}",
        ]

    elif table_name == "geth_traces":
        start_block, stop_block = (
            file_name.split("_")[2],
            file_name.split("_")[3].strip(".csv"),
        )
        del base_subprocess[2:4]
        additional_parts = [
            "--start-block",
            start_block,
            "--end-block",
            stop_block,
            "--output",
            f"data_downloads/geth_traces/{file_name.replace('.csv', '.json')}"
        ]

    # Only available from an archive node, has block_number which the regular extract does not
    elif table_name == "archive_contracts":
        base_subprocess = base_subprocess[:4]
        base_subprocess[1] = "extract_contracts"
        additional_parts = [
            "--traces",
            f"data_downloads/geth_traces/{file_name}",
            "--output",
            f"data_downloads/contracts/{file_name}"
        ]

    # Run the command with additional parts
    base_subprocess.extend(additional_parts)
    subprocess.run(base_subprocess)
