from os import getcwd, makedirs, path, remove

from sayn import PythonTask

from .ethereumetl_extract_helper import create_requisite_files, create_put_query, extract_table, get_end_block


class LoadData(PythonTask):

    def run(self):
        file_format = self.parameters["file_format"]
        stage = self.parameters["stage"]
        schema = self.parameters["schema"]["logs"]
        is_test = self.parameters["is_test"]
        test_values = self.parameters["test_values"]
        blockchain = self.parameters["blockchain"]
        blockchain_url = self.parameters["blockchain_url"]
        blocks_per_file = self.parameters["blocks_per_file"]
        max_workers = self.parameters["max_workers"]
        user_prefix = self.parameters["user_prefix"]
        current_directory = getcwd()
        tables = ['blocks', 'transactions', 'receipts', 'logs', 'token_transfers', 'contracts', 'tokens']

        # Get the last block number in the blocks table
        get_block_max_query = f"SELECT MAX(NUMBER) AS last_block FROM {schema}.{user_prefix}{blockchain}_blocks;"
        data = self.default_db.read_data(get_block_max_query)
        try:
            last_block = data[0]["last_block"]
            if last_block is None:
                last_block = -1
        except IndexError:
            last_block = -1

        # Check stage for existing files and get the last block that was fully loaded into stage
        max_extracted = []
        for table in tables:
            list_stage_query = f"LIST @{schema}.{stage}/{table}/;"
            staged_files = self.default_db.read_data(list_stage_query)
            list_to_compare = [int(x['name'].strip(".csv.gz").split("_")[-1]) for x in staged_files]
            max_extracted.append(max(list_to_compare, default=-1))
        min_max_extracted = min(max_extracted)

        # Start the extract at the last loaded block (includes table and stage) or testing start block if it's the largest
        start_block = max(last_block, min_max_extracted) + 1

        # Get the latest block number from the blockchain, set this as the end of the extract
        end_block = get_end_block(blockchain_url)

        # Set test run values
        if is_test:
            # Start block overwrite
            if start_block < test_values["start_block"]:
                start_block = test_values["start_block"]
            # End block overwrite
            if start_block < test_values["end_block"]:
                end_block = test_values["end_block"]
                # Don't break loop if valid end_block is given
                is_test = False

        # Repeat the extract process until reaching end block, using batches of blocks controlled by blocks_per_file parameter
        while end_block > start_block:

            if end_block > start_block + blocks_per_file:
                stop_block = start_block + blocks_per_file
            else:
                stop_block = end_block + 1

            file_name = f"data_blocks_{start_block}_{stop_block - 1}.csv"

            # Extract blocks, transactions and transaction hashes
            extract_table("blocks_and_transactions", blockchain_url, max_workers, file_name, self.logger)
            create_requisite_files("transaction_hashes", file_name, self.logger)

            # Put blocks and transactions into snowflake + remove from local memory
            self.default_db.execute(create_put_query("blocks", schema, stage, current_directory, file_name, self.logger))
            self.default_db.execute(create_put_query("transactions", schema, stage, current_directory, file_name, self.logger))
            remove(f'data_downloads/blocks/{file_name}')
            remove(f'data_downloads/transactions/{file_name}')

            # Extract logs, receipts and token transfers + remove transaction_hashes.txt from local memory
            extract_table("receipts_and_logs", blockchain_url, max_workers, file_name, self.logger)
            extract_table("token_transfers", blockchain_url, max_workers, file_name, self.logger)
            remove('data_downloads/transaction_hashes.txt')

            # Put logs and token transfers into snowflake + remove from local memory
            self.default_db.execute(create_put_query("logs", schema, stage, current_directory, file_name, self.logger))
            self.default_db.execute(create_put_query("token_transfers", schema, stage, current_directory, file_name, self.logger))
            remove(f"data_downloads/logs/{file_name}")
            remove(f"data_downloads/token_transfers/{file_name}")

            # Create contract addresses file, put receipts into snowflake + remove from local memory
            create_requisite_files("contract_addresses", file_name, self.logger)
            self.default_db.execute(create_put_query("receipts", schema, stage, current_directory, file_name, self.logger))
            remove(f"data_downloads/receipts/{file_name}")

            # Extract contracts, token_addresses and tokens
            extract_table("contracts", blockchain_url, max_workers, file_name, self.logger)
            create_requisite_files("token_addresses", file_name, self.logger)
            extract_table("tokens", blockchain_url, max_workers, file_name, self.logger)

            # Put contracts and tokens into snowflake + remove from local memory
            self.default_db.execute(create_put_query("contracts", schema, stage, current_directory, file_name, self.logger))
            self.default_db.execute(create_put_query("tokens", schema, stage, current_directory, file_name, self.logger))
            remove(f"data_downloads/contracts/{file_name}")
            remove(f"data_downloads/tokens/{file_name}")

            # Clean up remaining requisite files
            remove("data_downloads/filtered_contracts.csv")
            remove("data_downloads/contract_addresses.txt")
            remove("data_downloads/token_addresses.txt")

            start_block += blocks_per_file

            # Used to restrict test runs to one batch only
            if is_test:
                break

        return self.success()
