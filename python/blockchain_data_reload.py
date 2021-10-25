from os import getcwd, makedirs, path, remove

from sayn import PythonTask

from .ethereumetl_extract_helper import create_requisite_files, create_put_query, extract_table, get_end_block


class LoadData(PythonTask):

    def run(self):
        file_format = self.parameters["file_format"]
        schema = self.parameters["schema"]["logs"]
        is_test = self.parameters["is_test"]
        blocks_per_file = self.parameters["blocks_per_file"]
        max_workers = self.parameters["max_workers"]
        is_archive = self.parameters["is_archive"]
        user_prefix = self.parameters["user_prefix"]

        # Blockchain specific parameters
        blockchain = self.parameters["blockchain_name"]
        stage = f"{blockchain}_stage"
        blockchain_uri = self.parameters["blockchain"][blockchain]

        current_directory = getcwd()

        # Create subdirectories which will allow us to reuse the same file name
        tables = ['blocks', 'transactions', 'receipts', 'logs', 'token_transfers', 'contracts', 'tokens']

        # Check if traces are available
        if is_archive:
            tables.append('geth_traces')

        for table in tables:
            full_path = current_directory + '/data_downloads/' + table
            if path.isdir(full_path):
                pass
            else:
                makedirs(full_path)

        # Create stage if one doesn't exist
        staging_query = f'''

            USE SCHEMA {schema};

            CREATE STAGE IF NOT EXISTS { stage }
                 file_format = { file_format };

            '''
        self.logger.info(f"Creating Stage: { stage }")
        self.default_db.execute(staging_query)

        # Get all the missing block ranges
        get_missing_block_ranges_query = f'''
            WITH base AS (
            SELECT number
                 , LAG(number) OVER (ORDER BY number) AS prev_number
                 , CASE WHEN number != prev_number + 1 THEN prev_number + 1 ELSE NULL END AS reload_from

            FROM {schema}.{user_prefix}{blockchain}_blocks

            ORDER BY 1 DESC
            )

            SELECT reload_from
                 , number - 1 AS reload_to

            FROM base

            WHERE reload_from IS NOT NULL
        '''

        missing_block_ranges = self.default_db.read_data(get_missing_block_ranges_query)

        # Load each missing block range in batches determined by blocks per file
        for entry in missing_block_ranges:
            start_block = entry["reload_from"]
            end_block = entry["reload_to"]

            while end_block >= start_block:

                if end_block > start_block + blocks_per_file:
                    stop_block = start_block + blocks_per_file
                else:
                    stop_block = end_block + 1

                file_name = f"data_blocks_{start_block}_{stop_block - 1}.csv"

                # Extract blocks, transactions and transaction hashes
                extract_table("blocks_and_transactions", blockchain_uri, max_workers, file_name, self.logger)
                create_requisite_files("transaction_hashes", file_name, self.logger)

                # Put blocks and transactions into snowflake + remove from local memory
                self.default_db.execute(create_put_query("blocks", schema, stage, current_directory, file_name, self.logger))
                self.default_db.execute(create_put_query("transactions", schema, stage, current_directory, file_name, self.logger))
                remove(f'data_downloads/blocks/{file_name}')
                remove(f'data_downloads/transactions/{file_name}')

                # Extract logs, receipts and token transfers + remove transaction_hashes.txt from local memory
                extract_table("receipts_and_logs", blockchain_uri, max_workers, file_name, self.logger)
                extract_table("token_transfers", blockchain_uri, max_workers, file_name, self.logger)
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
                extract_table("contracts", blockchain_uri, max_workers, file_name, self.logger)
                create_requisite_files("token_addresses", file_name, self.logger)
                extract_table("tokens", blockchain_uri, max_workers, file_name, self.logger)

                # Put contracts and tokens into snowflake + remove from local memory
                self.default_db.execute(create_put_query("contracts", schema, stage, current_directory, file_name, self.logger))
                self.default_db.execute(create_put_query("tokens", schema, stage, current_directory, file_name, self.logger))
                remove(f"data_downloads/contracts/{file_name}")
                remove(f"data_downloads/tokens/{file_name}")

                # If archive node is available, extract and put geth traces into snowflake + remove files from local memory
                if is_archive:
                    extract_table("geth_traces", blockchain_uri, max_workers, file_name, self.logger)
                    create_requisite_files("geth_traces", file_name, self.logger)
                    self.default_db.execute(create_put_query("geth_traces", schema, stage, current_directory, file_name, self.logger))
                    remove(f"data_downloads/geth_traces/{file_name.replace('.csv', '.json')}")
                    remove(f"data_downloads/geth_traces/{file_name}")

                # Clean up remaining requisite files
                remove("data_downloads/filtered_contracts.csv")
                remove("data_downloads/contract_addresses.txt")
                remove("data_downloads/token_addresses.txt")

                start_block += blocks_per_file
                # Used to restrict test runs to one batch only
                if is_test:
                    break
            # Used to restrict test runs to one batch only
            if is_test:
                break
            ## Double break is used here to get around multiple entries with large ranges when testing

        return self.success()
