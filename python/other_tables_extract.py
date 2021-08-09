from sayn import PythonTask
from os import getcwd, makedirs, path, remove
import subprocess
import gzip
import shutil
import csv
import time

class loadData(PythonTask):

    def run(self):

        file_format = self.parameters["file_format"]
        schema = self.parameters["schema"]
        stage = self.parameters["stage"]
        max_workers = self.parameters["max_workers"]
        start_block = self.parameters["start_block"]
        end_block = self.parameters["end_block"]

        current_directory = getcwd()
        tables = ['transactions', 'receipts', 'logs', 'token_transfers', 'contracts', 'tokens']
        for table in tables:
            full_path = current_directory + '/data_downloads/' + table
            if path.isdir(full_path):
                pass
            else:
                makedirs(full_path)

        with open('t_files.csv') as f:
            reader = csv.reader(f)
            list_of_files = list(reader)

        staging_query = f'''

            USE SCHEMA {schema};

            CREATE FILE FORMAT IF NOT EXISTS { file_format }
                 TYPE = 'CSV'
                 FIELD_DELIMITER = ','
                 SKIP_HEADER = 1
                 NULL_IF = ('0000-00-00 00:00:00')
                 EMPTY_FIELD_AS_NULL = true
                 FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                 ;

            CREATE STAGE IF NOT EXISTS { stage }
                 file_format = { file_format };

            '''
        print(f"Creating Stage: { stage }")
        self.default_db.execute(staging_query)

        for i in range(start_block, end_block):
            start_time = time.time()
            file_name = list_of_files[i][0]
            file_path = 'data_downloads/transactions/' + file_name
            ref = file_name[:-3]
            decompressed_file = file_path[:-3]
            path_to_write_to = current_directory + '/data_downloads/transactions/'
            get_query = f'''
                USE SCHEMA {schema};

                GET @data_load_test/transactions/{file_name} file://{path_to_write_to};

            '''
            print(f"Getting data from snowflake for {ref}")
            self.default_db.execute(get_query)

            print(f"Extracting transaction hashes for {ref}")
            subprocess.run(
                [ "gzip"
                , "-d"
                , file_path
                ])
            subprocess.run(
                [ "ethereumetl"
                , "extract_csv_column"
                , "--input"
                , decompressed_file
                , "--column"
                , "hash"
                , "--output"
                , "data_downloads/transactions/hashes.txt"
                ])
            remove(decompressed_file)

            print(f"Exporting receipts and logs for {ref}")
            subprocess.run(
                [ "ethereumetl"
                , "export_receipts_and_logs"
                , "--transaction-hashes"
                , "data_downloads/transactions/hashes.txt"
                , "--provider-uri"
                , "https://bsc-dataseed.binance.org"
                , "--receipts-output"
                , f"data_downloads/receipts/receipts_for_{ref}"
                , "--logs-output"
                , f"data_downloads/logs/logs_for_{ref}"
                , "--max-workers"
                , f"{max_workers}"
                ])
            remove('data_downloads/transactions/hashes.txt')

            print(f"Exporting token transfers for {ref}")
            subprocess.run(
                [ "ethereumetl"
                , "extract_token_transfers"
                , "--logs"
                , f"data_downloads/logs/logs_for_{ref}"
                , "--output"
                , f"data_downloads/token_transfers/token_transfers_for_{ref}"
                , "--max-workers"
                , f"{max_workers}"
                ])

            print(f"Putting logs into snowflake for {ref}")
            self.default_db.execute(f'''
                USE SCHEMA {schema};

                PUT file://{current_directory + '/data_downloads/logs/logs_for_' + ref} @{stage}/logs/ auto_compress=true;
            '''
            )
            remove(f"data_downloads/logs/logs_for_{ref}")

            print(f"Putting token transfers into snowflake for {ref}")
            self.default_db.execute(f'''
                USE SCHEMA {schema};

                PUT file://{current_directory + '/data_downloads/token_transfers/token_transfers_for_' + ref} @{stage}/token_transfers/ auto_compress=true;
            '''
            )
            remove(f"data_downloads/token_transfers/token_transfers_for_{ref}")

            print(f"Extracting contract addresses for {ref}")
            subprocess.run(
                [ "ethereumetl"
                , "extract_csv_column"
                , "--input"
                , f"data_downloads/receipts/receipts_for_{ref}"
                , "--column"
                , "contract_address"
                , "--output"
                , "data_downloads/contract_addresses.txt"
                ])

            print(f"Putting receipts into snowflake for {ref}")
            self.default_db.execute(f'''
                USE SCHEMA {schema};

                PUT file://{current_directory + '/data_downloads/receipts/receipts_for_' + ref} @{stage}/receipts/ auto_compress=true;
            '''
            )
            remove(f"data_downloads/receipts/receipts_for_{ref}")

            print(f"Export contracts for {ref}")
            subprocess.run(
                [ "ethereumetl"
                , "export_contracts"
                , "--contract-addresses"
                , "data_downloads/contract_addresses.txt"
                , "--provider-uri"
                , "https://bsc-dataseed.binance.org"
                , "--output"
                , f"data_downloads/contracts/contracts_for_{ref}"
                , "--batch-size"
                , "1"
                , "--max-workers"
                , f"{max_workers}"
                ])

            print(f"Extract token addresses for {ref}")
            subprocess.run(
                [ "ethereumetl"
                , "filter_items"
                , "-i"
                , f"data_downloads/contracts/contracts_for_{ref}"
                , "-p"
                , ''"item['is_erc20'] or item['is_erc721']"''
                , "--output"
                , "data_downloads/filtered_contracts.csv"
                ])
            subprocess.run(
                [ "ethereumetl"
                , "extract_field"
                , "-i"
                , "data_downloads/filtered_contracts.csv"
                , "-f"
                , "address"
                , "-o"
                , "data_downloads/token_addresses.txt"
                ])
            remove("data_downloads/filtered_contracts.csv")

            print(f"Exporting tokens for {ref}")
            subprocess.run(
                [ "ethereumetl"
                , "export_tokens"
                , "--token-addresses"
                , "data_downloads/token_addresses.txt"
                , "--provider-uri"
                , "https://bsc-dataseed.binance.org"
                , "--output"
                , f"data_downloads/tokens/tokens_for_{ref}"
                , "--max-workers"
                , f"{max_workers}"
                ])

            print(f"Putting contracts into snowflake for {ref}")
            self.default_db.execute(f'''
                USE SCHEMA {schema};

                PUT file://{current_directory + '/data_downloads/contracts/contracts_for_' + ref} @{stage}/contracts/ auto_compress=true;
            '''
            )
            remove(f"data_downloads/contracts/contracts_for_{ref}")

            print(f"Putting tokens into snowflake for {ref}")
            self.default_db.execute(f'''
                USE SCHEMA {schema};

                PUT file://{current_directory + '/data_downloads/tokens/tokens_for_' + ref} @{stage}/tokens/ auto_compress=true;
            '''
            )
            remove(f"data_downloads/tokens/tokens_for_{ref}")

            remove("data_downloads/contract_addresses.txt")
            remove("data_downloads/token_addresses.txt")

            print(f"\n\n---- Progress {i}/{end_block} ----\n")
            print(f"Total Runtime: {(time.time() - start_time)/60} minutes")

        #
        # put_query = f'''
        #
        #     USE SCHEMA {schema};
        #
        #     PUT file://{full_path}/* @{stage}/{table}/ auto_compress=true;
        #
        #     '''
        #
        # while end_block > start_block:
        #
        #     if end_block > start_block + blocks_per_file:
        #         stop_block = start_block + blocks_per_file
        #     else:
        #         stop_block = end_block
        #
        #     file_name = f"/data_blocks_{start_block}_{stop_block - 1}.csv"
        #     relative_path = 'data_downloads/' + table + file_name
        #
        #     print(f"Creating {file_name}")
        #     subprocess.run(
        #         [ "ethereumetl"
        #         , "export_blocks_and_transactions"
        #         , "--start-block"
        #         , str(start_block)
        #         , "--end-block"
        #         , str(stop_block - 1)
        #         , f"--{table}-output"
        #         , relative_path
        #         , "--provider-uri"
        #         , "https://bsc-dataseed.binance.org"
        #         ])
        #
        #     print(f"Putting {file_name} into stage")
        #     self.default_db.execute(put_query)
        #     print(f"Removing {file_name}")
        #     remove(full_path + f"{file_name}")
        #
        #     start_block += blocks_per_file

        # with self.step("Copying Files into Table"):
        #
        #     full_table_name = 'eth_etl_bsc_' + table
        #
        #     copy_query = f'''
        #
        #     CREATE OR REPLACE TABLE { full_table_name } (
        #         number NUMBER,
        #         hash VARCHAR,
        #         parent_hash VARCHAR,
        #         nonce VARCHAR,
        #         sha3_uncles VARCHAR,
        #         logs_bloom VARCHAR,
        #         transactions_root VARCHAR,
        #         state_root VARCHAR,
        #         receipts_root VARCHAR,
        #         miner VARCHAR,
        #         difficulty NUMBER,
        #         total_difficulty NUMBER,
        #         size NUMBER,
        #         extra_data VARCHAR,
        #         gas_limit NUMBER,
        #         gas_used NUMBER,
        #         timestamp TIMESTAMP_NTZ,
        #         transaction_count NUMBER
        #     );
        #
        #     COPY INTO { full_table_name }
        #          from @{stage}/{table}/
        #          file_format = (format_name = { file_format });
        #
        #     '''
        #
        #     self.default_db.execute(copy_query)

        # remove(full_path)

        return self.success()
