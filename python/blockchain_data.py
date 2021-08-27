from os import getcwd, makedirs, path, remove
import subprocess

from sayn import PythonTask


class LoadData(PythonTask):

    def run(self):
        file_format = self.parameters["file_format"]
        stage = self.parameters["stage"]
        schema = self.parameters["schema"]
        is_test = self.parameters["is_test"]
        blockchain = self.parameters["blockchain"]
        blockchain_url = self.parameters["blockchain_url"]
        blocks_per_file = self.parameters["blocks_per_file"]
        max_workers = self.parameters["max_workers"]
        user_prefix = self.parameters["user_prefix"]

        current_directory = getcwd()
        tables = ['blocks', 'transactions', 'receipts', 'logs', 'token_transfers', 'contracts', 'tokens']
        for table in tables:
            full_path = current_directory + '/data_downloads/' + table
            if path.isdir(full_path):
                pass
            else:
                makedirs(full_path)

        staging_query = f'''

            USE SCHEMA {schema};

            CREATE STAGE IF NOT EXISTS { stage }
                 file_format = { file_format };

            '''
        print(f"Creating Stage: { stage }")
        self.default_db.execute(staging_query)


        get_block_max_query = f'''

        SELECT MAX(NUMBER) AS last_block

        FROM {schema}.{user_prefix}{blockchain}_blocks;
        '''

        data = self.default_db.read_data(get_block_max_query)

        try:
            last_block = data[0]["last_block"]
        except IndexError:
            last_block = -1

        max_extracted = []

        for table in tables:
            list_stage_query = f'''

            LIST @{schema}.{stage}/{table}/;
            '''
            staged_files = self.default_db.read_data(list_stage_query)
            list_to_compare = [int(x['name'].strip(".csv.gz").split("_")[-1]) for x in staged_files]
            max_extracted.append(max(list_to_compare, default=-1))

        min_max_extracted = min(max_extracted)

        start_block = max(last_block, min_max_extracted) + 1

        result = subprocess.run(
        [ "geth"
        , "attach"
        , blockchain_url
        , "--exec"
        , ''"eth.blockNumber"''
        ]
        , capture_output=True
        , text=True)

        end_block = int(result.stdout.strip('\n'))

        while end_block > start_block:

            if end_block > start_block + blocks_per_file:
                stop_block = start_block + blocks_per_file
            else:
                stop_block = end_block + 1

            file_name = f"data_blocks_{start_block}_{stop_block - 1}.csv"

            print(f"Exporting blocks and transactions for {file_name}")
            subprocess.run(
                    [ "ethereumetl"
                    , "export_blocks_and_transactions"
                    , "--start-block"
                    , str(start_block)
                    , "--end-block"
                    , str(stop_block - 1)
                    , "--blocks-output"
                    , "data_downloads/blocks/" + file_name
                    , "--transactions-output"
                    , "data_downloads/transactions/" + file_name
                    , "--provider-uri"
                    , blockchain_url
                    , "--max-workers"
                    , f"{max_workers}"
                    ])

            print(f"Extracting transaction hashes for {file_name}")
            subprocess.run(
                [ "ethereumetl"
                , "extract_csv_column"
                , "--input"
                , f"data_downloads/transactions/{file_name}"
                , "--column"
                , "hash"
                , "--output"
                , "data_downloads/transactions/hashes.txt"
                ])

            print(f"Putting blocks into snowflake for {file_name}")
            put_blocks_query = f'''

                USE SCHEMA {schema};

                PUT file://{current_directory}/data_downloads/blocks/{file_name} @{stage}/blocks/ auto_compress=true;

                '''
            self.default_db.execute(put_blocks_query)
            remove(f'data_downloads/blocks/{file_name}')

            print(f"Putting transactions into snowflake for {file_name}")
            put_transactions_query = f'''

                USE SCHEMA {schema};

                PUT file://{current_directory}/data_downloads/transactions/{file_name} @{stage}/transactions/ auto_compress=true;

                '''
            self.default_db.execute(put_transactions_query)
            remove(f'data_downloads/transactions/{file_name}')

            print(f"Exporting receipts and logs for {file_name}")
            subprocess.run(
                [ "ethereumetl"
                , "export_receipts_and_logs"
                , "--transaction-hashes"
                , "data_downloads/transactions/hashes.txt"
                , "--provider-uri"
                , blockchain_url
                , "--receipts-output"
                , f"data_downloads/receipts/{file_name}"
                , "--logs-output"
                , f"data_downloads/logs/{file_name}"
                , "--max-workers"
                , f"{max_workers}"
                ])
            remove('data_downloads/transactions/hashes.txt')

            print(f"Exporting token transfers for {file_name}")
            subprocess.run(
                [ "ethereumetl"
                , "extract_token_transfers"
                , "--logs"
                , f"data_downloads/logs/{file_name}"
                , "--output"
                , f"data_downloads/token_transfers/{file_name}"
                , "--max-workers"
                , f"{max_workers}"
                ])

            print(f"Putting logs into snowflake for {file_name}")
            self.default_db.execute(f'''
                USE SCHEMA {schema};

                PUT file://{current_directory + '/data_downloads/logs/' + file_name} @{stage}/logs/ auto_compress=true;
            '''
            )
            remove(f"data_downloads/logs/{file_name}")

            print(f"Putting token transfers into snowflake for {file_name}")
            self.default_db.execute(f'''
                USE SCHEMA {schema};

                PUT file://{current_directory + '/data_downloads/token_transfers/' + file_name} @{stage}/token_transfers/ auto_compress=true;
            '''
            )
            remove(f"data_downloads/token_transfers/{file_name}")

            print(f"Extracting contract addresses for {file_name}")
            subprocess.run(
                [ "ethereumetl"
                , "extract_csv_column"
                , "--input"
                , f"data_downloads/receipts/{file_name}"
                , "--column"
                , "contract_address"
                , "--output"
                , "data_downloads/contract_addresses.txt"
                ])

            print(f"Putting receipts into snowflake for {file_name}")
            self.default_db.execute(f'''
                USE SCHEMA {schema};

                PUT file://{current_directory + '/data_downloads/receipts/' + file_name} @{stage}/receipts/ auto_compress=true;
            '''
            )
            remove(f"data_downloads/receipts/{file_name}")

            print(f"Export contracts for {file_name}")
            subprocess.run(
                [ "ethereumetl"
                , "export_contracts"
                , "--contract-addresses"
                , "data_downloads/contract_addresses.txt"
                , "--provider-uri"
                , blockchain_url
                , "--output"
                , f"data_downloads/contracts/{file_name}"
                , "--batch-size"
                , "1"
                , "--max-workers"
                , f"{max_workers}"
                ])

            print(f"Extract token addresses for {file_name}")
            subprocess.run(
                [ "ethereumetl"
                , "filter_items"
                , "-i"
                , f"data_downloads/contracts/{file_name}"
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

            print(f"Exporting tokens for {file_name}")
            subprocess.run(
                [ "ethereumetl"
                , "export_tokens"
                , "--token-addresses"
                , "data_downloads/token_addresses.txt"
                , "--provider-uri"
                , blockchain_url
                , "--output"
                , f"data_downloads/tokens/{file_name}"
                , "--max-workers"
                , f"{max_workers}"
                ])

            print(f"Putting contracts into snowflake for {file_name}")
            self.default_db.execute(f'''
                USE SCHEMA {schema};

                PUT file://{current_directory + '/data_downloads/contracts/' + file_name} @{stage}/contracts/ auto_compress=true;
            '''
            )
            remove(f"data_downloads/contracts/{file_name}")

            print(f"Putting tokens into snowflake for {file_name}")
            self.default_db.execute(f'''
                USE SCHEMA {schema};

                PUT file://{current_directory + '/data_downloads/tokens/' + file_name} @{stage}/tokens/ auto_compress=true;
            '''
            )
            remove(f"data_downloads/tokens/{file_name}")
            remove("data_downloads/contract_addresses.txt")
            remove("data_downloads/token_addresses.txt")

            start_block += blocks_per_file

            if is_test == 'True':
                break

        return self.success()
