from sayn import PythonTask
from os import getcwd, makedirs, path, remove
import subprocess
import gzip
import shutil

class loadData(PythonTask):

    def run(self):

        file_format = self.parameters["file_format"]
        stage = self.parameters["stage"]
        schema = self.parameters["schema"]
        table = self.parameters["table"]
        start_block = self.parameters["start_block"]
        end_block = self.parameters["end_block"]
        blocks_per_file = self.parameters["blocks_per_file"]

        current_directory = getcwd()
        full_path = current_directory + '/data_downloads/' + table
        if path.isdir(full_path):
            pass
        else:
            makedirs(full_path)

        staging_query = f'''

            USE SCHEMA {schema};

            CREATE OR REPLACE FILE FORMAT { file_format }
                 TYPE = 'CSV'
                 FIELD_DELIMITER = ','
                 SKIP_HEADER = 1
                 NULL_IF = ('0000-00-00 00:00:00')
                 EMPTY_FIELD_AS_NULL = true
                 FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                 ;

            CREATE OR REPLACE stage { stage }
                 file_format = { file_format };

            '''
        print(f"Creating Stage: { stage }")
        self.default_db.execute(staging_query)

        put_query = f'''

            USE SCHEMA {schema};

            PUT file://{full_path}/* @{stage}/{table}/ auto_compress=true;

            '''

        while end_block > start_block:

            if end_block > start_block + blocks_per_file:
                stop_block = start_block + blocks_per_file
            else:
                stop_block = end_block

            file_name = f"/data_blocks_{start_block}_{stop_block - 1}.csv"
            relative_path = 'data_downloads/' + table + file_name

            print(f"Creating {file_name}")
            subprocess.run(
                [ "ethereumetl"
                , "export_blocks_and_transactions"
                , "--start-block"
                , str(start_block)
                , "--end-block"
                , str(stop_block - 1)
                , f"--{table}-output"
                , relative_path
                , "--provider-uri"
                , "https://bsc-dataseed.binance.org"
                ])

            print(f"Putting {file_name} into stage")
            self.default_db.execute(put_query)
            print(f"Removing {file_name}")
            remove(full_path + f"{file_name}")

            start_block += blocks_per_file

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
