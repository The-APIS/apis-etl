from sayn import PythonTask
import subprocess


class loadData(PythonTask):

    def run(self):

        select_query = f'''

        SELECT MAX(NUMBER) AS last_block

        FROM analytics_logs.bsc_blocks;
        '''

        data = self.default_db.read_data(select_query)

        last_block = data[0]["last_block"]

        print(type(last_block))

        result = subprocess.run(
        [ "geth"
        , "attach"
        , "https://bsc-dataseed.binance.org"
        , "--exec"
        , ''"eth.blockNumber"''
        ]
        , capture_output=True
        , text=True)

        current_block = result.stdout.strip('\n')

        if int(current_block) > last_block:
            print(f"\nrange for blocks to load:\nstarting block: {last_block + 1}\nfinish_block: {current_block}")


        return self.success()
