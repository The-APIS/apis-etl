from sayn import PythonTask

class getData(PythonTask):

    def run(self):

        file_format = self.parameters["file_format"]
        stage = self.parameters["stage"]
        schema = self.parameters["schema"]
        table = self.parameters["table"]
        path = self.parameters["path"]


        sql_query = f'''

        USE SCHEMA {schema};

        CREATE OR REPLACE FILE FORMAT {file_format}
          TYPE = 'CSV'
          FIELD_DELIMITER = '\t'
          SKIP_HEADER = 1
          NULL_IF = ('0000-00-00 00:00:00')
          EMPTY_FIELD_AS_NULL = true
          FIELD_OPTIONALLY_ENCLOSED_BY = '"'
          ;

        CREATE OR REPLACE stage {stage}
          file_format = {file_format};

        COPY INTO @{stage}/{table}/
          from {table};

        GET @{stage}/{table}/ file:///{path};

        DROP STAGE {stage};

        '''

        self.default_db.execute(sql_query)

        #"GET @analytics_models.unload_stage/f_ethereum_token_summary/ file:///Users/tim/get_test"
        return self.success()
