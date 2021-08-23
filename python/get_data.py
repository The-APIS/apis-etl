from os import getcwd, makedirs, path

from sayn import PythonTask


class GetData(PythonTask):

    def run(self):

        file_format = self.parameters["file_format"]
        stage = self.parameters["stage"]
        schema = self.parameters["schema"]
        table = self.parameters["table"]
        full_path = getcwd() + self.parameters["path"]

        if path.isdir(full_path):
            pass
        else:
            makedirs(full_path)


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

        GET @{stage}/{table}/ file://{full_path};

        DROP STAGE {stage};

        '''

        self.default_db.execute(sql_query)

        return self.success()
