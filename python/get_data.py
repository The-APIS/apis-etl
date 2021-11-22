# Used to create csv files from tables in snowflake, basically a reverse copy

from os import getcwd, makedirs, path

from sayn import PythonTask


class GetData(PythonTask):

    def run(self):

        file_format = self.parameters["file_format"]
        stage = self.parameters["stage"]
        schema = self.parameters["schema"]
        table = self.parameters["table"]
        columns = self.parameters["json_table_columns"]
        full_path = getcwd() + self.parameters["path"]

        if path.isdir(full_path):
            pass
        else:
            makedirs(full_path)

        file_type_options = {}

        file_type_options["csv_unloading"] = {
            "TYPE": "'CSV'",
            "FIELD_DELIMITER": "'\t'",
            "SKIP_HEADER": "1",
            "NULL_IF": "('0000-00-00 00:00:00')",
            "FIELD_OPTIONALLY_ENCLOSED_BY": ""'"'""
        }

        file_type_options["json_unloading"] = {
            "TYPE": "JSON",
            "NULL_IF": "('0000-00-00 00:00:00')",
            "COMPRESSION": "NONE"
        }

        if file_format not in file_type_options.keys():
            return self.fail("Incorrect file format name")

        if file_format == "json_unloading" and columns == None:
            return self.fail("Column specification required for json unloading")

        sql_query = f'''

        USE SCHEMA {schema};

        CREATE OR REPLACE FILE FORMAT {file_format}
        '''

        for option in file_type_options[file_format].keys():
            sql_query += f"{option} = {file_type_options[file_format][option]}\n"

        sql_query += f'''
          ;

        CREATE OR REPLACE stage {stage}
          file_format = {file_format};

        COPY INTO @{stage}/{table}/
        '''

        if file_format == "csv_unloading":
            sql_query += f"from {table};\n"

        # In the case of json unload, we need an object constructor
        elif file_format == "json_unloading":
            sql_query += "from(select object_construct("
            for column in columns:
                sql_query += f"'{column}',{column},"
            sql_query = sql_query[:-1] + f'''
                ) from {table})
                file_format = {file_format};
                '''

        sql_query += f'''
        GET @{stage}/{table}/ file://{full_path};

        DROP STAGE {stage};

        '''

        self.debug(sql_query)
        self.default_db.execute(sql_query)

        return self.success()
