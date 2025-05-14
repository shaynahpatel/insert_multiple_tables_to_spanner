import logging
from google.cloud import spanner
import apache_beam as beam
from google.api_core import exceptions



class CreateSpannerTables:
    def __init__(self, spanner_instance_id, spanner_database_id, project_id, pk_map):
        self.spanner_instance_id = spanner_instance_id
        self.spanner_database_id = spanner_database_id
        self.pk_map = pk_map
        self.spanner_client = None
        self.database = None
        self.project_id = project_id
      
        
    def _convert_type(self, neo4j_type):
        type_mapping = {
            "String": "STRING(MAX)",
            "Long": "INT64",
            "Integer": "INT64", 
            "Double": "FLOAT64",
            "Float": "FLOAT64",
            "Boolean": "BOOL",
            "Date": "STRING(MAX)",
            "DateTime": "TIMESTAMP",
            "Point": "STRING(MAX)",
            "StringArray": "ARRAY<STRING(MAX)>",
            "LongArray": "ARRAY<INT64>",
            "null" : "STRING(MAX)"
        }
        spanner_type = type_mapping.get(neo4j_type)
        if spanner_type is None:
            return "STRING(MAX)"
        return spanner_type
        
    def process(self, tables_and_columns):
        self.spanner_client = spanner.Client(project=self.project_id)
        instance = self.spanner_client.instance(self.spanner_instance_id)
        self.database = instance.database(self.spanner_database_id)

        for table_name, columns in tables_and_columns.items():
            pk_column = self.pk_map[table_name]
            
            column_defs = []
            for column_name, column_date_type in columns.items():
                spanner_type = self._convert_type(column_date_type)
                column_defs.append(f"{column_name} {spanner_type}")

            ddl_statement = f"CREATE TABLE `{table_name}` ({', '.join(column_defs)}) PRIMARY KEY (`{pk_column}`)"

            try:
                operation = self.database.update_ddl([ddl_statement])
                operation.result()
                logging.info(f"CreateSpannerTables: Created table '{table_name}' with primary key '{pk_column}'.")
    
            except exceptions.AlreadyExists:
                logging.info(f"CreateSpannerTables: Table '{table_name}' already exists.")

            except exceptions.FailedPrecondition as e:
                if "Duplicate name in schema" in str(e):
                    logging.warning(f"CreateSpannerTables: Table '{table_name}' already exists.")
                else:
                    raise e
            except Exception as ddl_error:
                logging.error(f"CreateSpannerTables: Error creating table '{table_name}': {ddl_error}")
                raise ddl_error