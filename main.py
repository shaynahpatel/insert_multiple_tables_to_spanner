import importlib
import json
import logging
import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import coders
from apache_beam.io.gcp.spanner import SpannerInsertOrUpdate, SpannerInsert

from create_spanner_tables import CreateSpannerTables



def convert_column_type(string_type):
    type_mapping = {
        "String": 'str',
        "Int" : 'int',
        "Long": 'int',
        "Date": 'str',
        "StringArray": 'List[str]',
        "Double": 'float',
        None: 'str',
        "Boolean": 'bool'
    }
    return type_mapping.get(string_type, string_type)


def generated_named_tuple_file(tables_and_columns, pk_map):
    generated_named_tuple_file = "./generated_named_tuples.py"
    with open(generated_named_tuple_file, 'w') as f:
        f.write("from apache_beam import coders\n")
        f.write("from typing import NamedTuple, List, Optional\n")
        f.write("import datetime\n")
        
        #loop through tables_and_columns dict
        for table_name, column_defs in tables_and_columns.items():
            logging.info(f"GENERATING NAMED TUPLE FOR: {table_name}")
            named_tuple_string = []
            
    ##NamedTuples require you write the non-null columns first
            pk = pk_map.get(table_name)
            pk_type = column_defs.get(pk)
            converted_pk_type = convert_column_type(pk_type)
            named_tuple_string.append(f"{pk} : {converted_pk_type}")
            
            ##Write the optional of the columns 
            for column_name, column_type in column_defs.items():
                if column_name != pk:
                    converted_column_type = convert_column_type(column_type)
                    named_tuple_string.append(f"{column_name} : Optional[{converted_column_type}] = None")
            generated_string = f'''class SpannerRow_{table_name}(NamedTuple):
\t{'\n\t'.join(named_tuple_string)}'''
            f.write(generated_string + "\n")

def register_coders(tables_and_columns):
    import generated_named_tuples 
    importlib.reload(generated_named_tuples)
    for table_name, column_defs in tables_and_columns.items():
        class_name = f"SpannerRow_{table_name}"
        target_class = getattr(generated_named_tuples, class_name)
        coders.registry.register_coder(target_class, coders.RowCoder)
        logging.info(f"REGISTERED ROWCODER FOR {class_name}")


#example JSONL
#{table_name: Order, column_data:{orderId: 123, order_status: "Shipped"}, last_modified: 05-14-2025}

class RemoveAndExtract(beam.DoFn):
    def process(self, element):
        if not element or not element.strip():
            pass
        else:
            element = json.loads(element)
            column_data = element["column_data"]
            yield column_data

def main(argv=None, save_main_session=True):
    project_id = '<PROJECT-ID>'
    spanner_instance_id = '<SPANNER INSTANCE ID>'
    spanner_database_id = '<SPANNER DATABASE ID>'

    tables_and_columns = {
        "Order": {"orderId" : "Int","order_status": "String"},
        "Product": {"productId": "Int", "product_name": "String"},
        "Supplier" : {"supplierId" : "Int", "supplier_name" : "String"}
    }

    pk_map = {
        "Order":"orderId",
        "Product":"productId",
        "Supplier":"supplierId"
    }

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output_dir',
        dest='output_dir',
        default='<GCS BUCKET DIRECTORY>',
        help='Output file to write results to.'
    )
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    
    CreateSpannerTables(spanner_instance_id, spanner_database_id, project_id, pk_map).process(tables_and_columns)

    generated_named_tuple_file(tables_and_columns, pk_map)

    
    register_coders(tables_and_columns)

    import generated_named_tuples

    with beam.Pipeline(options=pipeline_options) as p:
        logging.info("IN BEAM PIPELINE")
        ##Loop through multiple tables
        for table in tables_and_columns.keys():
            #Step 1: Read the dump file. Yields each json line
            read_dump = p | f"Read JSON dump file for {table}" >> beam.io.ReadFromText(f'{known_args.output_dir}{table}.jsonl', strip_trailing_newlines=True, validate=False)

            #Step 2: Clean and transform data. Ex: remove empty lines, convert data types, etc. Ensure these steps yeild a dictionary {column_name:value_to_insert}. See example of RemoveAndExtract below
            dump_data = read_dump | f"Remove Empty Lines and extract values for {table}" >> beam.ParDo(RemoveAndExtract())

            #Step 3: Logging
            dump_data | f"Log dump data for {table}" >> beam.Map(lambda x: logging.info(f'DUMP DATA IN PIPELINE:{x}'))  

            #Step 4: Convert rows to appropriate class 
            class_name = f"SpannerRow_{table}"
            target_class = getattr(generated_named_tuples, class_name)
            data_rows = dump_data | f"Convert to {table} row class" >> beam.Map(lambda x, target_class=target_class : target_class(**x)).with_output_types(target_class)

            #Step 5 Insert to Spanner
            data_rows | f"Write to Spanner for {table}" >> SpannerInsertOrUpdate(
                project_id=project_id,
                instance_id=spanner_instance_id,
                database_id=spanner_database_id,
                table=table
            )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()

#command to run
# python main.py --output_dir gs://spanner_insert_multi_retail/ --runner DirectRunner --project prj-shayna-sandbox  --region us-east1 --temp_location gs://neo4j_to_spanner_graph/temp/ --requirements_file ./requirements.txt --subnetwork https://www.googleapis.com/compute/v1/projects/prj-shayna-sandbox/regions/us-east1/subnetworks/default
