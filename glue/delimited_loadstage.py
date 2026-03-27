import csv
import json
import boto3
import sys
import os
import ast
from decimal import Decimal
from datetime import datetime
from datetime import timedelta
from sqlalchemy import *
import splunk
from awsglue.utils import getResolvedOptions

from sqlalchemy import Table, MetaData, text

s3_client = boto3.client('s3')
client = boto3.client("secretsmanager")


def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:dpmdev-di-glue-delimited_loadstage:" + account_id


def get_secret_values(secret_name):
    try:
        print(f"get_secret_values called with secret_name: {secret_name}")
        splunk.log_message({'Message': f"Retrieving secret values for {secret_name}.", "Status": "INFO"},
                           get_run_id())
        response = client.get_secret_value(SecretId=secret_name)
        secret_value = response['SecretString']
        json_secret_value = json.loads(secret_value)
        print(f"json_secret_value : {json_secret_value}")
        splunk.log_message({'Message': f"Secret values retrieved successfully for {secret_name}.", "Status": "SUCCESS"},
                           get_run_id())
        return json_secret_value
    except Exception as e:
        splunk.log_message({'Message': f"Error retrieving secret values for {secret_name}: {e}", "Status": "ERROR"},
                           get_run_id())
        raise e


def create_connection(ssmdbkey, region, database):
    try:
        print(f"create_connection called with ssmdbkey: {ssmdbkey}, region: {region}, database: {database}")
        splunk.log_message(
            {"Message": f"ssmdbkey : [{ssmdbkey}] ==> region: [{region}] ==> database: [{database}]", "Status": "INFO"},
            get_run_id())
        ssm = boto3.client('ssm', region_name=region)
        secret_name = ssm.get_parameter(Name=ssmdbkey, WithDecryption=True)['Parameter']['Value']
        creds = get_secret_values(secret_name)
        print(f"creds : {creds} ==> .<")
        return create_engine(
            f"postgresql://{creds['username']}:{creds['password']}@{creds['host']}:{creds['port']}/{database}",
            future=True,
            echo=True)
    except Exception as e:
        print(f"Error creating database connection: {e}")
        splunk.log_message({'Message': f"Error creating database connection: {e}", "Status": "ERROR"}, get_run_id())
        raise Exception(f"Error creating database connection: {e}")


def create_table(engine, table_name, headers, schema):
    """
    Dynamically create a table based on the headers of the delimited file.
    """
    print(f"create_table called with table_name: {table_name}, headers: {headers}, schema: {schema}")
    splunk.log_message(
        {"Message": f"Creating table: {table_name} with headers: {headers} and schema: {schema}", "Status": "INFO"},
        get_run_id())
    try:
        metadata = MetaData(schema=schema)

        dot_index = table_name.rfind('.')
        table_name = table_name[:dot_index] if dot_index != -1 else table_name
        table_name = table_name.rsplit('.', 1)[-1]

        # Dynamically create columns based on headers
        # columns = [Column('ID', BigInteger, primary_key=True)]
        # print(f"Initial columns: {columns}")
        # splunk.log_message({"Message": f"Initial columns: {columns}", "Status": "INFO"}, get_run_id())
        columns = []
        for header in headers:
            columns.append(Column(header, String))

        print(f"Final columns: {columns}")
        splunk.log_message({"Message": f"Final columns: {columns}", "Status": "INFO"}, get_run_id())
        my_table = Table(
            table_name,
            metadata,
            *columns
        )
        print(f"Table object created: {my_table}")
        splunk.log_message({"Message": f"Table object created: {my_table}", "Status": "INFO"}, get_run_id())

        ins = inspect(engine)
        print(f"Inspecting engine for table existence: {table_name}")
        splunk.log_message({"Message": f"Inspecting engine for table existence: {table_name}", "Status": "INFO"},
                           get_run_id())
        if not ins.dialect.has_table(engine.connect(), table_name):
            print(f"Creating table: {table_name} with columns: {columns}")
            splunk.log_message({"Message": f"Creating table: {table_name} with columns  : {columns}", "Status": "INFO"},
                               get_run_id())
            metadata.create_all(engine)
            print(f"{table_name} table created successfully...")
            splunk.log_message(
                {'Message': f"{table_name} table created successfully... ", "TableName": table_name,
                 "Status": "SUCCESS"},
                get_run_id())
        else:
            print(f"{table_name} table already exists... Dropping and recreating.")
            splunk.log_message(
                {'Message': f"{table_name} table already exists... Dropping and recreating.", "TableName": table_name,
                 "Status": "INFO"}, get_run_id())
            my_table.drop(engine, checkfirst=True)
            metadata.create_all(engine)
            print(f"{table_name} table recreated successfully...")
            splunk.log_message(
                {'Message': f"{table_name} table recreated successfully... ", "TableName": table_name,
                 "Status": "SUCCESS"},
                get_run_id())

        return my_table
    except Exception as e:
        splunk.log_message({'Message': f"Error creating table: {e}", "TableName": table_name, "Status": "ERROR"},
                           get_run_id())
        raise e


def load_stage(event):
    """
    Load data from a delimited file and insert it into the database.
    """
    # engine = create_connecton(event["dbkey"])

    print("Starting load_stage...")
    splunk.log_message({"Message": "Starting load_stage...", "Status": "INFO"}, get_run_id())

    key_location_check = event.get("key_location")
    if not key_location_check:
        print("The 'key_location' environment variable must be set and non-empty.")
        splunk.log_message(
            {"Message": "The 'key_location' environment variable must be set and non-empty.", "Status": "ERROR"},
            get_run_id())
        raise ValueError("The 'key_location' environment variable must be set and non-empty.")
    print(f"Key location check: {key_location_check}")
    splunk.log_message({"Message": f"key_location_check{key_location_check}", "Status": "SUCCESS"}, get_run_id())

    # Load the key_location from environment variable
    print("Loading key_location from environment variable...")
    data = json.loads(event.get("key_location"))
    print(f"Loaded key_location: {data}")
    splunk.log_message({"Message": f"Loaded key_location: {data}", "Status": "INFO"}, get_run_id())

    table_name = ""
    database = ""
    schema = ""
    delimiter = ""
    retention_days = ""
    headers = None
    # Iterate over the dictionary
    for key in data:
        print("key ->" + key)
        print("event.get('file_name') -> " + event.get("file_name"))
        if key in event.get("file_name"):
            delimiter = data[key]['delimiter']
            database = data[key]['database']
            schema = data[key]['schema']
            table_name = data[key]['table_name']
            retention_days = int(data[key].get('retention_days', 150))

    print(f"Final table_name: {table_name}")
    splunk.log_message({"Message": f"Final table_name: {table_name}", "Status": "INFO"}, get_run_id())
    print(f"Final database: {database}")
    splunk.log_message({"Message": f"Final database: {database}", "Status": "INFO"}, get_run_id())
    print(f"Final schema: {schema}")
    splunk.log_message({"Message": f"Final schema: {schema}", "Status": "INFO"}, get_run_id())
    print(f"Final delimiter: {delimiter}")
    splunk.log_message({"Message": f"Final delimiter: {delimiter}", "Status": "INFO"}, get_run_id())
    print(f"Final retention_days: {retention_days}")
    splunk.log_message({"Message": f"Final retention_days: {retention_days}", "Status": "INFO"}, get_run_id())

    if not table_name:
        print("The 'table_name' within key_location environment variable must be set and non-empty.")
        splunk.log_message(
            {"Message": "The 'table_name' within key_location environment variable must be set and non-empty.",
             "Status": "ERROR"}, get_run_id())
        raise ValueError("The 'table_name' within key_location environment variable must be set and non-empty.")

    if not delimiter:
        print("The 'delimiter' within key_location environment variable must be set and non-empty.")
        splunk.log_message(
            {"Message": "The 'delimiter' within key_location environment variable must be set and non-empty.",
             "Status": "ERROR"}, get_run_id())
        raise ValueError("The 'delimiter' within key_location environment variable must be set and non-empty.")

    if not database:
        print("The 'database' within key_location environment variable must be set and non-empty.")
        splunk.log_message(
            {"Message": "The 'database' within key_location environment variable must be set and non-empty.",
             "Status": "ERROR"}, get_run_id())
        raise ValueError("The 'database' within key_location environment variable must be set and non-empty.")

    if not schema:
        print("The 'schema' within key_location environment variable must be set and non-empty.")
        splunk.log_message(
            {"Message": "The 'schema' within key_location environment variable must be set and non-empty.",
             "Status": "ERROR"}, get_run_id())
        raise ValueError("The 'schema' within key_location environment variable must be set and non-empty.")

    print("Creating database connection...")
    splunk.log_message({"Message": "Creating database connection...", "Status": "INFO"}, get_run_id())
    engine = create_connection(event.get("dbkey"), event.get("region"), database)
    print("Database connection created successfully.")
    splunk.log_message({"Message": "Database connection created successfully.", "Status": "SUCCESS"}, get_run_id())

    try:
        bucket_name = event.get("bucket_name")
        object_key = event.get("object_key")
        print(f"Trying to get S3 object: bucket={bucket_name}, key={object_key}")
        splunk.log_message({"Message": "Trying to get S3 object...", "Status": "INFO"}, get_run_id())
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        print("S3 object retrieved successfully.")
        splunk.log_message({"Message": "S3 object retrieved successfully.", "Status": "SUCCESS"}, get_run_id())

        # Read the delimited file
        file_content = response['Body'].read().decode('utf-8')
        print(f"file_content: {file_content}")
        splunk.log_message({"Message": f"File content read successfully.", "Status": "SUCCESS"}, get_run_id())

        # Parse the delimited file
        reader = csv.reader(file_content.splitlines(), delimiter=delimiter)
        headers = next(reader)  # Extract the header row
        print(f"Parsed headers: {headers}")
        splunk.log_message({"Message": f"Successfully parsed headers: {headers}", "Status": "SUCCESS"}, get_run_id())

        # Create the table based on the headers
        print("Creating table...")
        splunk.log_message({"Message": "Creating table...", "Status": "INFO"}, get_run_id())
        # table = create_table(engine, table_name, headers, schema)
        table = create_table(engine, event.get("file_name"), headers, schema)
        print("Table created successfully.")
        splunk.log_message({"Message": "Table created successfully.", "Status": "SUCCESS"}, get_run_id())

        batch_size = event.get("batch_size", 1000)
        print(f"Batch size set to: {batch_size}")
        splunk.log_message({"Message": f"Batch size set to: {batch_size}", "Status": "INFO"}, get_run_id())

        try:
            with engine.begin() as conn:
                batch_data = []
                for row_num, row in enumerate(reader, start=2):
                    print(f"Processing row {row_num}: {row}")
                    splunk.log_message({"Message": f"Processing row {row_num}: {row}", "Status": "INFO"}, get_run_id())
                    if len(row) != len(headers):
                        print(
                            f"Row {row_num} length {len(row)} does not match headers length {len(headers)}. Rolling back.")
                        splunk.log_message({
                            "Message": f"Row {row_num} length {len(row)} does not match headers length {len(headers)}. Rolling back.",
                            "Status": "ERROR"
                        }, get_run_id())
                        raise ValueError(
                            f"Row {row_num} length {len(row)} does not match headers length {len(headers)}.")
                    # Convert row to a dictionary with headers as keys
                    print(f"Row {row_num} data: {row}")
                    splunk.log_message({"Message": f"Row {row_num} data: {row}", "Status": "INFO"}, get_run_id())
                    data_to_insert = {headers[i]: row[i] for i in range(len(headers))}
                    print(f"Data to insert: {data_to_insert}")
                    splunk.log_message({"Message": f"Data to insert: {data_to_insert}", "Status": "INFO"}, get_run_id())
                    batch_data.append(data_to_insert)

                    if len(batch_data) >= batch_size:
                        print(f"Inserting batch of {len(batch_data)} rows into {table.name}.")
                        splunk.log_message({"Message": f"Inserting batch of {len(batch_data)} rows into {table.name}.",
                                            "Status": "INFO"}, get_run_id())
                        conn.execute(table.insert(), batch_data)
                        print(f"Batch inserted successfully.")
                        splunk.log_message({"Message": f"Batch inserted successfully.", "Status": "SUCCESS"},
                                           get_run_id())
                        batch_data = []

                # Insert any remaining rows
                if batch_data:
                    print(f"Inserting final batch of {len(batch_data)} rows into {table.name}.")
                    splunk.log_message(
                        {"Message": f"Inserting final batch of {len(batch_data)} rows into {table.name}.",
                         "Status": "INFO"}, get_run_id())
                    conn.execute(table.insert(), batch_data)
                    print(f"Final batch inserted successfully.")
                    splunk.log_message({"Message": f"Final batch inserted successfully.", "Status": "SUCCESS"},
                                       get_run_id())

            print("All rows inserted successfully.")
            splunk.log_message({"Message": "All rows inserted successfully.", "Status": "SUCCESS"}, get_run_id())
        except Exception as e:
            print(f"Error during insert: {e}")
            splunk.log_message({"Message": f"Error during insert: {e}", "Status": "ERROR"}, get_run_id())
            raise

        file_name_table = create_file_type_table(engine, table, table_name, headers, schema)
        print(f"File name table created successfully: {file_name_table}")
        splunk.log_message({"Message": f"File name table created successfully: {file_name_table}", "Status": "SUCCESS"},
                           get_run_id())

        if retention_days != -1:
            print(f"Retention days set to {retention_days}, deleting old tables if any.")
            splunk.log_message(
                {"Message": f"Retention days set to {retention_days}, deleting old tables if any.", "Status": "INFO"},
                get_run_id())
            delete_old_tables(engine, table_name, retention_days, schema)
        else:
            print("Retention days is set to -1, skipping old table deletion.")
            splunk.log_message(
                {"Message": "Retention days is set to -1, skipping old table deletion.", "Status": "INFO"},
                get_run_id())

    except Exception as e:
        print(f"Error during load_stage: {e}")
        splunk.log_message({"Message": f"Error during load_stage: {e}", "Status": "ERROR"}, get_run_id())
        raise e


def create_file_type_table(engine, file_name_table, table_name, headers, schema):
    """
    Create a flat file_type_table with all header columns (no ID, no JSON),
    copy the data from the staging table, and create an index across all columns.
    """
    print(f"create_file_type_table called: table_name={table_name}, headers={headers}, schema={schema}")
    splunk.log_message({"Message": f"Creating file type table {schema}.{table_name} with headers {headers}",
                        "Status": "INFO"}, get_run_id())
    try:
        metadata = MetaData(schema=schema)

        # Ensure table_name is clean (no schema prefix)
        dot_index = table_name.rfind('.')
        table_name_clean = table_name[dot_index+1:] if dot_index != -1 else table_name

        # Define a clean table (just header columns)
        columns = [Column(header, String) for header in headers]
        file_type_table = Table(table_name_clean, metadata, *columns)

        # Drop existing table and recreate to ensure schema match
        with engine.connect() as conn:
            if engine.dialect.has_table(conn, table_name_clean, schema=schema):
                file_type_table.drop(engine, checkfirst=True)
        metadata.create_all(engine)

        # Copy data from staging table into the newly created file_type_table
        with engine.begin() as connection:
            select_stmt = file_name_table.select()
            insert_stmt = file_type_table.insert().from_select(
                names=[c.name for c in file_name_table.columns],
                select=select_stmt
            )
            connection.execute(insert_stmt)

        # ---- Create FULL INDEX across all columns ----
        index_name = f"{table_name_clean}_indx_1"
        column_list = ', '.join([f'"{col}"' for col in headers])
        index_sql = f'CREATE INDEX "{index_name}" ON "{schema}"."{table_name_clean}" ({column_list});'
        with engine.begin() as connection:
            connection.execute(text(index_sql))

        msg = f"File type table {schema}.{table_name_clean} created with index {index_name} on columns {headers}"
        print(msg)
        splunk.log_message({"Message": msg, "Status": "SUCCESS"}, get_run_id())

        return file_type_table

    except Exception as e:
        msg = f"Error creating file_type_table {table_name}: {e}"
        print(msg)
        splunk.log_message({"Message": msg, "Status": "ERROR"}, get_run_id())
        raise e
def delete_old_tables(engine, base_table_name, days_old, schema):
    """
    Deletes tables starting with base_table_name and created before n days.
    """
    print(f"delete_old_tables called with engine: {engine}, base_table_name: {base_table_name}, days_old: {days_old}")
    splunk.log_message({
                           'Message': f"delete_old_tables called with engine: {engine}, base_table_name: {base_table_name}, days_old: {days_old}",
                           "Status": "INFO"}, get_run_id())
    try:

        # Calculate the cutoff date
        cutoff_date = datetime.now() - timedelta(days=days_old)
        print(f"Cutoff date for table deletion: {cutoff_date}")
        splunk.log_message({"Message": f"Cutoff date for table deletion: {cutoff_date}", "Status": "INFO"},
                           get_run_id())

        table_delete_count = 0
        # Get the list of tables and filter by base_table_name
        inspector = inspect(engine)

        tables = [table for table in inspector.get_table_names(schema=schema) if table.startswith(base_table_name)]
        print(f"Tables to be checked for deletion: {tables}")
        splunk.log_message({"Message": f"Tables to be checked for deletion: {tables}", "Status": "INFO"}, get_run_id())
        for table in tables:
            # Get the table creation date (assuming the table name contains the date in YYYYMMDD format)
            date_str = table[len(base_table_name) + 1:]
            try:
                table_date = datetime.strptime(date_str, '%Y-%m-%dT%H%M%S')
                print(f"Table {table} creation date: {table_date}")
                splunk.log_message({"Message": f"Table {table} creation date: {table_date}", "Status": "INFO"},
                                   get_run_id())
                if table_date < cutoff_date:
                    print(f"Dropping table {table} as it is older than {days_old} days.")
                    splunk.log_message(
                        {"Message": f"Dropping table {table} as it is older than {days_old} days.", "Status": "INFO"},
                        get_run_id())
                    table_to_drop = Table(table, MetaData(schema=schema))
                    table_to_drop.drop(engine)
                    table_delete_count = table_delete_count + 1
                    print(f"Table {table} dropped successfully.")
                    splunk.log_message({"Message": f"Table {table} dropped successfully.", "Status": "SUCCESS"},
                                       get_run_id())
                else:
                    print(f"Table {table} is not older than {days_old} days, skipping deletion.")
                    splunk.log_message(
                        {"Message": f"Table {table} is not older than {days_old} days, skipping deletion.",
                         "Status": "INFO"}, get_run_id())
            except ValueError:
                print(f"Table {table} does not have a valid date format, skipping deletion.")
                splunk.log_message({"Message": f"Table {table} does not have a valid date format, skipping deletion.",
                                    "Status": "INFO"}, get_run_id())
                continue

        print(f"Total number of tables deleted: {table_delete_count}")
        splunk.log_message({"Message": f"Total number of tables deleted: {table_delete_count}", "Status": "INFO"},
                           get_run_id())
    except Exception as e:
        print(f"Error deleting old tables: {e}")
        splunk.log_message({"Message": f"Error deleting old tables: {e}", "Status": "ERROR"}, get_run_id())
        raise e


def main():
    try:
        event = getResolvedOptions(sys.argv,
                                   ['bucket_name', 'object_key', 'dbkey', 'region', 'file_name', 'base_file_name',
                                    'key_location'])

        load_stage(event)
    except Exception as e:
        splunk.log_message({"Message": f"Error during load_stage: {e}", "Status": "ERROR"}, {'status_code': 500})
        raise e


if __name__ == '__main__':
    main()
