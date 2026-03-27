import json
import boto3
import os
from sqlalchemy import *
import traceback
from awsglue.utils import getResolvedOptions
import sys
import splunk
from datetime import datetime, timedelta

s3_client = boto3.client('s3')


account_id = boto3.client("sts").get_caller_identity()["Account"]

def get_run_id():
    return "arn:dpm:glue:dpmdev-di-glue-load_stage:" + account_id


def getDatabaseParams(dbParamsKey):
    try:
        
        
        ssm = boto3.client('ssm')
        databaseDetails = ssm.get_parameter(Name=dbParamsKey, WithDecryption=True)
        databaseDetails = databaseDetails['Parameter']['Value']
        dbConnectionParams = json.loads(databaseDetails)        
        return dbConnectionParams["host"], dbConnectionParams["port"], dbConnectionParams["username"], dbConnectionParams["password"]
    except Exception as e:
        message = f'error retrieving connection details for database : {traceback.format_exc()}'
        splunk.log_message({'Status': 'ERROR', 'Message': message}, get_run_id())
        
        raise e


def create_connecton(dbParamsKey, database):
    try:
        host, port, username, password = getDatabaseParams(dbParamsKey)

        conn_engine = create_engine(
            f"postgresql://{username}:{password}@{host}:{port}/{database}", echo=True)
        

        
        
        splunk.log_message({"Message": f"Database connection created successfully.", "Status": "SUCCESS"}, get_run_id())
        return conn_engine
    except Exception as e:
        splunk.log_message({"Message": f"Error creating database connection: {e}.", "Status": "ERROR"},
                           get_run_id())

        raise e


def create_table(engine, unique_key, table_name, schema):
    """
    This function will create the tables required for the pgAdmin 4 database
    """
    try:
        metadata = MetaData(schema=schema)
        dot_index = table_name.rfind('.')
        table_name = table_name[:dot_index] if dot_index != -1 else table_name

        table_name = table_name.rsplit('.', 1)[-1]
        my_table = Table(
            table_name,
            metadata,
            Column('ID', BigInteger, primary_key=True),
            Column(unique_key, String),
            Column('JSON_DATA', JSON),
        )

        #        full_table_name = f"AUX.{table_name}"
        full_table_name = f"{schema}.{table_name}"

        if not full_table_name in metadata.tables:
            metadata.create_all(engine)

            splunk.log_message(
                {'Message': f"{table_name} table created successfully... ", "TableName": table_name,
                 "Status": "SUCCESS"},
                get_run_id())
        else:
            my_table.drop(engine, checkfirst=True)
            metadata.create_all(engine)
            splunk.log_message(
                {'Message': f"{table_name} table already exists... ", "TableName": table_name, "Status": "SUCCESS"},
                get_run_id())

        return my_table
    except Exception as e:
        splunk.log_message({'Message': f"Error creating table: {e}", "TableName": table_name, "Status": "ERROR"},
                           get_run_id())
        raise e


def load_stage():
    """
    This loads the elements needed to create the tables required for the pgAdmin 4 database.  It will call the function
    that actually creates the tables.
    """
    #    engine = create_connecton(os.getenv("dbkey"))
    try:
        response = s3_client.get_object(Bucket=os.getenv("bucket_name"), Key=os.getenv("object_key"))

        json_content = response['Body'].read().decode('utf-8')
        splunk.log_message({"Message": f"Successfully read S3 object.", "Status": "SUCCESS"}, get_run_id())
        load_data = json.loads(json_content)
        splunk.log_message({"Message": f"Successfully read and parsed JSON data.", "Status": "SUCCESS"},
                           get_run_id())

        # The following block of code is to parse the Json string from the argument to determine which field has the key
        # to use for the database.
        key_location_check = os.getenv("key_location")
        splunk.log_message({"Message": f"key_location_check{key_location_check}", "Status": "SUCCESS"}, get_run_id())

        table_key_index = " "
        database = " "
        schema = " "

        # Parse the JSON string into a dictionary
        data = json.loads(os.getenv("key_location"))
        # splunk.log_message({"Message": f"key location: {os.getenv("key_location")}", "Status": "log"}, get_run_id())
        # splunk.log_message({"Message": f"file name: {os.getenv("file_name")}", "Status": "log"}, get_run_id())

        # Iterate over the dictionary
        for key in data:
            if key in os.getenv("file_name"):
                key_index = data[key]['key_index']
                database = data[key]['database']
                schema = data[key]['schema']

                table_key_index = key_index
                base_name = key
                if 'retention_days' in data[key]:
                    retention_days = int(data[key]['retention_days'])
                else:
                    retention_days = -1
                if 'alternate_name' in data[key]:
                    alternate_name = data[key]['alternate_name']
                else:
                    alternate_name = ''
        try:
            key = list(load_data["json_data"][0].keys())[int(table_key_index)]
        except Exception as e:
            key = os.getenv('key')
        splunk.log_message({'Message': "Trying to check key value", "key": key, "Status": "success"}, get_run_id())

        # Creating the conection
        print("database", database)
        engine = create_connecton(os.getenv("dbkey"), database)

        filename = os.getenv("file_name")
        replaced_filename = os.getenv("file_name").replace(base_name, alternate_name)
        if alternate_name:
            file_name_table = create_table(engine, key, os.getenv("file_name").replace(base_name, alternate_name),
                                           schema)
        else:
            file_name_table = create_table(engine, key, os.getenv("file_name"), schema)

        batch_size = int(os.getenv("batch_size"))
        batch_records = []
        for record in load_data["json_data"]:
            data_to_insert = {
                key: record[key],
                'JSON_DATA': record
            }
            batch_records.append(data_to_insert)
            if len(batch_records) == batch_size:
                insert_stmt = insert(file_name_table)
                with engine.connect() as connection:
                    try:
                        result = connection.execute(insert_stmt, batch_records)
                    except Exception as e:
                        splunk.log_message({"Message": f"Error during batch insert: {e}", "Status": "ERROR"},
                                           get_run_id())
                        raise e
                batch_records.clear()
                splunk.log_message({
                    "Message": f"load_stage completed inserting the batch records into the the file name table successfully.",
                    "Status": "SUCCESS"}, get_run_id())

        # Insert remaining records in the last batch
        if batch_records:
            insert_stmt = insert(file_name_table)
            with engine.connect() as connection:
                try:
                    connection.execute(insert_stmt, batch_records)
                except Exception as e:
                    splunk.log_message({"Message": f"Error during batch insert: {e}", "Status": "ERROR"}, get_run_id())
                    raise e

        splunk.log_message({"Message": f"load_stage completed successfully.", "Status": "SUCCESS"},
                           get_run_id())

        if alternate_name:
            file_name_table = create_file_type_table(engine, key, file_name_table, alternate_name, schema)
        else:
            file_name_table = create_file_type_table(engine, key, file_name_table, base_name, schema)

        if retention_days != -1:
            delete_old_tables(engine, base_name, retention_days, schema)
            if alternate_name:
                delete_old_tables(engine, alternate_name, retention_days, schema)

    except Exception as e:
        splunk.log_message({"Message": f"Error during first table create of load_stage: {e}", "Status": "ERROR"},
                           get_run_id())
        raise e


def create_file_type_table(engine, key, file_name_table, table_name, schema):
    # The following block of code was added to create a table with just the base name without a date time stamp.
    # It uses the items found in the "base_file_name" argument to compare to the original file name.  If the
    # base file name is a subset of the original filename, then the base file name will be used.
    # base_array = os.getenv("base_file_name").split("|")
    # for base_name in base_array:
    #     if base_name in os.getenv("file_name"):
    try:
        file_type_table = create_table(engine, key, table_name, schema)

        with engine.connect() as connection:
            select_stmt = file_name_table.select()
            insert_stmt = file_type_table.insert().from_select(
                names=[c.name for c in file_name_table.columns], select=select_stmt)
            connection.execute(insert_stmt)
            splunk.log_message({"Message": f"load_stage completed copying into second table successfully.",
                                "Status": "SUCCESS"},
                               get_run_id())
    except Exception as e:
        splunk.log_message(
            {"Message": f"Error during second table create of load_stage: {e}", "Status": "ERROR"},
            get_run_id())
        raise e


def delete_old_tables(engine, base_table_name, days_old, schema):
    """
    Deletes tables starting with base_table_name and created before n days.
    """
    try:
        message = f'Method delete_old_tables parameters {engine}    {base_table_name} {days_old}'
        splunk.log_message({'Status': 'info', 'Message': message}, get_run_id())

        # Calculate the cutoff date
        cutoff_date = datetime.now() - timedelta(days=days_old)
        splunk.log_message({"Message": f"cutoff_date: {cutoff_date}", "Status": "LOG"}, get_run_id())

        table_delete_count = 0
        # Get the list of tables and filter by base_table_name
        inspector = inspect(engine)

        tables = [table for table in inspector.get_table_names(schema=schema) if table.startswith(base_table_name)]
        for table in tables:
            # Get the table creation date (assuming the table name contains the date in YYYYMMDD format)
            date_str = table[len(base_table_name) + 1:]
            try:
                table_date = datetime.strptime(date_str, '%Y-%m-%dT%H%M%S')
                if table_date < cutoff_date:
                    # Drop the table if it is older than the cutoff date
                    table_to_drop = Table(table, MetaData(schema=schema))
                    table_to_drop.drop(engine)
                    table_delete_count = table_delete_count + 1
                    splunk.log_message({"Message": f"Table {table} dropped successfully.", "Status": "SUCCESS"},
                                       get_run_id())
            except ValueError:
                # If the date format is not correct, skip the table
                continue

        splunk.log_message({"Message": f"Total number of tables deleted: {table_delete_count}", "Status": "SUCCESS"},
                           get_run_id())
    except Exception as e:
        splunk.log_message({"Message": f"Error deleting old tables: {e}", "Status": "ERROR"}, get_run_id())
        raise e


def set_job_params_as_env_vars():
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            value = sys.argv[i + 1]
            os.environ[key] = value
            message = f'Set environment variable {key} to {value}'
            splunk.log_message({'Status': 'info', 'Message': message}, get_run_id())


def main():
    try:
        set_job_params_as_env_vars()
        load_stage()
    except Exception as e:
        splunk.log_message({"Message": f"Error during load_stage: {e}", "Status": "ERROR"}, {'status_code': 500})
        raise e


if __name__ == '__main__':
    main()
