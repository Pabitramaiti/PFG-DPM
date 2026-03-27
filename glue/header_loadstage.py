import json
import boto3
import sys
import os
import ast
from decimal import Decimal
from datetime import datetime
from sqlalchemy import (
    Table, Column, create_engine, MetaData, Text, String, Numeric, Integer,
    Float, Boolean, Date, DateTime, Time, Enum, JSON, delete, insert, text
)
import splunk
from sqlalchemy.exc import SQLAlchemyError

s3_client = boto3.client('s3')
client = boto3.client("secretsmanager")
####################################################################
def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return f"arn:dpm:glue:dpmdev-di-glue-load_stage:{account_id}"
###################################################################
def get_secret_values(secret_name):
    try:
        response = client.get_secret_value(SecretId=secret_name)
        splunk.log_message({'Message': f"Secret values retrieved successfully", "Status": "SUCCESS"},
                           get_run_id())
        return json.loads(response['SecretString'])
    except Exception as e:
        splunk.log_message({'Message': f"Error retrieving secret values", "Status": "ERROR"},
                           get_run_id())
        raise Exception(f"Error retrieving secret values: {str(e)}")

####################################################################
def create_connection(ssmdbkey, region, database):
    try:
        splunk.log_message(
            {"Message": f"ssmdbkey : [{ssmdbkey}] ==> region: [{region}] ==> database: [{database}]", "Status": "INFO"},
            get_run_id())
        ssm = boto3.client('ssm', region_name=region)
        param_name = ssm.get_parameter(Name=ssmdbkey, WithDecryption=True)['Parameter']['Value']
        creds = get_secret_values(param_name)
        return create_engine(
            f"postgresql://{creds['username']}:{creds['password']}@{creds['host']}:{creds['port']}/{database}",
            future=True,
            echo=True)
    except Exception as e:
        raise Exception(f"Error creating database connection: {str(e)}")
#########################################################################
def get_sqlalchemy_type(type_string):
    type_mapping = {
        "String": String, "Text": Text, "Integer": Integer, "Float": Float, "Numeric": Numeric,
        "Boolean": Boolean, "Date": Date, "DateTime": DateTime, "Time": Time, "JSON": JSON, "Enum": Enum
    }
    return type_mapping.get(type_string) or ValueError(f"Unsupported column type: {type_string}")
#########################################################################
def get_nested_value(data, path):
    for key in path:
        data = data[key]
    return data
#########################################################################
def create_table(engine, table_columns, table_name, client_id, table_creation_type, schema):
    try:
        metadata = MetaData(schema=schema)  # Explicitly set the schema
        #####metadata = MetaData()
        columns = []
        client_attributes = table_columns.get(client_id, {}).get('attributes', [])

        for attr in client_attributes:
            columns.append(
                Column(attr['name'], get_sqlalchemy_type(attr['type']), primary_key=attr.get('primary_key', False)))

        stag_table = Table(table_name, metadata, *columns)
        metadata.create_all(engine, checkfirst=True)

        if table_creation_type == 'Replace':
            with engine.connect() as conn:
                conn.execute(delete(stag_table))  # Deletes all records in the table
                conn.commit()
        return stag_table
    except SQLAlchemyError as e:
        raise Exception(f"Table creation failed: {str(e)}")
#################################################################################
def load_stage():
    splunk.log_message({"Message": "Starting load_stage...", "Status": "INFO"}, get_run_id())
    database = os.getenv("database")
    schema = os.getenv("schema")

    splunk.log_message({"Message": f"Database: {database}", "Status": "INFO"}, get_run_id())
    splunk.log_message({"Message": f"Schema: {schema}", "Status": "INFO"}, get_run_id())
    if not database or not schema:
        splunk.log_message({"Message": "Both 'database' and 'schema' environment variables must be set and non-empty.",
                            "Status": "ERROR"}, get_run_id())
        raise ValueError("Both 'database' and 'schema' environment variables must be set and non-empty.")

    splunk.log_message({"Message": "Creating database connection...", "Status": "INFO"}, get_run_id())
    engine = create_connection(os.getenv("dbkey"), os.getenv("region"), database)
    splunk.log_message({"Message": "Database connection created successfully.", "Status": "SUCCESS"}, get_run_id())
    table_name = os.getenv("table_name")

    if not table_name:
        splunk.log_message(
            {"Message": "The 'table_name' environment variable must be set and non-empty.", "Status": "ERROR"},
            get_run_id())
        raise ValueError("The 'table_name' environment variable must be set and non-empty.")
    try:

        response = s3_client.get_object(Bucket=os.getenv("bucket_name"), Key=os.getenv("key"))
        data = json.loads(response['Body'].read().decode('utf-8'))
        ###  begin 4031  ####
        #load_data = get_nested_value(data, ast.literal_eval(os.getenv("json_loader_path")))
        client_headers = get_nested_value(data, ast.literal_eval(os.getenv("json_loader_path")))
        load_data = [table
                      for ch in client_headers
                      for ets in ch['ETS0420']
                      for table in ets['04200-CC-TABLE']]

        #print(f"this is data \n {load_data}")
        ########  end 4031 #####################
        if os.getenv("multiple_data") == "true":
            load_data = [item.get(os.getenv("identifier"), []) for item in load_data]
            load_data = [i for sublist in load_data for i in sublist]

            # Determine table name
        if os.getenv("header") != "":
            table_name = ".".join(os.getenv("file_name").split('.')[:2])

        table = create_table(engine, json.loads(os.getenv("columns")), table_name, os.getenv("client_id"),
                             os.getenv("table_creation_type"), schema)

        time_stamp_table = create_table(engine, json.loads(os.getenv("columns")), os.getenv("file_name"),
                                        os.getenv("client_id"), os.getenv("table_creation_type"), schema)

        with engine.connect() as conn:
            splunk.log_message({"Message": "Acquired connection to the database.", "Status": "INFO"}, get_run_id())
            txn = conn.begin()  # Start a transaction

            try:
                # Delete existing records before inserting new ones**
                splunk.log_message({"Message": f"Deleting existing records from table: {table.name}", "Status": "INFO"},
                                   get_run_id())
                conn.execute(delete(table))

                # **Insert new records**
                splunk.log_message({"Message": f"Inserting data into table: {table.name}", "Status": "INFO"},
                                   get_run_id())
                if load_data:
                    splunk.log_message(
                        {"Message": f"Inserting {len(load_data)} records into table: {table.name}", "Status": "INFO"},
                        get_run_id())

                    load_data_fixed = [cast_row_types(row, table) for row in load_data]
                    # conn.execute(insert(table).values(load_data))
                    conn.execute(insert(table), load_data_fixed)

                    # conn.execute(insert(time_stamp_table).values(load_data))
                    load_time_data_fixed = [cast_row_types(row, time_stamp_table) for row in load_data]
                    conn.execute(insert(time_stamp_table), load_time_data_fixed)
                else:
                    splunk.log_message({"Message": "No data to insert into the table.", "Status": "INFO"}, get_run_id())
                splunk.log_message({"Message": "Data inserted successfully.", "Status": "SUCCESS"}, get_run_id())
                txn.commit()  # Commit if everything is successful
                splunk.log_message({"Message": "load_stage completed successfully.", "Status": "SUCCESS"}, get_run_id())

            except SQLAlchemyError as e:
                txn.rollback()  # Rollback on failure
                splunk.log_message({"Message": f"Error during load_stage-1: {str(e)}", "Status": "ERROR"}, get_run_id())
                raise Exception(f"Error during load_stage: {str(e)}")
        splunk.log_message({"Message": f"load_stage completed successfully.", "Status": "SUCCESS"}, get_run_id())

        replica = json.loads(os.getenv("replica"))

        if replica.get("replica_needed") == 'Y':

            replica_table = create_table(engine, os.getenv("columns"), replica["replica_table_name"],
                                         os.getenv("client_id"), replica["replica_table_creation_type"], schema)
            with engine.connect() as conn:
                if load_data:
                    conn.execute(insert(replica_table).values(load_data))
                    conn.commit()
                    splunk.log_message(
                        {"Message": f"Data inserted into replica table: {replica_table.name}", "Status": "SUCCESS"},
                        get_run_id())
        splunk.log_message({"Message": "load_stage completed successfully.", "Status": "SUCCESS"}, get_run_id())
    except Exception as e:
        splunk.log_message({"Message": f"Error during load_stage: {str(e)}", "Status": "ERROR"}, get_run_id())
        raise Exception(f"Error during load_stage: {str(e)}")
######################################################################
def cast_row_types(row, table):
    # Make a copy so we don't modify the input
    row_converted = {}
    for col in table.columns:
        val = row.get(col.name)
        # Skip conversion if value is None
        if val is None:
            row_converted[col.name] = None
            continue
        # Type conversion, adapt as needed for your schema
        if isinstance(col.type, Numeric):
            row_converted[col.name] = Decimal(val)
        elif isinstance(col.type, Float):
            row_converted[col.name] = float(val)
        elif isinstance(col.type, Integer):
            row_converted[col.name] = int(val)
        else:
            row_converted[col.name] = val  # Leave as is (e.g., strings)
    return row_converted
##################################################################
def set_job_params_as_env_vars():
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            value = sys.argv[i + 1]
            os.environ[key] = value
            message = f'Set environment variable {key} to {value}'
            splunk.log_message({'Status': 'info', 'Message': message}, get_run_id())
######################################################################
def main():
    try:
        set_job_params_as_env_vars()
        load_stage()
    except Exception as e:
        splunk.log_message({"Message": f"Error: {str(e)}", "Status": "ERROR"}, get_run_id())
        raise Exception(f"Error: {str(e)}")
#######################################################################
if __name__ == '__main__':
    main()
