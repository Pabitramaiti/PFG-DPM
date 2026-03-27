import json
import boto3
from sqlalchemy import Table, Column, create_engine, text, Text, String, MetaData, Numeric, Integer, Float, BigInteger, \
    JSON, insert, select, func, inspect, bindparam, ARRAY, and_, Boolean, Date, DateTime, Time, Enum, delete
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
import sys
import os
import splunk

s3_client = boto3.client('s3')
ssm = boto3.client('ssm', region_name=region)
client = boto3.client("secretsmanager")


def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:dpmdev-di-glue-load_stage:" + account_id


def get_secret_values(secret_name):
    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret_value = response['SecretString']
        json_secret_value = json.loads(secret_value)
        splunk.log_message({'Message': f"Secret values retrieved successfully", "Status": "SUCCESS"},
                           get_run_id())

        return json_secret_value
    except Exception as e:
        splunk.log_message({'Message': f"Error retrieving secret values", "Status": "ERROR"},
                           get_run_id())
        raise e


def create_connection(ssmdbkey, region):
    try:

        secret_name = ssm.get_parameter(Name=ssmdbkey, WithDecryption=True)
        secret_name = secret_name['Parameter']['Value']

        host = get_secret_values(secret_name)

        conn_engine = create_engine(
            f"postgresql://{host['username']}:{host['password']}@{host['host']}:{host['port']}/{host['dbname']}",
            future=True, echo=True)
        splunk.log_message({"Message": f"Database connection created successfully.", "Status": "SUCCESS"}, get_run_id())
        print("Connection created successfully")
        return conn_engine

    except Exception as e:
        splunk.log_message({"Message": f"Error creating database connection: {e}.", "Status": "ERROR"}, get_run_id())
        raise e


# Function to dynamically fetch the SQLAlchemy type based on the type string
def get_sqlalchemy_type(type_string):
    # Map type strings to SQLAlchemy types
    type_mapping = {
        "String": String,  # Variable-length string
        "Text": Text,  # Variable-length string (larger than String)
        "Integer": Integer,  # Integer
        "Float": Float,  # Floating-point number
        "Numeric": Numeric,  # Fixed-point number
        "Boolean": Boolean,  # Boolean (True/False)
        "Date": Date,  # Date
        "DateTime": DateTime,  # Date and time
        "Time": Time,  # Time
        "JSON": JSON,  # JSON data
        "Enum": Enum  # Enumerated type
        # Add more types as needed
    }

    if type_string in type_mapping:
        return type_mapping[type_string]
    else:
        raise ValueError(f"Unsupported column type: {type_string}")


def create_table(engine, table_columns, table_name, client_id, table_creation_type):
    """
    This function will create the columns dynamically for tables required for the pgAdmin 4 database
    """
    try:
        metadata = MetaData(schema=os.getenv("schema"))

        columns = []

        # Parse the JSON string containing the column configuration
        columns_from_config = json.loads(table_columns)
        client_attributes = columns_from_config.get(client_id, {}).get('attributes', [])

        # Iterate over the column configuration to dynamically create columns
        for attribute in client_attributes:
            column_name = attribute['name']

            column_type_str = attribute['type']
            column_type = get_sqlalchemy_type(column_type_str)

            primary_key = attribute.get('primary_key', False)  # Default to False if 'primary_key' key not present
            columns.append(Column(column_name, column_type, primary_key=primary_key))

        # Create the table using the list of column definitions
        stag_table = Table(table_name, metadata, *columns)

        if stag_table.exists(engine):
            if table_creation_type == 'Replace':
                delete_stmt = delete(stag_table).where(stag_table.c.CID == client_id)
                # Execute the delete statement
                with engine.connect() as connection:
                    connection.execute(delete_stmt)
                    connection.commit()
            elif table_creation_type == 'New':
                stag_table.drop(engine)
                metadata.create_all(engine, checkfirst=True)
        else:
            # Create staging table in the database (if doesn't exist)
            metadata.create_all(engine, checkfirst=True)

        return stag_table

    except SQLAlchemyError as e:
        # Handle generic SQLAlchemy errors
        message = f"Table creation failed due to SQLAlchemy error. {str(e)}"
        raise Exception(message)

    except Exception as e:
        message = f"Table creation failed. {str(e)}"
        raise Exception(message)


def load_stage():
    """
    This loads the elements needed to create the tables required for the pgAdmin 4 database.  It will call the function
    that actually creates the tables.
    """
    engine = create_connection(os.getenv("dbkey"), os.getenv("region"))
    try:
        print("reading object_key")
        response = s3_client.get_object(Bucket=os.getenv("bucket_name"), Key=os.getenv("object_key"))
        json_content = response['Body'].read().decode('utf-8')
        splunk.log_message({"Message": f"Successfully read S3 object.", "Status": "SUCCESS"}, get_run_id())
        load_data = json.loads(json_content)['json_data']
        splunk.log_message({"Message": f"Successfully read and parsed JSON data.", "Status": "SUCCESS"}, get_run_id())
        table = create_table(engine, os.getenv("columns"), os.getenv("table_name"), os.getenv("client_id"),
                             os.getenv("table_creation_type"))
        try:
            with engine.connect() as connection:
                if load_data:
                    connection.execute(insert(table).values(load_data))
                    connection.commit()

            splunk.log_message({"Message": f"load_stage completed successfully.", "Status": "SUCCESS"}, get_run_id())
        except SQLAlchemyError as e:
            # Handle generic SQLAlchemy errors
            message = f"Data insertion into table failed due to SQLAlchemy error. {str(e)}"
            splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
            raise Exception(message)

        except Exception as e:
            splunk.log_message({"Message": f"Data insertion into table failed due to: {e}", "Status": "ERROR"},
                               get_run_id())
            raise e

        """
        The following block of code was added to create a table with just the base name without a date time stamp.
        It uses the items found in the "base_file_name" argument to compare to the original file name. If the
        base file name is a subset of the original filename, then the base file name will be used.
        """
        replica = json.loads(os.getenv("replica"))

        if replica["replica_needed"] == 'Y':
            replica_table = create_table(engine, os.getenv("columns"), replica["replica_table_name"],
                                         os.getenv("client_id"), replica["replica_table_creation_type"])

            try:
                with engine.connect() as connection:
                    # connection.execute(insert(table).values(load_data))
                    sql_query = f"INSERT INTO {replica_table} SELECT * FROM {table}"
                    connection.execute(text(sql_query))
                    connection.commit()

                splunk.log_message({"Message": f"load_stage completed successfully.", "Status": "SUCCESS"},
                                   get_run_id())
            except SQLAlchemyError as e:
                # Handle generic SQLAlchemy errors
                message = f"Data insertion into table failed due to SQLAlchemy error. {str(e)}"
                splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
                raise Exception(message)

            except Exception as e:
                splunk.log_message({"Message": f"Data insertion into table failed due to: {e}", "Status": "ERROR"},
                                   get_run_id())
                raise e

    except Exception as e:
        splunk.log_message({"Message": f"Error during load_stage: {e}", "Status": "ERROR"}, get_run_id())
        raise e


def set_job_params_as_env_vars():
    # print("sys.argv", sys.argv)
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    i = 1
    while i < len(sys.argv):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            if i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith('--'):
                value = sys.argv[i + 1]
                print(f"key:{key}")
                print(f"value:{value}")
                os.environ[key] = value
                i += 2  # Move to the next key-value pair
            else:
                # Handle cases where the key doesn't have a value
                # print(f"Key '{key}' does not have a value.")
                i += 1  # Move to the next key
        else:
            i += 1  # Move to the next argument


def main():
    try:
        set_job_params_as_env_vars()
        print("params execute successfully")

    except Exception as e:
        message = f"failed to retrieve input parameters. {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
        raise Exception(message)

    try:
        load_stage()

    except Exception as e:
        splunk.log_message({"Message": f"Error during load_stage: {e}", "Status": "ERROR"}, {'status_code': 500})
        raise e


if __name__ == '__main__':
    main()
