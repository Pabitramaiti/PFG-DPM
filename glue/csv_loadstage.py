import csv
import json
import boto3
import sys
import traceback
import io
from sqlalchemy import create_engine, MetaData, Table, Column, String, text, TIMESTAMP
from awsglue.utils import getResolvedOptions
import splunk

# Initialize AWS clients
s3_client = boto3.client('s3')
client = boto3.client('secretsmanager')

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return f"arn:dpm:glue:dpmdev-di-glue-delimited_loadstage:{account_id}"


def get_secret_values(secret_name):
    try:
        print(f"get_secret_values called with secret_name: {secret_name}")
        splunk.log_message({'Message': f"Retrieving secret values for {secret_name}", "Status": "INFO"}, get_run_id())
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        print(f"Error retrieving secret values: {e}")
        splunk.log_message({'Message': f"Error retrieving secret values: {e}", "Status": "ERROR"}, get_run_id())
        raise


def create_connection(ssmdbkey, region, database):
    try:
        ssm = boto3.client('ssm', region_name=region)
        secret_name = ssm.get_parameter(Name=ssmdbkey, WithDecryption=True)['Parameter']['Value']
        creds = get_secret_values(secret_name)
        engine = create_engine(
            f"postgresql://{creds['username']}:{creds['password']}@{creds['host']}:{creds['port']}/{database}",
            future=True, 
            pool_pre_ping=True
        )
        return engine
    except Exception as e:
        print(f"Error creating database connection: {e}")
        splunk.log_message({'Message': f"DB connection error: {e}", "Status": "ERROR"}, get_run_id())
        raise


def create_table(engine, table_name, headers, schema):
    """
    Drop the table if it exists, then create a new one from headers.
    """
    metadata = MetaData(schema=schema)
    #columns = [Column(header, String) for header in headers]
    columns = []
    for header in headers:
        # simple rule: if header name contains 'time' or 'date', use timestamptz
        if 'time' in header.lower() or 'date' in header.lower():
            col_type = TIMESTAMP(timezone=True)
        else:
            col_type = String
        columns.append(Column(header, col_type))

    print(f'Creating table {table_name} in schema {schema} with following columns:')
    for col in columns:
        print(f"  {col.name} : {col.type}")

    table = Table(table_name, metadata, *columns)


    with engine.connect() as conn:
        # Drop existing table if it exists
        if engine.dialect.has_table(conn, table_name, schema=schema):
            print(f"Table {table_name} exists — dropping and recreating it.")
            splunk.log_message({"Message": f"Table {table_name} exists — dropping and recreating it.", "Status": "INFO"}, get_run_id())
            table.drop(engine, checkfirst=True)
        else:
            print(f"Table {table_name} does not exist — creating new.")
            splunk.log_message({"Message": f"Table {table_name} does not exist — creating new.", "Status": "INFO"}, get_run_id())

    metadata.create_all(engine)
    print(f"Table {table_name} created successfully.")
    splunk.log_message({"Message": f"Table {table_name} created successfully.","Status": "SUCCESS"}, get_run_id())
    return table

def prepare_table(engine, table_name, schema):
    """
    Verifies that the target table exists and truncates it.
    Raises an error if the table doesn't exist.
    """
    metadata = MetaData(schema=schema)

    with engine.connect() as conn:
        if engine.dialect.has_table(conn, table_name, schema=schema):
            # Reflect the existing table structure
            table = Table(table_name, metadata, autoload_with=engine)

            msg = f'Table {schema}.{table_name} exists — current structure:'
            print(msg)
            splunk.log_message({"Message": msg, "Status": "INFO"}, get_run_id())

            # Log all columns and types
            for col in table.columns:
                col_msg = f"  {col.name} : {col.type}"
                print(col_msg)
                splunk.log_message({"Message": col_msg, "Status": "INFO"}, get_run_id())

            # Truncate existing data
            msg = f'Truncating table {schema}.{table_name} before data load.'
            print(msg)
            splunk.log_message({"Message": msg, "Status": "INFO"}, get_run_id())

            conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table_name}";'))
            conn.commit()

            msg = f'Table {schema}.{table_name} truncated successfully.'
            print(msg)
            splunk.log_message({"Message": msg, "Status": "SUCCESS"}, get_run_id())
        else:
            msg = f'Table {schema}.{table_name} does not exist — cannot continue.'
            print(msg)
            splunk.log_message({"Message": msg, "Status": "ERROR"}, get_run_id())
            raise ValueError(msg)

    return table

def main():

    try:
        print("Starting optimized csv load_stage...")
        splunk.log_message({"Message": "Starting optimized csv load_stage...", "Status": "INFO"}, get_run_id())
        
        event = getResolvedOptions(sys.argv, ['bucket_name', 'object_key', 'dbkey', 'region', 'file_name', 'key_location'])

        # Configuration
        bucket_name = event.get("bucket_name")
        object_key = event.get("object_key")
        file_name = event.get("file_name")
        region = event.get("region")
        dbkey = event.get("dbkey")

        print(f"Parameters received: bucket_name={bucket_name}, object_key={object_key}, file_name={file_name}, region={region}, dbkey={dbkey}")

        key_location = json.loads(event.get("key_location"))
        for key in sorted(key_location.keys(), key=len, reverse=True):
            if key in file_name:
                conf = key_location[key]
                delimiter = conf['delimiter']
                database = conf['database']
                schema = conf['schema']
                table_name = conf['table_name']
                mode = conf.get('mode', 'recreate').lower()
                break

        print(f"Final configuration values: delimiter={delimiter}::database={database}::schema={schema}::table_name={table_name}, mode={mode}")
        splunk.log_message({"Message": f"Final configuration values: delimiter={delimiter}::database={database}::schema={schema}::table_name={table_name}::mode={mode}", "Status": "INFO"}, get_run_id())

        if not table_name:
            print("The 'table_name' within key_location environment variable must be set and non-empty.")
            splunk.log_message({"Message": "The 'table_name' within key_location environment variable must be set and non-empty.", "Status": "ERROR"}, get_run_id())
            raise ValueError("The 'table_name' within key_location environment variable must be set and non-empty.")

        if not delimiter:
            print("The 'delimiter' within key_location environment variable must be set and non-empty.")
            splunk.log_message({"Message": "The 'delimiter' within key_location environment variable must be set and non-empty.", "Status": "ERROR"}, get_run_id())
            raise ValueError("The 'delimiter' within key_location environment variable must be set and non-empty.")

        if not database:
            print("The 'database' within key_location environment variable must be set and non-empty.")
            splunk.log_message({"Message": "The 'database' within key_location environment variable must be set and non-empty.", "Status": "ERROR"}, get_run_id())
            raise ValueError("The 'database' within key_location environment variable must be set and non-empty.")

        if not schema:
            print("The 'schema' within key_location environment variable must be set and non-empty.")
            splunk.log_message({"Message": "The 'schema' within key_location environment variable must be set and non-empty.","Status": "ERROR"}, get_run_id())
            raise ValueError("The 'schema' within key_location environment variable must be set and non-empty.")

        # Create DB connection
        engine = create_connection(dbkey, region, database)
        print("Database connection established successfully.")

        # First stream just to read headers
        resp_headers = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        stream_header = io.TextIOWrapper(resp_headers['Body'], encoding='utf-8')
        headers = next(csv.reader(stream_header, delimiter=delimiter))
        stream_header.detach()
        print(f"Headers extracted: {headers}")
        splunk.log_message({"Message": f"Headers extracted: {headers}", "Status": "INFO"}, get_run_id())

        if mode == 'truncate':
            table = prepare_table(engine, table_name, schema)
        else:
            table = create_table(engine, table_name, headers, schema)

        # Second stream to count rows
        resp_count = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        source_count = sum(1 for _ in io.TextIOWrapper(resp_count['Body'], encoding='utf-8')) - 1
        print(f"Source file has ~{source_count} data rows.")
        splunk.log_message({"Message": f"Source file has ~{source_count} data rows.", "Status": "INFO"}, get_run_id())

        conn = engine.raw_connection()
        try:
            cur = conn.cursor()
            resp_copy = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            stream_copy = io.TextIOWrapper(resp_copy['Body'], encoding='utf-8')
            print("Beginning COPY FROM STDIN load...")
            splunk.log_message({"Message": "Beginning COPY FROM STDIN load...", "Status": "INFO"}, get_run_id())
            cur.copy_expert(
                f"COPY \"{schema}\".\"{table_name}\" FROM STDIN WITH CSV HEADER DELIMITER '{delimiter}' QUOTE '\"'",
                stream_copy
            )
            conn.commit()
            print(f"COPY INTO {table_name} completed successfully.")
            splunk.log_message({"Message": f"COPY INTO {table_name} completed successfully.", "Status": "SUCCESS"}, get_run_id())
        except Exception as e:
            conn.rollback()
            error_msg = f"Error during COPY operation: {e}, Traceback:{traceback.format_exc()}"
            print(error_msg)
            splunk.log_message({"Message": error_msg, "Status": "ERROR"}, get_run_id())
            raise
        finally:
            conn.close()

        with engine.connect() as conn_verify:
            query = text(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
            result = conn_verify.execute(query)
            loaded_count = result.scalar()
            msg = f"Data load verification: {loaded_count} records present in {table_name}."
            print(msg)
            splunk.log_message({"Message": msg, "Status": "INFO"}, get_run_id())

        if loaded_count != source_count:
            diff = source_count - loaded_count
            warn_msg = f"WARNING: Mismatch — expected {source_count}, loaded {loaded_count} (difference {diff})."
            print(warn_msg)
            splunk.log_message({"Message": warn_msg, "Status": "WARNING"}, get_run_id())
        else:
            msg = f"Validation passed — all {loaded_count} records loaded successfully."
            print(msg)
            splunk.log_message({"Message": msg, "Status": "SUCCESS"}, get_run_id())


        print("All records loaded successfully using COPY.")
        splunk.log_message({"Message": "All records loaded successfully using COPY.", "Status": "SUCCESS"}, get_run_id())

    except Exception as e:
        error_msg = f"Error in load_stage::Exception={e}::Traceback:{traceback.format_exc()}"
        print(error_msg)
        splunk.log_message({"Message": error_msg, "Status": "ERROR"}, get_run_id())
        raise

if __name__ == '__main__':
    main()