import os
import re
import sys
import psycopg2
from psycopg2.extras import execute_values
import boto3
import splunk
import json
from datetime import datetime


def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:dpmdev-di-glue-load_stage:" + account_id

VALID_IDENTIFIER = re.compile(r'^[A-Za-z_0-9][A-Za-z0-9_]*$')

def validate_table_name(table_name):
    if not VALID_IDENTIFIER.match(table_name):
        raise ValueError(f"Invalid table name: {table_name}")

def table_exists(conn, table_name, schema='public'):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            );
        """, (schema, table_name))
        return cur.fetchone()[0]

def get_table_config_from_s3(bucket: str, key: str):
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    data = json.loads(content)
    return data

def create_connection(ssmdbkey, region):
    try:
        ssm = boto3.client('ssm', region_name=region)

        secret_name = ssm.get_parameter(Name=ssmdbkey, WithDecryption=True)
        secret_name = secret_name['Parameter']['Value']

        host = get_secret_values(secret_name)

        conn_str = form_db_url(host)

        # Connect to DB
        conn_engine = psycopg2.connect(conn_str)
        return conn_engine

    except Exception as e:
        splunk.log_message({"Message": f"Error creating database connection: {e}.", "Status": "ERROR"}, get_run_id())
        raise e


def get_secret_values(secret_name):
    try:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)
        secret_value = response['SecretString']
        json_secret_value = json.loads(secret_value)
        splunk.log_message({'Message': f"Secret values retrieved successfully for {secret_name}.", "Status": "SUCCESS"},
                           get_run_id())

        return json_secret_value
    except Exception as e:
        splunk.log_message({'Message': f"Error retrieving secret values for {secret_name}: {e}", "Status": "ERROR"},
                           get_run_id())
        raise e

def form_db_url(host):
    """Form DB connection URL from environment variables."""
    db_user = host['username']
    db_pass = host['password']
    db_host = host['host']
    db_port = host['port']
    db_name = host['dbname']

    if not all([db_user, db_pass, db_name]):
        raise ValueError("Missing one of DB_USER, DB_PASS, or DB_NAME environment variables")

    return f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"


def is_select_query(query):
    return bool(re.match(r'^\s*SELECT\s+', query, re.IGNORECASE))

def set_job_params_as_env_vars():
    print("sys.argv", sys.argv)
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

def create_dest_table_from_query(conn, table_name, query, schema='public'):
    """Create the destination table if it does not exist, based on query result metadata."""
    with conn.cursor() as cur:
        # Run the query with LIMIT 0 to get metadata only
        #Creating table from the query
        ddl = f"CREATE TABLE \"{schema}\".\"{table_name}\" AS {query} limit 0"
        print(f"Creating table if not exists:\n{ddl}")
        cur.execute(ddl)
        conn.commit()
        print(f"Destination table is created:\n{ddl}")

table_config= None

# Validating string if it has any malicious characters for SQL identifiers
def validate_identifier(name):
    if not VALID_IDENTIFIER.match(name):
        raise ValueError(f"Invalid identifier: {name}")

def main():
    global cursor, insert_cursor, conn,table_config
    try:
        print("Setting params")
        set_job_params_as_env_vars()
        print("params execute successfully")

    except Exception as e:
        message = f"failed to retrieve input parameters. {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
        raise Exception(message)
    try:
        # Get env variables
        query = os.getenv('sql_query')
        table_config_path = os.getenv("table_config_path","")
        if not all([query, table_config_path]):
            raise ValueError("Missing required environment variables.")
        if not is_select_query(query):
            raise ValueError("Only SELECT queries are allowed.")

        bucket_name=os.getenv("bucket_name")
        table_config=get_table_config_from_s3(bucket_name, table_config_path)

        # Get schema from environment variable, default to 'public'
        schema = os.getenv("schema", "public")
        print("schema", schema)
        validate_identifier(schema)

        #Validate the keys from table_config (exclude 'schema')
        for key, value in table_config.items():
            if key != 'schema':  # Skip schema from table name validation
                validate_table_name(value)

        conn = create_connection(os.getenv("dbkey"), os.getenv("region"))
        print("creating table")

        # Add schema to table_config for query formatting
        table_config['schema'] = schema

        #Replacing the table name if need to be formatted
        query = query.format(**table_config)
        move_table = os.getenv("move_table_if_exists", "false")
        delete_source_table = os.getenv("delete_source_table", "false")
        cursor = conn.cursor()
        dest_table_name = table_config.get('dest_table_name')
        validate_identifier(dest_table_name)

        if table_exists(conn, dest_table_name, schema) and move_table.lower()=="true":
            #move the table if already a table exists with the same name
            print("Table already exist. So creating a temp table and moving the current data")
            now = datetime.now()
            formatted_date = now.strftime("%d%m%y%H")
            temp_query = f"SELECT * from \"{schema}\".\"{dest_table_name}\""
            temp_table = f"{dest_table_name}_{formatted_date}"
            print("Creating temp table")
            drop_table(conn, temp_table, schema)
            create_dest_table_from_query(conn, temp_table, temp_query, schema)
            insert_query = f"INSERT INTO \"{schema}\".\"{temp_table}\" " + temp_query
            cursor.execute(insert_query)
            conn.commit()
            print(f"Moved data from {dest_table_name} to {temp_table}")

        drop_table(conn, dest_table_name, schema)
        create_dest_table_from_query(conn, dest_table_name, query, schema)
        print(f"Inserting data from the query")
        insert_query = f"INSERT INTO \"{schema}\".\"{dest_table_name}\" " + query
        cursor.execute(insert_query)
        conn.commit()
        print(f"Rows inserted successfully")

        # Delete source temp table if configured
        if delete_source_table.lower() == "true":
            source_table = table_config.get('table_name')
            if source_table:
                validate_identifier(source_table)
                print(f"Deleting source table {source_table}")
                drop_table(conn, source_table, schema)

    except Exception as e:
        print(f"Error: {e}")
        raise e

    finally:
        try:
            cursor.close()
            insert_cursor.close()
            conn.close()
        except:
            pass

def drop_table(conn, table_name, schema="public"):
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table_name}" CASCADE;')
    conn.commit()
    print(f"Dropped table {schema}.{table_name}")

if __name__ == '__main__':
    main()
