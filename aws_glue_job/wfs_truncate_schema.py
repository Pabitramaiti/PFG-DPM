import json
import psycopg2
import splunk
import boto3
import os
from psycopg2 import pool
import sys
import re
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.pool import ThreadedConnectionPool

#ssm = boto3.client('ssm', region_name='us-east-1')
client_db = boto3.client("secretsmanager")
schema = "public"

SPLUNK_EXECUTOR = ThreadPoolExecutor(max_workers=20)

# PostgreSQL connection details (set via AWS Lambda environment variables)
def get_db_credentials():
    """
    Fetch PostgreSQL credentials from AWS Secrets Manager.
    """
    try:
        #ssmdbkey = ssmDBKey
        # param_name = ssm.get_parameter(Name=ssmdbkey, WithDecryption=True)
        # param_value = param_name['Parameter']['Value']
        response = client_db.get_secret_value(SecretId='dpmtest/dev/institutional')
        json_secret_value = json.loads(response['SecretString'])
        return json_secret_value
    except Exception as e:
        raise Exception(f"Error getting db credentials: {str(e)}") from e

def initialize_db_pool(db_credentials):
    """
    Initialize the global database connection pool.
    """
    if not db_credentials:
        raise ValueError("Database credentials could not be loaded.")

    db_pool = ThreadedConnectionPool(
        5, 20,
        host=db_credentials["host"],
        port=db_credentials["port"],
        dbname=db_credentials["dbname"],
        user=db_credentials["username"],
        password=db_credentials["password"],
        options=f"-c search_path={schema}"
    )
    return db_pool

def get_db_connection(db_pool):
    return db_pool.getconn()

def release_db_connection(connection, db_pool):
    if connection is not None:
        db_pool.putconn(connection)

# Execute Function (example: truncate_wfs_public_schema_tables with no params)
def call_truncateFunction(db_pool):
    try:
        cursor = get_db_connection(db_pool).cursor()
        query = f"""SELECT public.truncate_wfs_public_schema_tables()"""

        cursor.execute(query)
        log_event("CALLED_FUNCTION", f"Executed Function successfully!")
        cursor.close()

    except Exception as e:
        log_event("FAILED", f"glue_truncate_wfs_public_schema::Fatal Error::{traceback.format_exc()}, Exception : {str(e)}")
        raise e

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:store_auxrecords_postgres:" + account_id

# --- Splunk logger ---
def log_event(status: str, message: str):
    splunk.log_message({"status": status, "message": message}, get_run_id())

def main():
    connection = None
    cursor = None
    db_pool = None

    try:
        #set_job_params_as_env_vars()
        log_event("STARTED", "glue_truncate_wfs_public_schema::Started execution")

        #transactionId = os.getenv("transactionId")
        ssmDBKey = os.getenv("ssmDBKey")

        # Fetch DB credentials only once per glue execution
        db_credentials = get_db_credentials(ssmDBKey)
        db_pool = initialize_db_pool(db_credentials)

        # Establish connection
        connection = get_db_connection(db_pool)
        cursor = connection.cursor()

        query = f"""
        SELECT * FROM public.bios_wfs_out_recon where accountnumber= '1AA00006'"""

        cursor.execute(query)
        records = cursor.fetchall()
        log_event("INFO", f"Fetched {len(records)} records from DB")
        
        if not records:
            log_event("ENDED", "There is no data found!")
            return

        call_truncateFunction(db_pool)
        #query = f"""call public.truncate_wfs_public_schema_tables2()"""
        #cursor.execute(query)
        #log_event("INFO", f"Executed Stored Procedure successfully!")

    except Exception as e:
        log_event("FAILED", f"glue_truncate_wfs_public_schema::Fatal Error::{traceback.format_exc()}, Exception : {str(e)}")
        raise e

    finally:
        SPLUNK_EXECUTOR.shutdown(wait=True)

if __name__ == '__main__':
    main()