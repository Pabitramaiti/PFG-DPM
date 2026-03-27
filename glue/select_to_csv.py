import psycopg2
import csv
import boto3
import os
import json
import sys
import splunk
import re



def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:dpmdev-di-glue-load_stage:" + account_id



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

def run_query_and_upload_batchwise(query,bucker_name,key,conn,batch_size=10000):
    """
    Run SQL query in batches (LIMIT/OFFSET), write to CSV, upload to S3.
    """
    output_file = os.path.basename(key)
    try:
        # 1. Connect to DB
        cursor = conn.cursor()

        # 2. Open CSV file
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = None
            offset = 0

            while True:
                batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
                cursor.execute(batch_query)
                rows = cursor.fetchall()

                if not rows:
                    break  # no more rows

                # Write header only once
                if writer is None:
                    colnames = [desc[0] for desc in cursor.description]
                    writer = csv.writer(csvfile)
                    writer.writerow(colnames)

                # Write rows
                writer.writerows(rows)

                print(f"Processed batch offset {offset} ({len(rows)} rows)")
                offset += batch_size

        cursor.close()
        conn.close()

        # 3. Upload to S3
        s3 = boto3.client("s3")
        s3.upload_file(output_file, bucker_name, key)
        print(f"Uploaded {output_file} to s3://{bucker_name}/{key}")

    except Exception as e:
        print(f"Error: {e}")
        raise e
    finally:
        if os.path.exists(output_file):
            os.remove(output_file)  # cleanup local file


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

def main():
    try:
        print("Setting params")
        set_job_params_as_env_vars()
        print("params execute successfully")
    except Exception as e:
        message = f"failed to retrieve input parameters."

    try:
        query_strings = os.getenv('sql_query_config')
        table_config_path = os.getenv("table_config_path","")
        bucket_name = os.getenv("bucket_name")
        if not all([query_strings]):
            raise ValueError("Missing required environment variables.")
        query_configs = json.loads(query_strings)
        table_config={}
        batch_size = int(os.getenv("batch_size"))
        if table_config_path is not None and table_config_path!= "":
            table_config = get_table_config_from_s3(bucket_name, table_config_path)
        #Validate the keys from table_config
        for key, value in table_config.items():
            validate_table_name(value)

        for config in query_configs:
            conn = create_connection(os.getenv("dbkey"), os.getenv("region"))
            query = config.get("query")
            query = query.format(**table_config)
            key = config.get("csv_path")
            run_query_and_upload_batchwise(query,bucket_name,key,conn,batch_size)


    except Exception as e:
        raise e

VALID_IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

def validate_table_name(table_name):
    if not VALID_IDENTIFIER.match(table_name):
        raise ValueError(f"Invalid table name: {table_name}")

def get_table_config_from_s3(bucket: str, key: str):
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    data = json.loads(content)
    return data



if __name__ == "__main__":
    main()
