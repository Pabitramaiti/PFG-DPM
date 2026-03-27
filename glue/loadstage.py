import json
import boto3
import os

from sqlalchemy import *
from awsglue.utils import getResolvedOptions
import sys
import splunk
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError
import traceback
import re

s3_client = boto3.client('s3')
ssm_client = boto3.client("secretsmanager")


def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue_loadstage:" + account_id


def is_valid_email(email: str) -> bool:
    """Return True if email looks like a valid address."""
    if not email:
        return False
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(email_regex, email) is not None


# --- Splunk logger ---
def log_event(status: str, message: str):
    job_name = "glue_loadstage"
    if os.getenv("statemachine_name"):
        parts = os.getenv("statemachine_name").split("-")
        if len(parts) >= 3:
            prefix = "_".join(parts[0:3])
            job_name = f"{prefix}_{job_name}"

    event_data = {
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "file_name": os.getenv("file_name", ""),
        "clientName": os.getenv("clientName", ""),
        "clientID": os.getenv("clientID", ""),
        "applicationName": os.getenv("applicationName", ""),
        "step_function": "conversion",
        "component": "glue",
        "job_name": job_name,
        "job_run_id": get_run_id(),
        "execution_name": os.getenv("execution_name", ""),
        "execution_id": os.getenv("execution_id", ""),
        "execution_starttime": os.getenv("execution_starttime", ""),
        "statemachine_name": os.getenv("statemachine_name", ""),
        "statemachine_id": os.getenv("statemachine_id", ""),
        "state_name": os.getenv("state_name", ""),
        "state_enteredtime": os.getenv("state_enteredtime", ""),
        "status": status,
        "message": message,
        "aws_account_id": boto3.client("sts").get_caller_identity()["Account"],
    }

    print(json.dumps(event_data))

    try:
        splunk.log_message(event_data, get_run_id())
    except Exception as e:
        print(f"[ERROR] : log_event : Failed to write log to Splunk: {str(e)}")

    if status == "FAILED" and is_valid_email(os.getenv("teamsId")):

        glue_link = (
            f"https://us-east-1.console.aws.amazon.com/gluestudio/home?region=us-east-1"
            f"#/editor/job/{event_data.get('job_name')}/runs"
        )

        stepfn_link = (
            f"https://us-east-1.console.aws.amazon.com/states/home?region=us-east-1"
            f"#/v2/executions/details/{event_data.get('execution_id')}"
        )

        subject = f"[ALERT] Glue job Failure - {event_data.get('job_name')} ({status})"

        body_text = (
            f"Glue job failed.\n\n"
            f"Event Time: {event_data.get('event_time')}\n"
            f"Client: {event_data.get('clientName')} ({event_data.get('clientID')})\n"
            f"Job: {event_data.get('job_name')}\n"
            f"Input File: {event_data.get('file_name')}\n"
            f"Status: {event_data.get('status')}\n"
            f"Message: {event_data.get('message')}\n"
            f"Glue Job: {glue_link}\n"
            f"Step Function Execution: {stepfn_link}\n"
        )

        body_html = f"""<html>
        <body>
          <h3>Glue Job Failure Notification</h3>
          <ul>
            <li><b>Event Time:</b> {event_data.get('event_time')}</li>
            <li><b>Client:</b> {event_data.get('clientName')} ({event_data.get('clientID')})</li>
            <li><b>Job Name:</b> {event_data.get('job_name')}</li>
            <li><b>Input File:</b> {event_data.get('file_name')}</li>
            <li><b>Status:</b> {event_data.get('status')}</li>
            <li><b>Message:</b> {event_data.get('message')}</li>
          </ul>
          <p><a href="{glue_link}">View Glue Job</a></p>
          <p><a href="{stepfn_link}">View Step Function Execution</a></p>
        </body>
        </html>"""

        sender = os.getenv("ALERT_SENDER_EMAIL", "noreply@broadridge.com")
        try:
            ses_client = boto3.client("ses", region_name="us-east-1")
            response = ses_client.send_email(
                Destination={"ToAddresses": [os.getenv("teamsId")]},
                Message={
                    "Body": {
                        "Html": {"Charset": "UTF-8", "Data": body_html},
                        "Text": {"Charset": "UTF-8", "Data": body_text},
                    },
                    "Subject": {"Charset": "UTF-8", "Data": subject},
                },
                Source=sender,
            )
            print(f"Email sent! MessageId: {response['MessageId']}")
        except ClientError as e:
            print(f"[ERROR] Failed to send email::Error={traceback.format_exc()}::Exception Msg={e}")


def get_secret_values(secret_name):
    try:

        response = ssm_client.get_secret_value(SecretId=secret_name)
        secret_value = response['SecretString']
        json_secret_value = json.loads(secret_value)

        return json_secret_value
    except Exception as e:
        raise Exception(f"Error retrieving secret values for {secret_name}: {str(e)}") from e


def create_connecton(ssmdbkey, database):
    try:
        ssm = boto3.client('ssm', region_name='us-east-1')

        secret_name = ssm.get_parameter(Name=ssmdbkey, WithDecryption=True)
        secret_name = secret_name['Parameter']['Value']

        host = get_secret_values(secret_name)

        conn_engine = create_engine(
            f"postgresql://{host['username']}:{host['password']}@{host['host']}:{host['port']}/{database}",
            echo=True)

        return conn_engine

    except Exception as e:
        raise Exception(f"Error creating database connection: {str(e)}.") from e


############################################################################
def create_table(engine, key_columns, table_name, schema):
    """
    Create a table with dynamic multiple key columns + index.
    Index will be called {table_name}_idx_1
    """
    try:
        log_event("INFO", f"Creating table {table_name} in schema {schema} with keys {key_columns}")

        # Validate key_columns
        if not key_columns:
            raise ValueError("No key columns provided")

        metadata = MetaData(schema=schema)
        dot_index = table_name.rfind('.')
        table_name_clean = table_name[:dot_index] if dot_index != -1 else table_name

        # Define columns
        columns = [
            Column('ID', BigInteger, primary_key=True, autoincrement=True)
        ]

        for col in key_columns:
            if not col or not col.strip():
                raise ValueError(f"Invalid column name: '{col}'")
            columns.append(Column(col.strip(), String))

        columns.append(Column('JSON_DATA', JSON))

        my_table = Table(
            table_name_clean,
            metadata,
            *columns
        )

        full_table_name = f"{schema}.{table_name_clean}"

        # Drop and recreate
        if engine.dialect.has_table(engine.connect(), table_name_clean, schema=schema):
            log_event("INFO", f"Table {full_table_name} already exists... Dropping and recreating.")
            my_table.drop(engine, checkfirst=True)

        metadata.create_all(engine)

        # --- Create Index dynamically ---
        if key_columns:
            index_name = f"{table_name_clean}_idx_1"
            column_list = ', '.join([f'"{col}"' for col in key_columns])
            create_index_sql = f'CREATE INDEX "{index_name}" ON "{schema}"."{table_name_clean}" ({column_list});'
            with engine.connect() as conn:
                conn.execute(text(create_index_sql))

            log_event("INFO", f"Index {index_name} created on {key_columns}")

        log_event("INFO", f"Table {table_name_clean} created successfully.")
        return my_table
    except Exception as e:
        raise Exception(f"Error creating table {table_name}: {str(e)}") from e


###########################################################################
def load_stage():
    """
    Modified: Supports multiple keys for HHNMADDR
    Trim spaces on key columns during insert.
    """
    try:
        log_event("INFO", "Starting load_stage process.")
        response = s3_client.get_object(Bucket=os.getenv("bucket_name"), Key=os.getenv("object_key"))
        json_content = response['Body'].read().decode('utf-8')
        load_data = json.loads(json_content)

        config_data = json.loads(os.getenv("key_location"))
        filename = os.getenv("file_name")

        table_key_index = ""
        database = ""
        schema = ""
        base_name = ""
        alternate_name = ""
        retention_days = -1
        table_name = ""
        columns_to_be_created = []
        for key in config_data:
            if key in filename:
                columns_to_be_created = config_data[key].get("columns_to_be_created")
                key_index = config_data[key].get('key_index')
                database = config_data[key]['database']
                schema = config_data[key]['schema']
                base_name = key
                retention_days = int(config_data[key].get('retention_days', -1))
                alternate_name = config_data[key].get('alternate_name', '')
                table_key_index = key_index

        # multiple indexes

        log_event(
            "INFO",
            f"key_index={key_index}, database={database}, schema={schema}, base_name={base_name}, retention_days={retention_days}, "
            f"alternate_name={alternate_name}"
        )
        #Use columns_to_be_created if provided, otherwise fall back to key_index logic
        key_columns=[]
        if table_key_index is None or table_key_index == "":
            if columns_to_be_created:
                key_columns = columns_to_be_created
                log_event("INFO", f"Using columns_to_be_created from config: {key_columns}")
        else:
            possible_indexes = [int(i) for i in str(table_key_index).split('|')]
            log_event("INFO",f"table_key_index={table_key_index}, possible_indexes={possible_indexes}")
            json_keys = list(load_data["json_data"][0].keys())
            for idx in possible_indexes:
                if idx >= len(json_keys):
                    raise ValueError(
                        f"Index {idx} is out of range. JSON data only has {len(json_keys)} json_keys={json_keys}")

                column_name = json_keys[idx].strip()
                if not column_name:
                    raise ValueError(f"Empty column name found at index {idx}")
                key_columns.append(column_name)

        if not key_columns:
            raise ValueError("No key columns found in the data")

        # Log the key columns for debugging
        log_event("INFO", f"Found key columns: {key_columns}")

        # --- Connect & Create Table + Index
        engine = create_connecton(os.getenv("dbkey"), database)
        ######################  BRCCICSPPI 5894 ######################
        if alternate_name:
            base_name = alternate_name
        # print (f"this is alternate_name --- ----> {alternate_name}")
        file_name_table = create_table(engine, key_columns, base_name, schema)
        table_name = base_name
        #####################################################
        '''
        if alternate_name:
            file_name_table = create_table(engine, key_columns, filename.replace(base_name, alternate_name), schema)
            table_name = filename.replace(base_name, alternate_name)
        else:
            file_name_table = create_table(engine, key_columns, base_name, schema)
            table_name = base_name
        '''
        log_event(
            "INFO",
            f"Table to be used for loading data: {table_name}, file_name_table: {file_name_table}, "
            f"Resolved key columns: {key_columns}, Configuration resolved: table_key_index={table_key_index}, "
            f"database={database}, schema={schema}, base_name={base_name}, retention_days={retention_days}, "
            f"alternate_name={alternate_name}, table_key_index={table_key_index}"
        )
        # --- Insert Data (trimmed)
        batch_size = int(os.getenv("batch_size"))
        batch_records = []

        for record in load_data["json_data"]:
            data_to_insert = {}
            for col in key_columns:
                val = record.get(col, None)
                if filename.startswith("BatchAndEntryCodes_"):
                    data_to_insert[col] = val
                else:
                    if isinstance(val, str):
                        val = val.strip()
                    data_to_insert[col] = val
            data_to_insert['JSON_DATA'] = record
            batch_records.append(data_to_insert)
            if len(batch_records) >= batch_size:
                insert_stmt = insert(file_name_table)
                with engine.begin() as connection:
                    connection.execute(insert_stmt, batch_records)
                batch_records.clear()
        if batch_records:
            insert_stmt = insert(file_name_table)
            with engine.begin() as connection:
                connection.execute(insert_stmt, batch_records)
        log_event("INFO", "Data loaded successfully.")
    except Exception as e:
        raise Exception(f"Error in load_stage: {str(e)}") from e


#####################################################################################
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
            log_event("INFO", "load_stage completed copying into second table successfully.")
    except Exception as e:
        raise Exception(f"Error during second table create of load_stage: {str(e)}") from e


def delete_old_tables(engine, base_table_name, days_old, schema):
    """
    Deletes tables starting with base_table_name and created before n days.
    """
    try:
        # Calculate the cutoff date
        cutoff_date = datetime.now() - timedelta(days=days_old)
        log_event("INFO",
                  f"Starting delete_old_tables with base_table_name={base_table_name}, days_old={days_old}, schema={schema}, cutoff_date={cutoff_date}")

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
                    log_event("INFO", f"Table {table} dropped successfully.")
            except ValueError:
                # If the date format is not correct, skip the table
                continue

        log_event("INFO", f"Total number of tables deleted: {table_delete_count}")
    except Exception as e:
        raise Exception(f"Error deleting old tables: {str(e)}") from e


def set_job_params_as_env_vars():
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    messages = []
    messages.append(f"Set environment variable:")
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i + 1]
            os.environ[key] = value
            messages.append(f"{key}={value}")

    log_event("INFO", f"glue_loadstage::Environment variables set from job parameters: {', '.join(messages)}")


def main():
    try:
        set_job_params_as_env_vars()
        load_stage()
        log_event("ENDED", "glue_loadstage completed successfully.")
    except Exception as e:
        log_event("FAILED", f"glue_loadstage::Fatal Error={traceback.format_exc()}::Exception::{e}")
        raise e


if __name__ == '__main__':
    main()
