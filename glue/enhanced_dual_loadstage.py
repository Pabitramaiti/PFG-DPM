import json
import boto3
import os
import sys
import splunk
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from sqlalchemy import create_engine, MetaData, Table, Column, BigInteger, String, JSON, insert, inspect, Integer, DateTime, Text, func, text
import botocore.exceptions

#Global config file instance
_CONFIG = None
client = boto3.client("secretsmanager")
#Global Instances of sqlalchemy engine and metadata
_ENGINE = None
_METADATA = None

#Global S3 client instance
s3_client = boto3.client("s3")

#Global ASCII copybook cache
_ASCII_COPYBOOK_CACHE = {}

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:br_icsdev_dpmtest_conversionJsonToXml:"+ account_id

# Log messages to Splunk with status and message
def log_message(status, message):
    splunk.log_message({'FileName':inputFileName,'Status': status, 'Message': message}, get_run_id())


def load_ascii_copybook_from_s3(bucket_name, copybook_path):
    """Load ASCII copybook JSON from S3 and cache it"""
    global _ASCII_COPYBOOK_CACHE
    
    if copybook_path in _ASCII_COPYBOOK_CACHE:
        return _ASCII_COPYBOOK_CACHE[copybook_path]
    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=copybook_path)
        copybook_data = json.loads(response["Body"].read().decode("utf-8"))
        _ASCII_COPYBOOK_CACHE[copybook_path] = copybook_data
        log_message("success", f"Loaded ASCII copybook from S3: {copybook_path}")
        return copybook_data
    except Exception as e:
        message = f"Error loading ASCII copybook from S3: {e}"
        print(message)
        log_message("failed", message)
        raise Exception(message)

def get_fields_from_ascii_copybook(load_stage_config_id, bucket_name):
    """Extract field headers from ASCII copybook for the given config ID"""
    try:
        # Get ASCII copybook path from config (required)
        ascii_copybook_path = get_config().get(load_stage_config_id, {}).get("asciiCopybookPath", "")
        
        if not ascii_copybook_path:
            raise ValueError(f"asciiCopybookPath is required in config for {load_stage_config_id}")
        
        log_message("info", f"Loading ASCII copybook from: {ascii_copybook_path}")
        
        # Load ASCII copybook
        copybook_data = load_ascii_copybook_from_s3(bucket_name, ascii_copybook_path)
        
        # Validate copybook structure
        if not isinstance(copybook_data, list):
            raise ValueError("ASCII copybook must be a list of records")
        
        # Extract fields for the specific record type
        for record in copybook_data:
            if not isinstance(record, dict):
                continue
                
            if record.get("rec_type") == load_stage_config_id:
                fields = record.get("fields", [])
                if not isinstance(fields, list):
                    raise ValueError(f"Fields must be a list for record type {load_stage_config_id}")
                    
                if fields:
                    log_message("success", f"Successfully loaded {len(fields)} fields from ASCII copybook for {load_stage_config_id}")
                else:
                    log_message("info", f"No fields found in ASCII copybook for {load_stage_config_id}")
                    
                return fields
        
        # If no matching record type found, fall back to config
        log_message("info", f"No matching record type '{load_stage_config_id}' found in ASCII copybook, falling back to config")
        return get_config().get(load_stage_config_id, {}).get("delimitedFileHeaders", [])
        
    except Exception as e:
        message = f"Error loading ASCII copybook, falling back to config headers: {e}"
        print(message)
        log_message("failed", message)
        # Fall back to original config-based headers
        raise Exception(message)

def get_secret_values(secret_name):
    try:
        response = client.get_secret_value(SecretId=secret_name)

        return json.loads(response['SecretString'])
    except Exception as e:
        message = f"Error retrieving secret values: {e}"
        log_message("failed", message)
        raise Exception(message)

def load_config_from_s3(bucket_name, file_key):
    global _CONFIG
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        _CONFIG = json.loads(response["Body"].read().decode("utf-8"))
        print("Configuration has been loaded successfully.")
        log_message("success", "Configuration has been loaded successfully.")
    except Exception as e:
        message=f"Error retrieving file from S3: {e}"
        log_message("failed", message)
        raise Exception(message)

def get_config():
    global _CONFIG
    if _CONFIG is None:
        raise ValueError("Configuration has not been loaded yet.")
    return _CONFIG

def generate_frequent_fields_columns(load_stage_config_id):
    """Dynamically generate column definitions for frequent fields"""
    config = get_config().get(load_stage_config_id, {})
    frequent_fields = config.get("frequentFields", [])
    
    if not frequent_fields:
        print(f"No frequent fields defined for {load_stage_config_id}")
        log_message("info", f"No frequent fields defined for {load_stage_config_id}")
        return []
    
    generated_columns = []
    print(f"Generating columns for {len(frequent_fields)} frequent fields: {frequent_fields}")

    for field in frequent_fields:
        column = {
            "definition": {
                "columnName": field.lower().replace("_", "-"),
                "columnDatatype": "String",
                "isNullable": True
            },
            "dataSource": {
                "type": "frequentFields",
                "frequentFields": field
            }
        }
        generated_columns.append(column)
        print(f"Generated column: {column['definition']['columnName']} -> {field}")

    return generated_columns

def loadDBEngine():
    database_name = os.getenv("databaseName")
    database_ssm_key = os.getenv("databaseSMKey")
    aws_region = os.getenv("AWS_REGION", "us-east-1")  # Allow region to be configurable
    # Create an SSM client
    ssm_client = boto3.client('ssm', region_name=aws_region)
    
    # Retrieve a parameter from the Parameter Store
    response = ssm_client.get_parameter(Name=database_ssm_key, WithDecryption=True)
    
    # Extract the parameter value
    secret_name = response['Parameter']['Value']

    host = get_secret_values(secret_name)

    # Create an SQLAlchemy engine/database connection
    DATABASE_URL = f"postgresql://{host['username']}:{host['password']}@{host['host']}:{host['port']}/{database_name}"
    return create_engine(DATABASE_URL,future=True, echo=True)

def build_dynamic_table_name(base_name: str, table_schema: dict) -> str:

    """
    Author: @Anurag Das
    Purpose:
        If appendDateTimeStampToTable is True, treat the table name itself as a pattern
        and replace any <YYYY>, <YY>, <MM>, <DD>, <HH>, <mm>, <SS>, etc. tokens.
        Otherwise returns the name unchanged.
    Returns:
        str : Final table name with placeholders replaced when applicable.
    """

    if not table_schema.get("appendDateTimeStampToTable", False):
        return base_name

    now = datetime.now()
    replacements = {
        "<YYYY>": now.strftime("%Y"),           # 4-digit year 2025
        "<YY>": now.strftime("%y"),             # 2-digit year 25
        "<MM>": now.strftime("%m"),             # 2-digit month 01-12
        "<DD>": now.strftime("%d"),             # 2-digit day 01-31
        "<HH>": now.strftime("%H"),             # 2-digit hour 00-23
        "<hh>": now.strftime("%I"),             # 12‑hour
        "<mm>": now.strftime("%M"),             # 2-digit minute 00-59
        "<SS>": now.strftime("%S"),             # 2-digit second 00-59
        "<MONTH>": now.strftime("%b"),          # Abbreviated month name
        "<MONTH_FULL>": now.strftime("%B"),     # Full month name
        "<DAY>": now.strftime("%a"),            # Abbreviated day name
        "<DAY_FULL>": now.strftime("%A"),       # Full day name
        "<JD>": now.strftime("%j"),             # Day of the year
        "<WEEK>": now.strftime("%W"),           # Week number
        "<AMPM>": now.strftime("%p"),           # AM/PM
        "<TZ>": now.strftime("%Z"),             # Timezone
        "<MMDDYY>": now.strftime("%m%d%y"),     # MMDDYY format
    }

    token_pattern = re.compile("|".join(sorted(map(re.escape, replacements.keys()), key=len, reverse=True)))
    result = token_pattern.sub(lambda m: replacements[m.group(0)], base_name)

    return result

def get_table_definition(load_stage_config_id):
    _METADATA.reflect(bind=_ENGINE)

    config = get_config().get(load_stage_config_id, {})
    table_schema = config.get("tableSchema", {})
    table_name = table_schema.get("tableName", "").strip()
    schema_name = os.getenv("databaseSchema")
    table_with_schema = f"{schema_name}.{table_name}" if schema_name else table_name

    table_name = build_dynamic_table_name(table_name, table_schema)

    # Enhanced table existence check using inspector
    inspector = inspect(_ENGINE)
    table_exists = inspector.has_table(table_name, schema=schema_name)
    
    if table_exists and table_with_schema in _METADATA.tables:
        log_message("info", f"Table '{table_with_schema}' already exists (confirmed by inspector).")
        return _METADATA.tables[table_with_schema], True  # <- Return True if it exists
    elif table_exists:
        # Table exists in DB but not in metadata - re-reflect and check again
        _METADATA.clear()
        _METADATA.reflect(bind=_ENGINE)
        if table_with_schema in _METADATA.tables:
            log_message("info", f"Table '{table_with_schema}' exists - backup operations will be performed.")
            return _METADATA.tables[table_with_schema], True
        else:
            log_message("warning", f"Table exists in DB but metadata reflection failed for '{table_with_schema}'.")

    # define and create table as before...
    table_schema = get_config().get(load_stage_config_id, {}).get("tableSchema", {})
    do_render_id_as_primary_key = get_config().get(load_stage_config_id, {}).get("renderIdAsPrimaryKey", False)

    columns = []

    if do_render_id_as_primary_key:
        columns.append(Column("id", BigInteger, primary_key=True))

    # Get static columns from config
    static_columns = table_schema.get("columns", [])
    
    # Generate dynamic columns for frequent fields
    frequent_field_columns = generate_frequent_fields_columns(load_stage_config_id)
    
    # Merge static and dynamic columns
    all_columns = static_columns + frequent_field_columns

    for col in all_columns:
        col_def = col.get("definition", {})
        column_name = col_def.get("columnName", "").strip()
        column_type = col_def.get("columnDatatype", "").strip().lower()
        is_nullable = col_def.get("isNullable", True)
        default_value = col_def.get("defaultIfAbsent", None)

        if column_type == "string":
            column_type = String
        elif column_type == "json":
            column_type = JSON
        elif column_type == "datetime":
            column_type = DateTime
        else:
            raise ValueError(f"Unsupported column type: {column_type}")

        if default_value is not None:
            columns.append(Column(column_name, column_type, nullable=is_nullable, default=default_value))
        else:
            columns.append(Column(column_name, column_type, nullable=is_nullable))

    if get_config().get(load_stage_config_id, {}).get("renderDataAsJson", False):
        columns.append(Column("json_data", JSON, nullable=False))
    if get_config().get(load_stage_config_id, {}).get("renderCreatedTimeStamp", False):
        columns.append(Column("created_at", DateTime, default=func.now()))

    db_table = Table(
        table_name,
        _METADATA,
        *columns,
        schema=os.getenv("databaseSchema")
    )
    _METADATA.create_all(_ENGINE, tables=[db_table])

    return db_table, False  # <- False means it was just created

def get_insert_data(json_data, load_stage_config_id):
    # load_stage_config_id = os.getenv("loadStageConfigId")
    static_columns = get_config().get(load_stage_config_id, {}).get("tableSchema", {}).get("columns", [])
    frequent_field_columns = generate_frequent_fields_columns(load_stage_config_id)
    
    # Combine static and dynamic columns
    all_columns = static_columns + frequent_field_columns
    
    print(f"ColumnsConfig: {len(static_columns)} static + {len(frequent_field_columns)} frequent = {len(all_columns)} total columns")
    data = {}
    
    for column in all_columns:
        column_name = column.get("definition", {}).get("columnName", "").strip()
        read_type = column.get("dataSource", {}).get("type", "").strip().lower()

        if read_type == "argument":
            arg = column.get("dataSource", {}).get("argument", {}).get("name", "").strip()
            column_value = os.getenv(arg)
        elif read_type == "frequentfields":
            token = column.get("dataSource", {}).get("frequentFields", "").strip()
            column_value = json_data.get(token, "")
        elif read_type == "none":
            # Handle columns with type "none"- use default value if available
            column_value = column.get("definition", {}).get("defaultIfAbsent", "")
        else:
            continue

        data[column_name] = column_value

    if get_config().get(load_stage_config_id, {}).get("renderDataAsJson", False):
        column_name = "json_data"
        data[column_name] = json_data

    return data

def _truncate_table_if_exists(connection, db_table):
    try:
        full_table_name = f"{db_table.schema}.{db_table.name}"if db_table.schema else db_table.name
        connection.execute(text(f"TRUNCATE TABLE {full_table_name}"))
        log_message("success", f"Table '{full_table_name}' truncated successfully.")
    except Exception as e:
        print(f"Could not truncate table '{full_table_name}': {e}")
        log_message("failed", f"Could not truncate table '{full_table_name}': {e}")

def persistJsonData(insert_data_list, load_stage_config_id, batch_size=1000):
    global _ENGINE, _METADATA

    if _ENGINE is None:
        _ENGINE = loadDBEngine()
    if _METADATA is None:
        _METADATA = MetaData()

    db_table, was_table_already_existing = get_table_definition(load_stage_config_id)

    try:
        with _ENGINE.connect() as connection:
            inspector = inspect(_ENGINE)
            table_name = db_table.name
            schema_name = db_table.schema
            full_table_name = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'

            # Double-check table existence using inspector (safety measure)
            actual_table_exists = inspector.has_table(table_name, schema=schema_name)
            
            # Use the more reliable check: if table actually exists in DB, perform backup
            should_backup = was_table_already_existing or actual_table_exists

            if should_backup:
                
                # --- Step 1: Create backup table if not exists ---
                backup_table_name = f"{table_name}_backup"
                full_backup_table_name = f'"{schema_name}"."{backup_table_name}"' if schema_name else f'"{backup_table_name}"'

                if not inspector.has_table(backup_table_name, schema=schema_name):
                    log_message("info", f"Creating backup table: {full_backup_table_name}")
                    
                    # Create backup table with same structure + backup timestamp
                    connection.execute(text(f"""
                        CREATE TABLE {full_backup_table_name} AS
                        SELECT *, CURRENT_TIMESTAMP as backup_ts FROM {full_table_name} WHERE 1=0;
                    """))
                    
                    # Ensure backup_ts column exists with proper type
                    connection.execute(text(f"""
                        ALTER TABLE {full_backup_table_name}
                        ALTER COLUMN backup_ts SET DEFAULT CURRENT_TIMESTAMP;
                    """))
                else:
                    # Ensure backup_ts column exists in existing backup table
                    try:
                        connection.execute(text(f"""
                            ALTER TABLE {full_backup_table_name}
                            ADD COLUMN IF NOT EXISTS backup_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                        """))
                    except Exception as e:
                        pass  # Column likely already exists

                # --- Step 2: Backup current data (if exists) ---
                if inspector.has_table(table_name, schema=schema_name):
                    # First check if there's any data to backup
                    result = connection.execute(text(f"SELECT COUNT(*) as count FROM {full_table_name}"))
                    row_count = result.fetchone()[0]
                    
                    if row_count > 0:
                        # Check if main table has created_at column to preserve original timestamps
                        main_table_columns = [col.name for col in db_table.columns]
                        
                        if 'created_at' in main_table_columns:
                            try:
                                # Simple approach: just copy all data and update backup_ts separately
                                connection.execute(text(f"""
                                    INSERT INTO {full_backup_table_name} 
                                    SELECT * FROM {full_table_name};
                                """))
                                # Update backup_ts to match created_at for the rows we just inserted
                                connection.execute(text(f"""
                                    UPDATE {full_backup_table_name} 
                                    SET backup_ts = created_at 
                                    WHERE backup_ts IS NULL OR backup_ts = CURRENT_TIMESTAMP;
                                """))
                                log_message("success", f"Backed up {row_count} rows with preserved timestamps")
                            except Exception as e:
                                print(f"Error in backup with timestamp preservation: {e}")
                                connection.execute(text(f"""
                                    INSERT INTO {full_backup_table_name} 
                                    SELECT * FROM {full_table_name};
                                """))
                                log_message("success", f"Backed up {row_count} rows with default timestamps")
                        else:
                            try:
                                connection.execute(text(f"""
                                    INSERT INTO {full_backup_table_name} 
                                    SELECT * FROM {full_table_name};
                                """))
                                log_message("success", f"Backed up {row_count} rows")
                            except Exception as e:
                                print(f"Error in backup: {e}")
                                raise e

                # --- Step 3: Truncate original table ---
                try:
                    # Perform truncation
                    connection.execute(text(f'TRUNCATE TABLE {full_table_name} RESTART IDENTITY CASCADE;'))
                    
                    # Verify truncation worked
                    result = connection.execute(text(f"SELECT COUNT(*) as count FROM {full_table_name}"))
                    rows_after = result.fetchone()[0]
                    
                    if rows_after == 0:
                        log_message("success", f"Successfully truncated and reset identity on {full_table_name}")
                    else:
                        raise Exception(f"Truncation failed - table still has {rows_after} rows")
                        
                except Exception as e:
                    message = f"Error truncating table: {e}"
                    print(f"Error truncating table: {e}")
                    log_message("failed", message)
                    raise Exception(message)

            # --- Step 4: Insert new data in batches ---
            total_rows = len(insert_data_list)
            
            for i in range(0, total_rows, batch_size):
                batch = insert_data_list[i:i + batch_size]
                try:
                    connection.execute(db_table.insert(), batch)
                except Exception as e:
                    print(f"Error inserting batch {i // batch_size + 1}: {e}")
                    raise

            connection.commit()
            
            # Verify final row count
            result = connection.execute(text(f"SELECT COUNT(*) as count FROM {full_table_name}"))
            final_count = result.fetchone()[0]
            
            if final_count == total_rows:
                log_message("success", f"Successfully inserted {total_rows} rows into {full_table_name}")
            else:
                print(f"WARNING: Row count mismatch. Expected {total_rows}, got {final_count}")
                log_message("warning", f"Row count mismatch in {full_table_name}. Expected {total_rows}, got {final_count}")

            # --- Optional: Retain only last N backups (only if backup was performed) ---
            if should_backup:
                backup_retention_count = int(os.getenv("backupRetentionCount", "30"))
                backup_table_name = f"{table_name}_backup"
                full_backup_table_name = f'"{schema_name}"."{backup_table_name}"' if schema_name else f'"{backup_table_name}"'
                
                connection.execute(text(f"""
                    DELETE FROM {full_backup_table_name}
                    WHERE ctid NOT IN (
                        SELECT ctid FROM {full_backup_table_name}
                        ORDER BY backup_ts DESC
                        LIMIT {backup_retention_count}
                    );
                """))
                log_message("success", f"Pruned backup table to last {backup_retention_count} records.")

    except Exception as e:
        message = f"Error during persistJsonData: {e}"
        print(f"{message}")
        log_message("failed", message)
        raise Exception(message)

def persistDelimitedData():
    print("Function not implemented. This is Future Enhancement.")

def prepareJsonifiedData(load_stage_config_id, input_file_content,bucket_name):
    delimiter = get_config().get(load_stage_config_id, {}).get("inputFileDelimiter", "").strip()
    delimiter = bytes(delimiter, "utf-8").decode("unicode_escape")
    
    #Check if skipHeader is present and set to True
    skip_header_config = get_config().get(load_stage_config_id, {}).get("skipHeader", "False")
    skip_header = str(skip_header_config).strip().lower() == "true"


    # Get headers from ASCII copybook instead of config to avoid redundancy
    # This function will fall back to config delimitedFileHeaders if ASCII copybook is not available
    headers = get_fields_from_ascii_copybook(load_stage_config_id,bucket_name)

    all_rows = []

    # Get suffixFields and prefixFields config if present
    suffix_fields = get_config().get(load_stage_config_id, {}).get("suffixFields", {})
    prefix_fields = get_config().get(load_stage_config_id, {}).get("prefixFields", {})

    def apply_prefix_suffix(row, prefix_fields, suffix_fields):
        for field in row:
            val = row[field]
            if val is not None:
                # Trim any whitespace from configured prefix/suffix values
                prefix = str(prefix_fields.get(field, "")).strip()
                suffix = str(suffix_fields.get(field, "")).strip()
                row[field] = prefix + str(val) + suffix
        return row

    for idx, line in enumerate(input_file_content):
        if skip_header and idx == 0:
            continue  # Skip header or first line
        values = line.decode("utf-8").strip().split(delimiter)
        values = [value.strip() for value in values]
        json_object = dict(zip(headers, values))
        # Apply both prefix and suffix if configured
        if suffix_fields or prefix_fields:
            json_object = apply_prefix_suffix(json_object, prefix_fields, suffix_fields)
        insert_data = get_insert_data(json_object, load_stage_config_id)
        all_rows.append(insert_data)

    batch_size = int(os.getenv("batchSize", "1000"))
    # batch_size = 100
    persistJsonData(all_rows, load_stage_config_id, batch_size=batch_size)

def process_delimited_data(load_stage_config_id, input_file_content,bucket_name):
    log_message("info", "Started processing delimited data.")
    try:
        do_render_json_data = get_config().get(load_stage_config_id, {}).get("renderDataAsJson", False)

        if do_render_json_data:
            prepareJsonifiedData(load_stage_config_id, input_file_content,bucket_name)
        else:
            persistDelimitedData()

        log_message("success", "Completed processing delimiter data.")
    except Exception as e:
        message = f"Error processing delimited data function: {e}"
        print(message)
        log_message("failed", message)
        raise Exception(message)

def process_json_data(load_stage_config_id, input_file_content):
    """Process JSON input files - currently not implemented"""
    error_msg = "JSON file processing is not yet implemented. Only delimited files are currently supported."
    print(f"{error_msg}")
    log_message("failed", error_msg)
    raise NotImplementedError(error_msg)

def process_single_file(file_config, bucket_name):
    """Process a single file with its configuration"""
    try:
        load_stage_config_id = file_config["loadStageConfigId"]
        file_key = file_config.get("fileKey")
        is_optional = file_config.get("isOptional", False)

        # If fileKey is missing, search for file using filepath and regex
        if not file_key or file_key==" ":
            file_path = file_config.get("file_path")
            file_regex = file_config.get("file_pattern")
            if file_path != " " and file_regex != " ":
                # List objects in the given S3 path
                paginator = s3_client.get_paginator("list_objects_v2")
                matches = []
                for page in paginator.paginate(Bucket=bucket_name, Prefix=file_path):
                    for obj in page.get("Contents", []):
                        filename = os.path.basename(obj["Key"])
                        if re.match(file_regex, filename):
                            matches.append(obj["Key"])

                if len(matches) == 1:
                    file_key = matches[0]
                elif len(matches) == 0:
                    if is_optional:
                        return {
                            "status": "skipped",
                            "file": None,
                            "reason": "Optional file not found by regex"
                        }
                    else:
                        raise FileNotFoundError(f"Required file not found by regex: {file_regex} in {file_path}")
                else:
                    # Multiple matches is an ambiguous situation - fail explicitly
                    log_message("failure", f"Multiple files found matching pattern '{file_regex}' under {bucket_name}/{file_path}: {matches}")
                    raise RuntimeError(f"Multiple files found matching pattern '{file_regex}' under {bucket_name}/{file_path}: {matches}")
            else:
                raise ValueError("fileKey missing and no filepath/regex provided")

        # Check if file exists before attempting to process
        try:
            s3_client.head_object(Bucket=bucket_name, Key=file_key)
        except s3_client.exceptions.NoSuchKey:
            if is_optional:
                return {
                    "status": "skipped", 
                    "file": file_key,
                    "reason": "Optional file not found"
                }
            else:
                raise FileNotFoundError(f"Required file not found: {file_key}")
        # As file is failing with Not Found Error for is optional files
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404" and is_optional:
                return {"status": "skipped", "file": file_key, "reason": "Optional file not found"}
            else:
                raise FileNotFoundError(f"Required file not found: {file_key}")
        except Exception as e:
            message = f"Error checking file existence: {e}"
            log_message("failed", message)
            raise Exception(message)
        
        input_file_type = get_config().get(load_stage_config_id, {}).get("inputFileType", "").strip().lower()
        # Get the S3 object (file content) using global client
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        input_file_content = s3_object["Body"].iter_lines()

        if input_file_type == "delimited":
            process_delimited_data(load_stage_config_id, input_file_content,bucket_name)
        elif input_file_type == "json":
            process_json_data(load_stage_config_id, input_file_content)
        else:
            raise ValueError(f"Invalid input file type: {input_file_type}")
            
        log_message("success", f"Successfully processed file: {file_key}")
        return {
            "status": "success", 
            "file": file_key
        }
        
    except Exception as e:
        message = f"Error processing file {file_config.get('fileKey', 'unknown')}: {e}"
        print(message)
        log_message("failed", message)
        
        return {
            "status": "error", 
            "file": file_config.get('fileKey', 'unknown'), 
            "error": str(e)
        }

def process_files_parallel(files_to_process, bucket_name, max_workers):
    """Process multiple files in parallel using ThreadPoolExecutor"""
    max_workers = min(len(files_to_process), int(max_workers))
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all files for processing
        future_to_file = {executor.submit(process_single_file, file_config, bucket_name): file_config 
                         for file_config in files_to_process}
        
        # Process completed tasks
        for future in as_completed(future_to_file):
            file_config = future_to_file[future]
            try:
                result = future.result()
                results.append(result)
                if result['status'] == 'success':
                    pass  # Success handled by logging in process_single_file
                elif result['status'] == 'skipped':
                    print(f"Skipped: {result['file']} - {result.get('reason', 'Optional file')}")
                else:
                    print(f"Failed: {result['file']} - {result.get('error', 'Unknown error')}")
            except Exception as e:
                error_result = {
                    "status": "error", 
                    "file": file_config.get('fileKey', 'unknown'), 
                    "error": str(e)
                }
                results.append(error_result)
                print(f"Failed: {error_result['file']} - {str(e)}")
    
    return results

def main():
    log_message("info", "Load Stage Glue Execution Started.")
    try:
        global inputFileName
        inputFileName = os.getenv('inputFileName')
        bucket_name = os.getenv('bucketName')
        max_workers= os.getenv('maxWorkers','5')
        if not bucket_name:
            raise ValueError("bucketName environment variable is required")
            
        load_config_from_s3(bucket_name, os.getenv("loadStageConfig"))
        
        # Check if we're processing multiple files or single file
        files_to_process_env = os.getenv("filesToProcess", "").strip()
        enable_parallel = os.getenv("enableParallelProcessing", "false").strip().lower() == "true"
        
        if files_to_process_env:
            # Multiple files mode - expect JSON array of file configurations
            try:
                # Parse the JSON from environment variable
                raw_files_data = json.loads(files_to_process_env)
                
                # Handle Step Functions JsonToString double-encoding
                if isinstance(raw_files_data, str):
                    raw_files_data = json.loads(raw_files_data)
                
                files_to_process = []
                # Use standard format - ensure isOptional field exists
                if isinstance(raw_files_data, list) and len(raw_files_data) > 0:
                    for item in raw_files_data:
                        if "isOptional"not in item:
                            item["isOptional"] = False  # Default to required
                        files_to_process.append(item)
                    print(f"Using standard format with {len(files_to_process)} items")
                else:
                    files_to_process = raw_files_data
                
                if not isinstance(files_to_process, list):
                    raise ValueError(f"filesToProcess must be a JSON array, but got {type(files_to_process).__name__}: {files_to_process}")
                
                # Determine processing approach based on file count and settings
                if enable_parallel and len(files_to_process) > 1:
                    # Use parallel processing for multiple files
                    results = process_files_parallel(files_to_process, bucket_name, max_workers)
                elif len(files_to_process) == 1:
                    # Single file - use direct processing (more efficient)
                    result = process_single_file(files_to_process[0], bucket_name)
                    results = [result]
                else:
                    # Sequential processing for multiple files
                    results = []
                    for file_config in files_to_process:
                        result = process_single_file(file_config, bucket_name)
                        results.append(result)
                
                # Summary report
                successful = [r for r in results if r["status"] == "success"]
                failed = [r for r in results if r["status"] == "error"]
                skipped = [r for r in results if r["status"] == "skipped"]
                
                print(f"Processing Summary:")
                print(f"Successful: {len(successful)}")
                print(f"Failed: {len(failed)}")
                print(f"Skipped (Optional): {len(skipped)}")
                print(f"Total: {len(results)}")
                
                if skipped:
                    print("\nSkipped files (optional):")
                    for skip in skipped:
                        print(f"- {skip['file']}: {skip['reason']}")
                
                if failed:
                    print("\nFailed files:")
                    for fail in failed:
                        print(f"- {fail['file']}: {fail['error']}")
                    raise Exception(f"{len(failed)} file(s) failed to process. See logs for details.")
                        
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in filesToProcess environment variable: {e}")
            except Exception as e:
                message = f"Error during load_stage: {e}"
                print(message)
                log_message("failed", message)
                raise Exception(message)

        log_message("success", "Load Stage Glue Execution Completed.")
    except ValueError as ve:
        message=f"ValueError: {ve}"
        print(message)
        log_message("failed", message)
        raise Exception(message)
    except Exception as e:
        message = f"Error during load_stage: {e}"
        print(message)
        log_message("failed", message)
        raise Exception(message)

def set_job_params_as_env_vars():
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i + 1]
            os.environ[key] = value

inputFileName = ''
if __name__ == '__main__':
    set_job_params_as_env_vars()
    try:
        main()
    except Exception as e:
        message = f"Fatal error: {e}"
        log_message("failed",message)
        sys.exit(1)
