import json
import boto3
import os
import sys
import splunk
import re
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, monotonically_increasing_id, split, trim, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
import botocore.exceptions

# Initialize Spark and Glue contexts
spark = SparkSession.builder \
    .appName("LoadStageGlueJob") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.3") \
    .config("spark.driver.memory", "32g") \
    .config("spark.driver.maxResultSize", "16g") \
    .config("spark.executor.memory", "48g") \
    .config("spark.executor.memoryOverhead", "12g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.shuffle.partitions", "400") \
    .config("spark.default.parallelism", "240") \
    .config("spark.network.timeout", "1800s") \
    .config("spark.sql.broadcastTimeout", "1200") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.rpc.askTimeout", "600s") \
    .config("spark.rpc.lookupTimeout", "600s") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "2000") \
    .config("spark.sql.sources.parallelPartitionDiscovery.threshold", "64") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256MB") \
    .config("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", "512MB") \
    .getOrCreate()



sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)

# Global config file instance
_CONFIG = None
client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")
_ASCII_COPYBOOK_CACHE = {}

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:br_icsdev_dpmdi_spark_loadstage:" + account_id

def log_message(status, message):
    splunk.log_message({'FileName': inputFileName, 'Status': status, 'Message': message}, get_run_id())

def load_ascii_copybook_from_s3(bucket_name, copybook_path):
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
    try:
        ascii_copybook_path = get_config().get(load_stage_config_id, {}).get("asciiCopybookPath", "")
        if not ascii_copybook_path:
            raise ValueError(f"asciiCopybookPath is required in config for {load_stage_config_id}")

        log_message("info", f"Loading ASCII copybook from: {ascii_copybook_path}")
        copybook_data = load_ascii_copybook_from_s3(bucket_name, ascii_copybook_path)

        if not isinstance(copybook_data, list):
            raise ValueError("ASCII copybook must be a list of records")

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

        log_message("info", f"No matching record type '{load_stage_config_id}' found in ASCII copybook, falling back to config")
        return get_config().get(load_stage_config_id, {}).get("delimitedFileHeaders", [])
    except Exception as e:
        message = f"Error loading ASCII copybook, falling back to config headers: {e}"
        print(message)
        log_message("failed", message)
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
        message = f"Error retrieving file from S3 file : {file_key} : {e}"
        log_message("failed", message)
        raise Exception(message)

def get_config():
    global _CONFIG
    if _CONFIG is None:
        raise ValueError("Configuration has not been loaded yet.")
    return _CONFIG

def generate_frequent_fields_columns(load_stage_config_id):
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

def build_dynamic_table_name(base_name: str, table_schema: dict) -> str:
    if not table_schema.get("appendDateTimeStampToTable", False):
        return base_name

    now = datetime.now()
    replacements = {
        "<YYYY>": now.strftime("%Y"),
        "<YY>": now.strftime("%y"),
        "<MM>": now.strftime("%m"),
        "<DD>": now.strftime("%d"),
        "<HH>": now.strftime("%H"),
        "<hh>": now.strftime("%I"),
        "<mm>": now.strftime("%M"),
        "<SS>": now.strftime("%S"),
        "<MONTH>": now.strftime("%b"),
        "<MONTH_FULL>": now.strftime("%B"),
        "<DAY>": now.strftime("%a"),
        "<DAY_FULL>": now.strftime("%A"),
        "<JD>": now.strftime("%j"),
        "<WEEK>": now.strftime("%W"),
        "<AMPM>": now.strftime("%p"),
        "<TZ>": now.strftime("%Z"),
        "<MMDDYY>": now.strftime("%m%d%y"),
    }

    token_pattern = re.compile("|".join(sorted(map(re.escape, replacements.keys()), key=len, reverse=True)))
    result = token_pattern.sub(lambda m: replacements[m.group(0)], base_name)
    return result

def get_jdbc_url():
    """Get JDBC URL and credentials for PostgreSQL database connection"""
    database_name = os.getenv("databaseName")
    if not database_name:
        raise ValueError("databaseName environment variable is required")
    print(f"Database Name: {database_name}")

 #   database_ssm_key = '/dpmtest/dev/database-secret'
    database_ssm_key = os.getenv("databaseSMKey")
  #  print(f"Database key: {database_ssm_key}")
    if not database_ssm_key:
        raise ValueError("databaseSMKey environment variable is required")


    aws_region = os.getenv("AWS_REGION", "us-east-1")

    try:
        ssm_client = boto3.client('ssm', region_name=aws_region)
        response = ssm_client.get_parameter(Name=database_ssm_key, WithDecryption=True)
        secret_name = response['Parameter']['Value']
        host = get_secret_values(secret_name)

        jdbc_url = f"jdbc:postgresql://{host['host']}:{host['port']}/{database_name}"
        return jdbc_url, host['username'], host['password']
    except Exception as e:
        raise Exception(f"Error getting database connection details: {str(e)}")

def create_spark_schema(load_stage_config_id):
    """Create Spark StructType schema from config"""
    print("starting create spark schema method")
    config = get_config().get(load_stage_config_id, {})
    table_schema = config.get("tableSchema", {})
    static_columns = table_schema.get("columns", [])
    frequent_field_columns = generate_frequent_fields_columns(load_stage_config_id)
    all_columns = static_columns + frequent_field_columns
    print(f"All columns(static + frequent): {all_columns}")

    fields = []
    do_render_id_as_primary_key = config.get("renderIdAsPrimaryKey", False)

    if do_render_id_as_primary_key:
        fields.append(StructField("id", LongType(), False))

    for col_config in all_columns:
        col_def = col_config.get("definition", {})
        column_name = col_def.get("columnName", "").strip()
        column_type = col_def.get("columnDatatype", "").strip().lower()
        is_nullable = col_def.get("isNullable", True)

        if column_type in ["string", "json"]:
            spark_type = StringType()
        elif column_type == "datetime":
            spark_type = TimestampType()
        else:
            spark_type = StringType()

        fields.append(StructField(column_name, spark_type, is_nullable))

    if config.get("renderDataAsJson", False):
        fields.append(StructField("json_data", StringType(), False))

    if config.get("renderCreatedTimeStamp", False):
        fields.append(StructField("created_at", TimestampType(), True))

    return StructType(fields)

def backup_and_truncate_table(jdbc_url, username, password, table_name, schema_name):
    print("Starting backup_and_truncate_table method")
    """In-database backup and truncate to avoid Spark OOM on large tables."""
    from py4j.java_gateway import java_import

    full_table_name = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
    backup_table_name = f"{table_name}_backup"
    full_backup_table_name = f'"{schema_name}"."{backup_table_name}"' if schema_name else f'"{backup_table_name}"'

    try:
        java_import(spark._jvm, "java.sql.DriverManager")
        conn = spark._jvm.DriverManager.getConnection(jdbc_url, username, password)
        conn.setAutoCommit(False)
        stmt = conn.createStatement()
        stmt.setQueryTimeout(0)

        # 1. Check table existence
        exists_sql = f"""
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables
              WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
            )
        """
        rs = stmt.executeQuery(exists_sql)
        rs.next()
        if not rs.getBoolean(1):
            rs.close()
            stmt.close()
            conn.commit()
            conn.close()
            log_message("info", f"Table {full_table_name} does not exist - skipping backup.")
            return
        rs.close()

        # 2. Create backup table WITHOUT constraints (allows duplicate IDs across backups)
        create_sql = f"""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = '{schema_name}' AND table_name = '{backup_table_name}'
          ) THEN
            -- Copy structure WITHOUT constraints (no PRIMARY KEY, UNIQUE, etc.)
            CREATE TABLE {full_backup_table_name} AS
            SELECT * FROM {full_table_name} WHERE 1=0;

            -- Add backup timestamp column
            ALTER TABLE {full_backup_table_name} ADD COLUMN backup_ts TIMESTAMP;
          END IF;

          -- Ensure backup_ts column exists (for tables created before this fix)
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = '{schema_name}'
              AND table_name = '{backup_table_name}'
              AND column_name = 'backup_ts'
          ) THEN
            ALTER TABLE {full_backup_table_name} ADD COLUMN backup_ts timestamp;
          END IF;
        END$$;
        """
        stmt.execute(create_sql)

        # 3. In-DB copy (no Spark read/write)
        insert_sql = f"""
            INSERT INTO {full_backup_table_name}
            SELECT *, CURRENT_TIMESTAMP AS backup_ts
            FROM {full_table_name}
        """
        stmt.execute(insert_sql)
        log_message("success", f"Backed up table {full_table_name} to {full_backup_table_name}")

        # 4. Truncate source
        stmt.execute(f"TRUNCATE TABLE {full_table_name} RESTART IDENTITY CASCADE")
        log_message("success", f"Truncated table {full_table_name}")

        # 5. Retention cleanup - keep backups from last N days
        backup_retention_count = int(os.getenv("backupRetentionDays", "30"))
        prune_sql = f"""
            DELETE FROM {full_backup_table_name}
            WHERE backup_ts < CURRENT_TIMESTAMP - INTERVAL '{backup_retention_count} days'
        """
        stmt.execute(prune_sql)
        log_message("success", f"Pruned backup table to retain last {backup_retention_count} days of backups")

        stmt.close()
        conn.commit()
        conn.close()
    except Exception as e:
        message = f"Error during backup: {e}"
        print(message)
        log_message("Error", message)
        raise e




def process_delimited_file_spark(load_stage_config_id, bucket_name, file_key):
    """Process delimited file using Spark"""
    try:
     #   print("line 329 process delimited spark")
        config = get_config().get(load_stage_config_id, {})
        delimiter = config.get("inputFileDelimiter", "").strip()
        delimiter = bytes(delimiter, "utf-8").decode("unicode_escape")
        skip_header = str(config.get("skipHeader", "False")).strip().lower() == "true"

        headers = get_fields_from_ascii_copybook(load_stage_config_id, bucket_name)
        s3_path = f"s3://{bucket_name}/{file_key}"
       # print("line 340 s3 path")
        print(s3_path)

        raw_df = spark.read.option("maxPartitionBytes", "67108864").text(s3_path)

        if skip_header:
            raw_df = raw_df.rdd.zipWithIndex().filter(lambda x: x[1] > 0).map(lambda x: x[0]).toDF()

        split_col = split(raw_df['value'], re.escape(delimiter))

        for idx, header in enumerate(headers):
            raw_df = raw_df.withColumn(header, trim(split_col.getItem(idx)))

        raw_df = raw_df.drop('value')

        suffix_fields = config.get("suffixFields", {})
        prefix_fields = config.get("prefixFields", {})

        for field in headers:
            if field in suffix_fields or field in prefix_fields:
                prefix = str(prefix_fields.get(field, "")).strip()
                suffix = str(suffix_fields.get(field, "")).strip()
                if prefix or suffix:
                    raw_df = raw_df.withColumn(
                        field,
                        concat(lit(prefix), col(field), lit(suffix))
                    )

        # *** CREATE JSON COLUMN FIRST (before filtering columns) ***
        if config.get("renderDataAsJson", False):
            json_struct = struct(*[col(h) for h in headers if h in raw_df.columns])
            raw_df = raw_df.withColumn("json_data", to_json(json_struct))

        static_columns = config.get("tableSchema", {}).get("columns", [])
        frequent_field_columns = generate_frequent_fields_columns(load_stage_config_id)
        all_columns = static_columns + frequent_field_columns
       # print("all columns::", all_columns)

        columns_to_select = []

        for column_config in all_columns:
            column_name = column_config.get("definition", {}).get("columnName", "").strip()
            read_type = column_config.get("dataSource", {}).get("type", "").strip().lower()

            if read_type == "frequentfields":
                source_field = column_config.get("dataSource", {}).get("frequentFields", "").strip()
                if source_field in raw_df.columns:
                    columns_to_select.append(col(source_field).alias(column_name))
            elif read_type == "argument":
                arg = column_config.get("dataSource", {}).get("argument", {}).get("name", "").strip()
                arg_value = os.getenv(arg, "")
                columns_to_select.append(lit(arg_value).alias(column_name))
            elif read_type == "none":
                default_val = column_config.get("definition", {}).get("defaultIfAbsent", "")
                columns_to_select.append(lit(default_val).alias(column_name))

        # Add json_data if it exists (already created above)
        if config.get("renderDataAsJson", False):
            columns_to_select.append(col("json_data"))

        # Select only the mapped columns
        final_df = raw_df.select(*columns_to_select)

        # Add timestamp if needed
        if config.get("renderCreatedTimeStamp", False):
            final_df = final_df.withColumn("created_at", current_timestamp())

        # Add ID if needed
        if config.get("renderIdAsPrimaryKey", False):
            final_df = final_df.withColumn("id", monotonically_increasing_id())

        num_partitions = 10
        final_df = final_df.repartition(num_partitions)

      #  print("=== Final DataFrame Schema ===")
      #  final_df.printSchema()
      #  print(f"Final columns: {final_df.columns}")

        return final_df

    except Exception as e:
        message = f"Error processing delimited file with Spark: {e}"
        print(message)
        log_message("failed", message)
        raise Exception(message)

def write_to_postgres_spark(df, load_stage_config_id):
    """Hybrid version: write DataFrame to PostgreSQL with explicit types, ordering, safety, and better logging"""
    try:
        #Load configuration and environment
        config = get_config().get(load_stage_config_id, {})
        table_schema = config.get("tableSchema", {})
        base_table_name = table_schema.get("tableName", "").strip()
        table_name = build_dynamic_table_name(base_table_name, table_schema)
        schema_name = os.getenv("databaseSchema")

        if not schema_name:
            raise EnvironmentError("Missing required environment variable: 'databaseSchema'")

        jdbc_url, username, password = get_jdbc_url()

        # JDBC connection properties
        jdbc_properties = {
            "user": username,
            "password": password,
            "driver": "org.postgresql.Driver",
            "batchsize": os.getenv("batchSize", "10000"),
            "isolationLevel": "READ_COMMITTED",
            "stringtype": "unspecified"
        }

        full_table_name = f'"{schema_name}"."{table_name}"'
        print(f"--- Preparing to write to table: {full_table_name} ---")

        # Backup and truncate existing data
        print("=== Starting backup and truncate ===")
        backup_and_truncate_table(jdbc_url, username, password, table_name, schema_name)
        print("=== Completed backup and truncate ===")

        # Build column type mapping (config-driven)
        column_type_map = {}
        static_columns = table_schema.get("columns", [])
        frequent_field_columns = generate_frequent_fields_columns(load_stage_config_id)
        all_columns = static_columns + frequent_field_columns

        for col_config in all_columns:
            col_def = col_config.get("definition", {})
            column_name = col_def.get("columnName", "").strip()
            column_type = col_def.get("columnDatatype", "").strip().lower()

            if column_name:  # Fill mapping table
                if column_type == "json":
                    column_type_map[column_name] = "json"
                elif column_type == "datetime":
                    column_type_map[column_name] = "timestamp"
                elif column_type in ["int", "integer", "bigint"]:
                    column_type_map[column_name] = "bigint"
                elif column_type in ["decimal", "float", "double"]:
                    column_type_map[column_name] = "numeric"
                else:
                    column_type_map[column_name] = "varchar"

        # Reorder columns (id, business fields, json_data, created_at)
        ordered_columns = []
        if config.get("renderIdAsPrimaryKey", False) and "id" in df.columns:
           ordered_columns.append("id")

        business_columns = [c for c in df.columns if c not in ["id", "json_data", "created_at"]]
        ordered_columns.extend(business_columns)

        if "json_data" in df.columns:
            ordered_columns.append("json_data")
        if "created_at" in df.columns:
            ordered_columns.append("created_at")

        df = df.select(*ordered_columns)

        # Cast json_data column if needed
        if "json_data" in df.columns:
            df = df.withColumn("json_data", col("json_data").cast(StringType()))
            print("Casted json_data column to StringType for PostgreSQL JSON compatibility")

        #  Create table if not exists with correct column types (config mapping)
        from py4j.java_gateway import java_import
        java_import(spark._jvm, "java.sql.DriverManager")

        conn = spark._jvm.DriverManager.getConnection(jdbc_url, username, password)
        stmt = conn.createStatement()

        check_table_sql = f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
            )
        """
        rs = stmt.executeQuery(check_table_sql)
        rs.next()
        table_exists = rs.getBoolean(1)
        rs.close()

        if not table_exists:
            columns_def = []
            if "id" in ordered_columns:
                columns_def.append('"id" BIGINT PRIMARY KEY')

            for col_name in business_columns:
                pg_type = column_type_map.get(col_name, "varchar")
                columns_def.append(f'"{col_name}" {pg_type.upper()}')

            if "json_data" in ordered_columns:
                columns_def.append('"json_data" JSON')
            if "created_at" in ordered_columns:
                columns_def.append('"created_at" TIMESTAMP')

            create_table_sql = f"CREATE TABLE {full_table_name} ({', '.join(columns_def)})"
            stmt.execute(create_table_sql)
            print(f"Created table {full_table_name} with schema based on config mapping")
            log_message("success", f"Created table {full_table_name}")
        else:
            print(f"Table {full_table_name} already exists")

        stmt.close()
        conn.close()

        # Write data using Spark JDBC
        print(f"=== Starting JDBC write to {full_table_name} ===")
        df.write.jdbc(
            url=jdbc_url,
            table=full_table_name,
            mode="append",
            properties=jdbc_properties
        )
        print(f"=== Completed JDBC write to {full_table_name} ===")

        #Logging success
        row_count = df.count()
        log_message("success", f"Successfully wrote {row_count} rows to {full_table_name}")

    except Exception as e:
        # Robust error diagnostics
        message = f"Error writing to PostgreSQL: {e}"
        print("=== ERROR DETAILS ===")
        print(f"Type: {type(e).__name__}")
        print(f"Message: {str(e)}")
        try:
            print("Schema:")
            df.printSchema()
            print("Columns:", df.columns)
        except Exception:
            print("Could not print DataFrame details during error inspection.")

        log_message("failed", message)
        raise Exception(message)

def process_single_file(file_config, bucket_name):
    """Process a single file with Spark"""
    try:
        load_stage_config_id = file_config["loadStageConfigId"]
        file_key = file_config.get("fileKey")
        is_optional = file_config.get("isOptional", False)

        if not file_key or file_key == " ":
            file_path = file_config.get("file_path")
            file_regex = file_config.get("file_pattern")
            if file_path != " " and file_regex != " ":
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
                        # Create table if file is optional and not found
                        config = get_config().get(load_stage_config_id, {})
                        table_schema = config.get("tableSchema", {})
                        base_table_name = table_schema.get("tableName", "").strip()
                        table_name = build_dynamic_table_name(base_table_name, table_schema)
                        schema_name = os.getenv("databaseSchema")
                        if not schema_name:
                            raise EnvironmentError("Missing required environment variable: 'databaseSchema'")
                        jdbc_url, username, password = get_jdbc_url()
                        write_to_postgres_spark(spark.createDataFrame([], create_spark_schema(load_stage_config_id)), load_stage_config_id)
                        log_message("info", f"Table {table_name} created as fileKey is not present.")
                        return {"status": "skipped", "file": None, "reason": "Optional file not found, table created"}
                    else:
                        raise FileNotFoundError(f"Required file not found by regex: {file_regex} in {file_path}")
                else:
                    log_message("failure", f"Multiple files found matching pattern '{file_regex}' under {bucket_name}/{file_path}: {matches}")
                    raise RuntimeError(f"Multiple files found matching pattern '{file_regex}'")
            else:
                raise ValueError("fileKey missing and no filepath/regex provided")

        try:
            s3_client.head_object(Bucket=bucket_name, Key=file_key)
        except s3_client.exceptions.NoSuchKey:
            if is_optional:
                # Create table if file is optional and not found
                config = get_config().get(load_stage_config_id, {})
                table_schema = config.get("tableSchema", {})
                base_table_name = table_schema.get("tableName", "").strip()
                table_name = build_dynamic_table_name(base_table_name, table_schema)
                schema_name = os.getenv("databaseSchema")
                if not schema_name:
                    raise EnvironmentError("Missing required environment variable: 'databaseSchema'")
                jdbc_url, username, password = get_jdbc_url()
                write_to_postgres_spark(spark.createDataFrame([], create_spark_schema(load_stage_config_id)), load_stage_config_id)
                log_message("info", f"Table {table_name} created as fileKey is not present.")
            #    print(f"Table 660 {table_name} created as fileKey is not present.")
               # return {"status": "skipped", "file": file_key, "reason": "Optional file not found, table created"}
                print(f"Optional file not found-skipping: {file_key}")
                return {"status": "skipped", "file": file_key, "reason": "Optional file not found"}
            else:
                raise FileNotFoundError(f"Required file not found: {file_key}")
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404" and is_optional:
                config = get_config().get(load_stage_config_id, {})
                table_schema = config.get("tableSchema", {})
                base_table_name = table_schema.get("tableName", "").strip()
                table_name = build_dynamic_table_name(base_table_name, table_schema)
                schema_name = os.getenv("databaseSchema")
                if not schema_name:
                    raise EnvironmentError("Missing required environment variable: 'databaseSchema'")
                jdbc_url, username, password = get_jdbc_url()
                write_to_postgres_spark(spark.createDataFrame([], create_spark_schema(load_stage_config_id)), load_stage_config_id)
                log_message("info", f"Table {table_name} created as fileKey is not present.")
                print(f"Table {table_name} created as fileKey is not present.")
               # return {"status": "skipped", "file": file_key, "reason": "Optional file not found, table created"}
                print(f"Optional file not found-skipping: {file_key}")
                return {"status": "skipped", "file": file_key, "reason": "Optional file not found"}
            else:
                raise FileNotFoundError(f"Required file not found: {file_key}")

        input_file_type = get_config().get(load_stage_config_id, {}).get("inputFileType", "").strip().lower()

        if input_file_type == "delimited":
            df = process_delimited_file_spark(load_stage_config_id, bucket_name, file_key)
          #  print("data single process",df)
            write_to_postgres_spark(df, load_stage_config_id)

        elif input_file_type == "json":
            error_msg = "JSON file processing is not yet implemented"
            log_message("failed", error_msg)
            raise NotImplementedError(error_msg)
        else:
            raise ValueError(f"Invalid input file type: {input_file_type}")

        log_message("success", f"Successfully processed file: {file_key}")
        return {"status": "success", "file": file_key}

    except Exception as e:
        message = f"Error processing file {file_config.get('fileKey', 'unknown')}: {e}"
        print(message)
        log_message("failed", message)
        return {"status": "error", "file": file_config.get('fileKey', 'unknown'), "error": str(e)}

def main():
    log_message("info", "Load Stage Glue Execution Started (Spark Mode).")
 #   print("main method")
 #   print("Command-line arguments:", sys.argv)
 #   print("Environment variables:", list(os.environ.keys()))
 #   print("Number of arguments:", len(sys.argv))
 #   print("Arguments list:")
 #   for i, arg in enumerate(sys.argv):
      #  print(f"  {i}: {arg}")
 #   print("All ENV vars printed")
    try:
        global inputFileName
        inputFileName = os.getenv('inputFileName')
        bucket_name = os.getenv('bucketName')

        if not bucket_name:
            raise ValueError("bucketName environment variable is required")

        load_config_from_s3(bucket_name, os.getenv("loadStageConfig"))

        files_to_process_env = os.getenv("filesToProcess", "").strip()

        if files_to_process_env:
            try:
                raw_files_data = json.loads(files_to_process_env)
                if isinstance(raw_files_data, str):
                    raw_files_data = json.loads(raw_files_data)

                files_to_process = []
                if isinstance(raw_files_data, list) and len(raw_files_data) > 0:
                    for item in raw_files_data:
                        if "isOptional" not in item:
                            item["isOptional"] = False
                        files_to_process.append(item)
                else:
                    files_to_process = raw_files_data

                if not isinstance(files_to_process, list):
                    raise ValueError(f"filesToProcess must be a JSON array")

                # Process files sequentially in Spark (Spark handles parallelism internally)
                results = []
                for file_config in files_to_process:
                    result = process_single_file(file_config, bucket_name)
                    results.append(result)

                successful = [r for r in results if r["status"] == "success"]
                failed = [r for r in results if r["status"] == "error"]
                skipped = [r for r in results if r["status"] == "skipped"]

                summary = f"Processing Summary - Successful: {len(successful)}, Failed: {len(failed)}, Skipped: {len(skipped)}, Total: {len(results)}"
                log_message("info", summary)
                print(summary)




                if failed:
                    print("\nFailed files:")
                    for fail in failed:
                        print(f"- {fail['file']}: {fail['error']}")
                    raise Exception(f"{len(failed)} file(s) failed to process")

            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in filesToProcess: {e}")

        log_message("success", "Load Stage Glue Execution Completed (Spark Mode).")

    except Exception as e:
        message = f"Error during load_stage: {e}"
        print(message)
        log_message("failed", message)
        raise Exception(message)

def set_job_params_as_env_vars():
    """Convert all --key value pairs from sys.argv to environment variables."""
    argv = sys.argv[1:]  # skip script name
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg.startswith('--'):
            key = arg[2:]
            # handle flag‑style args with no value
            value = ''
            if i + 1 < len(argv) and not argv[i + 1].startswith('--'):
                value = argv[i + 1]
                i += 1  # skip the value on next loop
            os.environ[key] = value
          #  print(f"Set env: {key} = {value}")
        i += 1

inputFileName = ''
if __name__ == '__main__':
    set_job_params_as_env_vars()

    # Initialize Glue job
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job.init(args['JOB_NAME'], args)

    try:
        main()
        job.commit()
    except Exception as e:
        message = f"Fatal error: {e}"
        log_message("failed", message)
        sys.exit(1)
