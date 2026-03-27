import sys
import json
import traceback
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import boto3
import splunk
import os
import builtins
from botocore.config import Config
from pyspark.sql.types import StructType, StructField, LongType

# Set Spark configurations before initializing the context
from pyspark.conf import SparkConf

conf = SparkConf()
conf.set("spark.sql.adaptive.enabled", "true")
conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:bundle_to_json:" + account_id

def log_message(status, message):
    splunk.log_message({'FileName':inputFileName,'Status': status, 'Message': message}, get_run_id())

def get_aws_parameter(parameter_name, region):
    """
    Retrieve a parameter value from AWS Systems Manager Parameter Store.
    """
    ssm_client = boto3.client('ssm', region_name=region)
    response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
    return response['Parameter']['Value']

########code for data masking .###########
###### it will mask the fields mentioned in mask_data.txt file present in s3 bucket for COMBO files(specially 2010).##########
#####created for masking PII data before sending to kafka.###########

def load_mask_fields(bucket, region,mask_data_file):
    """Load fields to mask from S3 config file"""
    try:
        s3_client = boto3.client('s3', region_name=region)
        s3_key = mask_data_file
        response = s3_client.get_object(Bucket=bucket, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        fields_to_mask = [line.strip() for line in content.split('\n') if line.strip()]
        print(f"Fields to mask loaded from s3://{bucket}/{s3_key}: {fields_to_mask}")
        log_message('info', f"Fields to mask: {fields_to_mask}")
        return fields_to_mask
    except Exception as e:
        error_msg = f"Error loading mask fields from s3://{bucket}/{s3_key}: {str(e)}"
        print(error_msg)
        log_message('warning', error_msg)
        return []

def mask_field_value(value):
    """Mask a field value by replacing with asterisks, keeping first 2 and last 2 chars"""
    if not value:
        return value
    if len(value) <= 4:
        return '*' * len(value)
    return value[:2] + '*' * (len(value) - 4) + value[-2:]

def mask_json_data(json_str, fields_to_mask):
    """Mask specified fields in JSON content with support for array index notation"""
    try:
        data = json.loads(json_str)
        
        def mask_recursive(obj, fields):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    # Check for exact field match
                    if key in fields:
                        if isinstance(value, str):
                            obj[key] = mask_field_value(value)
                        elif isinstance(value, (int, float)):
                            obj[key] = mask_field_value(str(value))
                        elif isinstance(value, (dict, list)):
                            mask_recursive(value, fields)
                    else:
                        # Check for array index notation like "20103-NAP-LINES[0].20103-NAP-LINE"
                        for field in fields:
                            if '[0]' in field:
                                parent_field, rest = field.split('[0].', 1)
                                if key == parent_field and isinstance(value, list) and len(value) > 0:
                                    if isinstance(value[0], dict) and rest in value[0]:
                                        if isinstance(value[0][rest], str):
                                            value[0][rest] = mask_field_value(value[0][rest])
                                        elif isinstance(value[0][rest], (int, float)):
                                            value[0][rest] = mask_field_value(str(value[0][rest]))
                        
                        # Continue recursive masking
                        if isinstance(value, (dict, list)):
                            mask_recursive(value, fields)
                            
            elif isinstance(obj, list):
                for item in obj:
                    mask_recursive(item, fields)
        
        mask_recursive(data, fields_to_mask)
        return json.dumps(data)
    except Exception as e:
        print(f"Error masking JSON: {str(e)}")
        return json_str


def extract_account_number(filename):
    """Extract account number from the filename"""
    try:
        base_name = os.path.basename(filename)
        account_number = base_name.split('_')[0]
        return account_number
    except Exception as e:
        error_message = f"Failed to extract account number from {filename}: {str(e)}"
        log_message('failed', error_message)
        raise Exception(f"Account number extraction failed: {error_message}")

def split_json_content(content):
    """UDF to split JSON content and extract individual records"""
    try:
        split_objects = content.split('{"B228_FILE":{')
        results = []

        for i, json_obj in enumerate(split_objects[1:], 1):
            json_obj = '{"B228_FILE":{' + json_obj
            try:
                parsed = json.loads(json_obj)
                filename = parsed.get("B228_FILE", {}).get("FILENAME")
                if filename:
                    results.append((filename, json.dumps(parsed)))
            except:
                continue
        return results
    except:
        return []

# Register UDF
split_udf = udf(split_json_content, ArrayType(StructType([
    StructField("filename", StringType(), True),
    StructField("content", StringType(), True)
])))

def s3_prefix_has_objects(bucket, prefix, region):
    s3 = boto3.client("s3", region_name=region)
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, MaxKeys=1):
        if page.get("KeyCount", 0) > 0:
            return True
    return False

def process_files_spark(input_bucket, input_path, kafka_brokers, kafka_topic_prefix, kafka_topic_count, 
                       batch_size, region, kafka_username, kafka_password, kafka_jaas_config, 
                       data_masking_enable,mask_data_file):
    try:
        # Check if data masking is enabled
        masking_enabled = data_masking_enable.lower() == 'true'
        fields_to_mask = []
        
        if masking_enabled:
            print("Data masking is ENABLED")
            log_message('info', "Data masking is ENABLED")
            fields_to_mask = load_mask_fields(input_bucket, region,mask_data_file)
            if not fields_to_mask:
                print("WARNING: Data masking enabled but no fields found in mask_data.txt")
                log_message('warning', "Data masking enabled but no fields found in mask_data.txt")
        else:
            print("Data masking is DISABLED")
            log_message('info', "Data masking is DISABLED")
        
        # Get Kafka credentials
        sasl_plain_username = get_aws_parameter(kafka_username, region)
        sasl_plain_password = get_aws_parameter(kafka_password, region)
        sasl_jaas_config = get_aws_parameter(kafka_jaas_config, region)

        input_path = f"s3a://{input_bucket}/{input_path}*"
        s3_prefix = input_path.replace("s3a://", "", 1).split("/", 1)[1].rstrip("*")
        if not s3_prefix_has_objects(input_bucket, s3_prefix, region):
            msg = f"Error:No objects found at s3://{input_bucket}/{s3_prefix}. failed and Exiting gracefully."
            print(msg)
            log_message('error', msg)
            return        

        df = spark.read.text(input_path, wholetext=True).withColumn("filename", input_file_name())

        total_files = df.select("filename").distinct().count()
        print(f"Total objects found in S3 path: {total_files}")
        log_message('info', f"Total objects found in S3 path: {total_files}")

        # Split into batch and non-batch sources
        batch_df = df.filter(col("filename").rlike("/BATCH[^/]*$"))
        non_batch_df = df.filter(~col("filename").rlike("/BATCH[^/]*$"))

        non_batch_file_count = non_batch_df.select("filename").distinct().count()
        print(f"Non-BATCH individual JSON files: {non_batch_file_count}")
        log_message('info', f"Non-BATCH individual JSON files: {non_batch_file_count}")

        batch_files = batch_df.select("filename").distinct().count()
        print(f"BATCH container files: {batch_files}")
        log_message('info', f"BATCH container files: {batch_files}")

        if batch_files == 0 and non_batch_file_count == 0:
            print("WARNING: No files to process.")
            log_message('warning', "No input files found.")
            return

        # Prepare non-batch files with conditional masking
        if masking_enabled and fields_to_mask:
            mask_udf = udf(lambda json_str: mask_json_data(json_str, fields_to_mask), StringType())
            non_batch_prepared_df = non_batch_df.select(
                regexp_extract(col("filename"), r'([^/]+)$', 1).alias("output_filename"),
                mask_udf(col("value")).alias("json_content")
            ).withColumn(
                "account_number", regexp_extract(col("output_filename"), r'^([^_]+)_', 1)
            )
        else:
            non_batch_prepared_df = non_batch_df.select(
                regexp_extract(col("filename"), r'([^/]+)$', 1).alias("output_filename"),
                col("value").alias("json_content")
            ).withColumn(
                "account_number", regexp_extract(col("output_filename"), r'^([^_]+)_', 1)
            )

        # Split batch files into individual JSON records (no masking for batch files)
        split_df = batch_df.select(
            explode(split_udf(col("value"))).alias("split_data")
        ).select(
            col("split_data.filename").alias("output_filename"),
            col("split_data.content").alias("json_content")
        ).withColumn(
            "account_number", regexp_extract(col("output_filename"), r'^([^_]+)_', 1)
        )

        # Combine both sources
        combined_df = split_df.unionByName(non_batch_prepared_df)

        total_json_records = combined_df.count()
        print(f"Total individual JSON records (split + non-batch): {total_json_records}")
        log_message('info', f"Total individual JSON records: {total_json_records}")

        # Account counts based on all JSON records
        account_counts_df = combined_df.groupBy("account_number") \
            .count() \
            .filter(col("account_number").isNotNull() & (col("account_number") != "")) \
            .orderBy(desc("count"))

        account_counts = [(r["account_number"], r["count"]) for r in account_counts_df.collect()]

        topic_count = int(kafka_topic_count)
        print(f"Creating {topic_count} Kafka topics for distribution")
        log_message('info', f"Creating {topic_count} Kafka topics for distribution")

        # Greedy assignment
        topic_loads = [0] * topic_count
        account_to_topic = {}
        for acct, cnt in account_counts:
            idx = topic_loads.index(builtins.min(topic_loads))
            account_to_topic[acct] = idx + 1
            topic_loads[idx] += cnt
        print(f"JSON records distribution across topics: {topic_loads}")
        log_message('info', f"JSON records distribution across topics: {topic_loads}")

        # Create a topic mapping column
        def get_topic_number(acct):
            if topic_count == 1:
                return 0
            return account_to_topic.get(acct, 1)

        get_topic_udf = udf(get_topic_number, IntegerType())

        # Add topic column to the dataframe
        result_df = combined_df.withColumn("topic_num", get_topic_udf(col("account_number")))

        # Calculate topic name for each record
        result_df = result_df.withColumn(
            "topic",
            when(col("topic_num") == 0, lit(kafka_topic_prefix))
            .otherwise(concat(lit(kafka_topic_prefix), col("topic_num")))
        )

        # Prepare the DataFrame for Kafka - select only needed columns and rename
        kafka_df = result_df.select(
            col("topic"),
            col("json_content").alias("value")
        )

        # Repartition for better parallelism with large data
        kafka_df = kafka_df.repartition(180)

        # Set Kafka properties
        kafka_options = {
            "kafka.bootstrap.servers": kafka_brokers,
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "SCRAM-SHA-512",
            "kafka.sasl.jaas.config": sasl_jaas_config,
            "kafka.batch.size": str(int(batch_size) * 32),
            "kafka.linger.ms": "10",
            "kafka.buffer.memory": "134217728",
            "kafka.acks": "1",
            "kafka.retries": "3",
            "kafka.compression.type": "snappy",
            "kafka.retry.backoff.ms": "100",
        }

        # Write to Kafka using native Spark Kafka connector
        print("Starting to send messages to Kafka using native Spark connector")
        log_message('info', "Starting to send messages to Kafka using native Spark connector")
        kafka_df.write \
            .format("kafka") \
            .options(**kafka_options) \
            .save()

        print(f"Successfully sent approximately {total_json_records} JSON records to Kafka")
        log_message('success', f"Successfully sent approximately {total_json_records} JSON records to Kafka")

    except Exception as e:
        error_message = f"Error in method process_files_spark: {str(e)}"
        print(f"PROCESSING ERROR: {error_message}")
        print(f"Stack trace: {traceback.format_exc()}")
        log_message('failed', error_message)
        raise

def main():
    try:
        set_job_params_as_env_vars()
        global inputFileName
        global kafka_topic_count
        inputFileName = os.getenv('inputFileName')
        input_bucket = os.getenv('input_bucket')
        input_path = os.getenv('input_path')
        kafka_topic_count = os.getenv('kafka_topic_count', '1')
        kafka_topic_prefix = os.getenv('kafka_topic_prefix')
        batch_size = os.getenv('batch_size', '5000')
        region = os.getenv('region', 'us-east-1')
        kafka_brokers = os.getenv('kafka_brokers')
        kafka_username = os.getenv('kafka_username')
        kafka_password = os.getenv('kafka_password')
        kafka_jaas_config = os.getenv('kafka_jaas_config')
        data_masking_enable = os.getenv('data_masking_enable', 'false')
        mask_data_file=os.getenv('mask_data_file')

        print(f"Job parameters retrieved:")
        print(f"  - Input bucket: {input_bucket}")
        print(f"  - Input prefix: {input_path}")
        print(f"  - Kafka topic count: {kafka_topic_count}")
        print(f"  - Kafka brokers: {kafka_brokers}")
        print(f"  - Kafka topic prefix: {kafka_topic_prefix}")
        print(f"  - Batch size: {batch_size}")
        print(f"  - AWS Region: {region}")
        print(f"  - kafka_username: {kafka_username}")
        print(f"  - kafka_password: {kafka_password}")
        print(f"  - kafka_jaas_config: {kafka_jaas_config}")
        print(f"  - data_masking_enable: {data_masking_enable}")
        print(f"  - Mask data file: s3://{input_bucket}/{mask_data_file}")

        print(f"Starting processing of files with prefix 'BATCH' from {input_bucket}/{input_path}")
        log_message('info', f"Job parameters retrieved: Input bucket: {input_bucket}, Input prefix: {input_path}, Data masking: {data_masking_enable}")

        # Process files and send to Kafka using native connector
        process_files_spark(input_bucket, input_path, kafka_brokers, kafka_topic_prefix,
                           kafka_topic_count, batch_size, region, kafka_username, kafka_password, 
                           kafka_jaas_config, data_masking_enable,mask_data_file)

    except Exception as e:
        error_message = f"Main function error: {str(e)}"
        print(f"MAIN ERROR: {error_message}")
        print(f"Stack trace: {traceback.format_exc()}")
        log_message('failed', error_message)
        raise

# Set environment variables from job arguments
def set_job_params_as_env_vars():
    i = 1
    while i < len(sys.argv):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            if i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith('--'):
                value = sys.argv[i + 1]
                os.environ[key] = value
                i += 2
            else:
                i += 1
        else:
            i += 1

if __name__ == "__main__":
    main()