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
# Define proper schema for the returned values
from pyspark.sql.types import StructType, StructField, LongType

# Set Spark configurations before initializing the context
from pyspark.conf import SparkConf

conf = SparkConf()
conf.set("spark.sql.adaptive.enabled", "true")
conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Initialize Spark and Glue contexts with configurations
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:bundle_to_json:" + account_id

def log_message(status, message):
    splunk.log_message({'FileName':inputFileName,'Status': status, 'Message': message}, get_run_id())

def extract_account_number(filename):
    """Extract account number from the filename"""
    try:
        # Extract the base filename without path
        base_name = os.path.basename(filename)
        # Account number is the part before the first underscore
        account_number = base_name.split('_')[0]
        return account_number
    except Exception:
        # If extraction fails, return a default value
        return "unknown"


def split_json_content(content):
    """UDF to split JSON content and extract individual records"""
    try:
        # Split by the delimiter
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

def process_files_spark(input_bucket, input_path, output_bucket, output_path):
    try:
        input_path = f"s3a://{input_bucket}/{input_path}*"

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

        if batch_files == 0 and non_batch_file_count == 0:
            print("WARNING: No files to process.")
            log_message('warning', "No input files found.")
            return

        # Prepare non-batch (already individual JSON files)
        non_batch_prepared_df = non_batch_df.select(
            regexp_extract(col("filename"), r'([^/]+)$', 1).alias("output_filename"),
            col("value").alias("json_content")
        ).withColumn(
            "account_number", regexp_extract(col("output_filename"), r'^([^_]+)_', 1)
        )

        # Split batch files into individual JSON records
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

        # Get file counts per account
        file_counts_df = combined_df.select("account_number", "output_filename").distinct() \
            .groupBy("account_number") \
            .count() \
            .withColumnRenamed("count", "file_count") \
            .filter(col("account_number").isNotNull() & (col("account_number") != ""))

        # Join record counts with file counts
        account_stats_df = account_counts_df.join(file_counts_df, "account_number")

        account_counts = [(r["account_number"], r["count"], r["file_count"]) for r in account_stats_df.collect()]
        account_counts.sort(key=lambda x: x[1], reverse=True)

        folder_count = int(kafka_topic_count)
        print(f"Creating {folder_count} output folders for distribution")

        # Greedy assignment
        folder_loads = [0] * folder_count
        account_to_folder = {}
        account_folder_stats = {}  # To track accounts and their stats per folder

        for acct, record_cnt, file_cnt in account_counts:
            idx = folder_loads.index(builtins.min(folder_loads))
            folder_num = idx + 1
            account_to_folder[acct] = folder_num
            folder_loads[idx] += record_cnt

            # Store account stats by folder
            if folder_num not in account_folder_stats:
                account_folder_stats[folder_num] = []
            account_folder_stats[folder_num].append((acct, record_cnt, file_cnt))

        print(f"JSON records distribution across folders: {folder_loads}")

        # Print account distribution details
        print("\nAccount distribution by folder:")
        for folder_num, accounts in sorted(account_folder_stats.items()):
            print(f"\nFolder {folder_num} contains {len(accounts)} accounts:")
            for acct, record_cnt, file_cnt in sorted(accounts, key=lambda x: x[1], reverse=True):
                print(f"  - Account {acct}: {file_cnt} files, {record_cnt} records")

            folder_file_count = builtins.sum(file_cnt for _, _, file_cnt in accounts)
            folder_record_count = builtins.sum(record_cnt for _, record_cnt, _ in accounts)
            print(f"  Total: {folder_file_count} files, {folder_record_count} records")
            log_message('info', f"Folder {folder_num}: {folder_file_count} files, {folder_record_count} records across {len(accounts)} accounts")

                # Generate top accounts report with folder information
        print("\n=== GENERATING TOP ACCOUNTS REPORT WITH FOLDER ASSIGNMENTS ===")

        # Sort accounts by file count for the report (largest first)
        accounts_by_file_count = []
        for acct, record_cnt, file_cnt in account_counts:
            folder = account_to_folder.get(acct, "unknown")
            accounts_by_file_count.append((acct, record_cnt, file_cnt, folder))

        accounts_by_file_count.sort(key=lambda x: x[2], reverse=True)  # Sort by file count

        # Generate the report content
        import datetime
        report_content = "TOP ACCOUNTS REPORT WITH FOLDER ASSIGNMENTS\n"
        report_content += "=========================================\n\n"
        report_content += f"Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"

        # Add the main table
        report_content += "Rank | Account Number | File Count | Record Count | Assigned Folder\n"
        report_content += "---- | -------------- | ---------- | ------------ | --------------\n"

        for i, (acct, record_cnt, file_cnt, folder) in enumerate(accounts_by_file_count, 1):
            report_content += f"{i:4d} | {acct:14s} | {file_cnt:10,d} | {record_cnt:12,d} | {folder:14d}\n"

        # Add summary statistics
        total_files = builtins.sum(file_cnt for _, _, file_cnt, _ in accounts_by_file_count)
        total_records = builtins.sum(record_cnt for _, record_cnt, _, _ in accounts_by_file_count)
        total_accounts = len(accounts_by_file_count)

        report_content += f"\n\nSUMMARY\n"
        report_content += f"-------\n"
        report_content += f"Total accounts: {total_accounts:,d}\n"
        report_content += f"Total files: {total_files:,d}\n"
        report_content += f"Total records: {total_records:,d}\n"
        report_content += f"Total folders: {folder_count:,d}\n"

        # Add folder distribution section
        report_content += f"\n\nFOLDER DISTRIBUTION\n"
        report_content += f"-----------------\n"
        for i, load in enumerate(folder_loads):
            report_content += f"Folder {i+1}: {load:,d} records\n"

        # Save the report to S3
        report_file = f"{output_path.rstrip('/')}/top_accounts_with_folders_report.txt"
        try:
            import boto3
            print(f"Writing report to s3://{output_bucket}/{report_file}")
            s3_client = boto3.client('s3')
            s3_client.put_object(
                Bucket=output_bucket,
                Key=report_file,
                Body=report_content.encode('utf-8'),
                ContentType='text/plain'
            )
            print(f"✅ Report successfully written to s3://{output_bucket}/{report_file}")
            log_message('info', f"Top accounts report generated at s3://{output_bucket}/{report_file}")

            # Also write a CSV version
            csv_content = "rank,account_number,file_count,record_count,folder\n"
            for i, (acct, record_cnt, file_cnt, folder) in enumerate(accounts_by_file_count, 1):
                csv_content += f"{i},{acct},{file_cnt},{record_cnt},{folder}\n"

            csv_file = f"{output_path.rstrip('/')}/top_accounts_with_folders.csv"
            s3_client.put_object(
                Bucket=output_bucket,
                Key=csv_file,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv'
            )
            print(f"✅ CSV report also written to s3://{output_bucket}/{csv_file}")

            # Stop execution here as requested
            print("\n⛔ STOPPING EXECUTION AFTER REPORT GENERATION AS REQUESTED")
            print("File processing will be skipped.")
            return

        except Exception as e:
            error_msg = f"Error writing report: {str(e)}"
            print(f"ERROR: {error_msg}")
            log_message('warning', error_msg)
            print("Continuing with processing...")

        # Only continue if report generation fails
        print("Continuing with file processing...")

        account_to_folder_bc = sc.broadcast(account_to_folder)

        def process_partition(iterator):
            config = Config(
                retries={'max_attempts': 15, 'mode': 'adaptive'},
                max_pool_connections=50
            )
            s3_client = boto3.client('s3', config=config)

            success_count = 0
            error_count = 0
            files_by_name = {}

            for row in iterator:
                fname = row.output_filename
                files_by_name.setdefault(fname, []).append(row.json_content)

            base_path = output_path.rstrip('/')
            last_folder = base_path.split('/')[-1]
            parent_path = base_path[:-len(last_folder)] if base_path.endswith(last_folder) else f"{base_path}/"
            mapping = account_to_folder_bc.value

            for filename, contents in files_by_name.items():
                try:
                    acct = filename.split('_', 1)[0]
                    folder_num = mapping.get(acct, 1)
                    numbered_path = f"{parent_path}{last_folder}{folder_num}"
                    key = f"{numbered_path}/{filename}"
                    s3_client.put_object(
                        Bucket=output_bucket,
                        Key=key,
                        Body='\n'.join(contents).encode('utf-8'),
                        ContentType='application/json'
                    )
                    success_count += len(contents)
                except Exception as e:
                    error_count += len(contents)
                    error_message = f"Error writing {filename}: {str(e)}"
                    print(error_message)
                    log_message('error', error_message)
            return [(success_count, error_count)]

        # Repartition for parallelism
        combined_write_df = combined_df.select("output_filename", "json_content").repartition(800)

        stats_schema = StructType([
            StructField("success_count", LongType(), False),
            StructField("error_count", LongType(), False)
        ])

        stats_df = combined_write_df.rdd.mapPartitions(process_partition).toDF(stats_schema)
        results = stats_df.agg(
            sum("success_count").alias("success_count"),
            sum("error_count").alias("error_count")
        ).collect()[0]

        success_count = results.success_count
        error_count = results.error_count
        total = success_count + error_count
        print(f"Successfully wrote {success_count} of {total} JSON records")
        log_message('success', f"Processed {success_count} records successfully, {error_count} errors")
                # Add summary of account distribution
        summary = "Account distribution summary:\n"
        for folder_num, accounts in sorted(account_folder_stats.items()):
            folder_file_count = builtins.sum(file_cnt for _, _, file_cnt in accounts)
            folder_record_count = builtins.sum(record_cnt for _, record_cnt, _ in accounts)
            summary += f"Folder {folder_num}: {folder_file_count} files, {folder_record_count} records across {len(accounts)} accounts\n"
        log_message('info', summary)

    except Exception as e:
        error_message = f"Error: {str(e)}"
        print(f"PROCESSING ERROR: {error_message}")
        print(f"Stack trace: {traceback.format_exc()}")
        log_message('failed', error_message)
        raise

def process_with_streaming(input_bucket, input_path, output_bucket, output_path):
    """Alternative streaming approach for very large datasets"""
    try:
        # Create streaming DataFrame
        streaming_df = spark.readStream \
            .format("text") \
            .option("wholetext", "true") \
            .load(f"s3a://{input_bucket}/{input_path}")

        # Filter for files whose name starts with BATCH
        # First add the filename column
        streaming_df = streaming_df.withColumn("filename", input_file_name())

        # Log for debugging
        print("Filtering for files starting with BATCH prefix")

        # Apply filter for BATCH files
        streaming_df = streaming_df.filter(col("filename").rlike("/BATCH[^/]*$"))

        # Process and write stream
        query = streaming_df \
            .select(explode(split_udf(col("value"))).alias("split_data")) \
            .select(
                col("split_data.filename").alias("filename"),
                col("split_data.content").alias("content")
            ) \
            .writeStream \
            .format("json") \
            .option("path", f"s3a://{output_bucket}/{output_path}/") \
            .option("checkpointLocation", f"s3a://{output_bucket}/{output_path}/checkpoints/") \
            .partitionBy("filename") \
            .start()

        query.awaitTermination()

    except Exception as e:
        error_message = f"Streaming error: {str(e)}"
        print(f"STREAMING ERROR: {error_message}")
        print(f"Stack trace: {traceback.format_exc()}")
        splunk.log_message({'Status': 'failed', 'Message': error_message, 'Stack': traceback.format_exc()}, get_run_id())
        raise

# Set environment variables from job arguments
def set_job_params_as_env_vars():
    i = 1
    while i < len(sys.argv):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            # Check if next argument exists and is not another key
            if i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith('--'):
                value = sys.argv[i + 1]
                os.environ[key] = value
                i += 2
            else:
                # No value for this key, skip to next
                i += 1
        else:
            # Stray value, skip it
            i += 1

def main():
    try:
        set_job_params_as_env_vars()
        global inputFileName
        global kafka_topic_count
        inputFileName = os.getenv('inputFileName')
        input_bucket = os.getenv('input_bucket')
        output_bucket = os.getenv('output_bucket')
        input_path = os.getenv('input_path')
        output_path = os.getenv('output_path')
        kafka_topic_count = os.getenv('kafka_topic_count', '6')
        print(f"Job parameters retrieved:")
        print(f"  - Input bucket: {input_bucket}")
        print(f"  - Input prefix: {input_path}")
        print(f"  - Output bucket: {output_bucket}")
        print(f"  - Output prefix: {output_path}")
        print(f"  - Kafka topic count: {kafka_topic_count}")

        print(f"Starting processing of files with prefix 'BATCH' from {input_bucket}/{input_path}")

        # Use batch processing for better performance with large files
        process_files_spark(input_bucket, input_path, output_bucket, output_path)

        #job.commit()
    except Exception as e:
        error_message = f"Main function error: {str(e)}"
        print(f"MAIN ERROR: {error_message}")
        print(f"Stack trace: {traceback.format_exc()}")
        log_message('failed', error_message)
        raise

if __name__ == "__main__":
    main()


