import os
import json
import re
import boto3
from dpm_splunk_logger_py import splunk
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

# Initialize AWS clients
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

# Constants for failure handling
FAILED_DIRECTORY = 'citizen/failed'

def lambda_handler(event, context):
    splunk.log_message({'Message': "Lambda triggered successfully"}, context)
    # Extract bucket and key from event
    detail = event.get('detail', {})
    bucket = detail.get('bucket', {}).get('name')
    key = detail.get('object', {}).get('key')
    #print("bucket", bucket)
    #print("key", key)
    if not bucket or not key:
        error_message="Bucket or key not provided in the event"
        splunk.log_message({'Message': error_message,  "Status": "failed"}, context)
        raise ValueError("Bucket or key not provided in the event")

    try:
        # Fetch cron_config from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        cron_config = json.loads(response['Body'].read().decode('utf-8'))
    except ClientError as e:
        error_message=f"Error fetching cron_config from S3: {e}"
        splunk.log_message({'Message': error_message,  "Status": "failed"}, context)
        raise Exception(error_message)
    except json.JSONDecodeError as e:
        error_message=f"Error decoding JSON from cron_config: {e}"
        splunk.log_message({'Message': error_message,  "Status": "failed"}, context)
        raise Exception(error_message)
    except Exception as e:
        error_message="f{e}"
        splunk.log_message({'Message': error_message,  "Status": "failed"}, context)
        raise e

    # Extract cronPayload
    cron_payload = cron_config.get("cronPayload", [])
    if not cron_payload:
        raise ValueError("cronPayload is missing or empty in the cron_config")

    # Process each entry in the cron_payload
    for item in cron_payload:
        details = item.get("detail", {})
        objects = details.get("object", [{}])
        for obj in objects:
            done_file_path = obj.get("done_file_path")
            report_file_path = obj.get("report_file_path")
            report_file_regex = obj.get("report_file_regex")
            done_file_regex = obj.get("done_file_regex")
            lambda_to_call = obj.get("lambda_to_call")

            # Validate and process done files
            process_done_files(event, done_file_path, report_file_path, report_file_regex, done_file_regex, lambda_to_call,context)

    # splunk.log_message(
    #             {'Message': "Lambda triggered successfully", "Status": "success"}, context)
    return {"status": "Processing Completed"}

def process_done_files(event, done_file_path, report_file_path, report_file_regex, done_file_regex, lambda_to_call,context):
    """Validate done files and trigger Lambda if all report files are present."""
    # Extract bucket and prefix
    bucket = event.get('detail', {}).get('bucket', {}).get('name')

    if not bucket:
        raise ValueError("Bucket name not found in the event")

    # Compile regex patterns
    report_file_pattern = re.compile(report_file_regex)
    done_file_pattern = re.compile(done_file_regex)

    # Fetch matching done files
    all_done_files = get_s3_file_list(bucket, done_file_path, ".done.json")
    for done_file in all_done_files:
        if done_file_pattern.match(os.path.basename(done_file)):
            #print(f"Processing done file: {done_file}")
            validate_and_trigger_lambda(bucket, done_file, report_file_path, report_file_pattern, lambda_to_call,context)

def get_s3_file_list(bucket, prefix, suffix):
    """Get a list of files in an S3 prefix with a specific suffix."""
    file_list = []
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith(suffix):
                file_list.append(obj['Key'])
    return file_list

def validate_and_trigger_lambda(bucket, done_file_key, report_file_path, report_file_pattern, lambda_to_call,context):
    """Validate report files and trigger the associated Lambda."""
    try:
        # Fetch and parse the done file
        response = s3.get_object(Bucket=bucket, Key=done_file_key)
        done_data = json.loads(response['Body'].read().decode('utf-8'))

        # Get the list of report files from the specified path
        all_report_files = get_s3_file_list(bucket, report_file_path, ".rpt")
        #print(f"All report files found in path '{report_file_path}': {all_report_files}")

        # Extract just the file names from the keys for easier comparison
        all_report_file_names = [os.path.basename(file) for file in all_report_files]
        #print(f"Report file names extracted: {all_report_file_names}")

        missing_files_tracker = {}  # Tracker for missing files

        for record_key, records in done_data.items():
            missing_files = []
            for record in records:
                # Extract reportFile from each record
                report_file = record.get("reportFile")
                if not report_file:
                    #print(f"Missing 'reportFile' in record: {record}")
                    continue

                #print(f"Checking presence of report file: {report_file}")

                # Validate the presence of the report file
                if report_file not in all_report_file_names:
                    #print(f"Report file missing: {report_file}")
                    missing_files.append(report_file)
                else:
                    pass    # No action needed
                    #print(f"Report file found: {report_file}")

            if missing_files:
                # Add '_missing_files' to the done_file_key for the tracker
                missing_files_tracker[f"{done_file_key}_missing_files"] = missing_files

        if missing_files_tracker:
            #print(f"Missing files for {done_file_key}: {missing_files_tracker}")
            handle_missing_files(bucket, done_file_key, missing_files_tracker)
        else:
            #print(f"All report files are present for {done_file_key}. Triggering Lambda...")
            payload = {"done_file": done_file_key}
            trigger_lambda(lambda_to_call, payload,context)

    except ClientError as e:
        error_message=f"Error fetching or processing done file {done_file_key}: {e}"
        raise Exception(error_message)
    except json.JSONDecodeError as e:
        error_message=f"Error decoding JSON from done file {done_file_key}: {e}"
        raise Exception(error_message)
    except Exception as e:
        raise e

def handle_missing_files(bucket, done_file_key, missing_files_tracker):
    """Handle missing files by creating a tracker and moving the done file if necessary."""
    # Extract the directory path from the done file key
    done_file_dir = os.path.dirname(done_file_key)
    missing_files_key = f"{os.path.splitext(os.path.basename(done_file_key))[0]}.missing_report.json"

    try:
        # Save missing files tracker to the same directory as the done file
        missing_files_tracker_content = {os.path.basename(done_file_key): missing_files_tracker.get(f"{done_file_key}_missing_files", [])}
        missing_files_path = f"{done_file_dir}/{missing_files_key}"
        
        s3.put_object(Bucket=bucket, Key=missing_files_path, Body=json.dumps(missing_files_tracker_content))
        #print(f"Missing files tracker saved to: {missing_files_path}")

        # Check if the missing report file has been there for more than 24 hours
        check_for_done_file_24hrs(bucket, missing_files_path, done_file_key)

    except ClientError as e:
        error_message=f"Error saving missing files tracker: {e}"
        raise Exception(error_message)
    except ClientError as e:
        error_message=f"Error saving missing files tracker: {e}"
        raise Exception(error_message)
    except Exception as e:
        raise e

def check_for_done_file_24hrs(bucket,missing_files_path, done_file_key):
    """Check if the done file was modified more than 24 hours ago."""
    try:
        # Get the metadata of the done file
        response = s3.head_object(Bucket=bucket, Key=done_file_key)
        last_modified = response['LastModified']
        
        # Ensure that current time is in the same timezone as last_modified
        current_time = datetime.now(last_modified.tzinfo)

        # Check if 24 hrs have passed since the done file was last modified
        expiry_time = last_modified + timedelta(hours=24)

        if current_time > expiry_time:
            #print(f"24 hours passed for {done_file_key}. Moving to failed directory.")
            move_to_failed_directory(bucket, done_file_key, missing_files_path)

    except ClientError as e:
        error_message=f"Error checking done file timestamp for {done_file_key}: {e}"
        raise Exception(error_message)
    except Exception as e:
        raise e

def move_to_failed_directory(bucket, done_file_key, missing_files_key=None):
    """Move the done file and missing files tracker to the failed directory."""
    try:
        # Move the done file to the failed directory
        failed_done_file_key = f"{FAILED_DIRECTORY}/{os.path.basename(done_file_key)}"
        s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': done_file_key}, Key=failed_done_file_key)
        s3.delete_object(Bucket=bucket, Key=done_file_key)

        if missing_files_key:
            #print("missing_files_key",missing_files_key)
            # Move the missing files tracker to the failed directory (same as done file)
            failed_missing_files_key = f"{FAILED_DIRECTORY}/{os.path.basename(missing_files_key)}"
            #print("os.path.basename(missing_files_key",os.path.basename(missing_files_key))
            #print("failed_missing_files_key",failed_missing_files_key)
            s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': missing_files_key}, Key=failed_missing_files_key)
            s3.delete_object(Bucket=bucket, Key=missing_files_key)

        #print(f"Moved {done_file_key} and {missing_files_key if missing_files_key else ''} to failed directory.")

    except ClientError as e:
        error_message=f"Error moving files to failed directory: {e}"
        raise Exception(error_message)
    except Exception as e:
        raise e

def trigger_lambda(lambda_to_call, payload,context):
    """Invoke the specified Lambda function."""
    try:
        response = lambda_client.invoke(
            FunctionName=lambda_to_call,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        response_payload = response['Payload'].read().decode('utf-8')
        splunk.log_message({'Message': f"Lambda {lambda_to_call} triggered successfully. Response: {response_payload}"}, context)
        #print(f"Lambda {lambda_to_call} triggered successfully. Response: {response_payload}")


    except ClientError as e:
        error_message=f"Error triggering Lambda: {e}"
        raise Exception(error_message)
    except Exception as e:
        #print(f"Unexpected error: {e}")
        raise e
