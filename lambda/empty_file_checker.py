import boto3
import json
import re
from dpm_splunk_logger_py import splunk
import traceback 

s3 = boto3.client("s3")

def log_message(status, message, context=None, event=None):
    """Function to log messages in splunk with enhanced logging"""
    
    try:
        input_file_name = event.get('inputFileName', '') if event else os.getenv('inputFileName', '')
        property_config_name = event.get('propertyConfigName', '') if event else os.getenv('propertyConfigName', '')

        log_data = {
            'Status': status,
            'Message': str(message)
        }

        if input_file_name:
            log_data['FileName'] = input_file_name
        if property_config_name:
            log_data['PropertyConfigName'] = property_config_name

        print(f"INFO: Logging message - Status: {status}, Message: {message}")
        print(f"INFO: Splunk log data: {log_data}")

        if context and hasattr(context, 'invoked_function_arn'):
            splunk.log_message(log_data, context)
        else:
            print("INFO: Skipping Splunk logging - no valid context provided")

        print("INFO: Successfully logged message to Splunk")

    except Exception as e:
        print(f"ERROR: Failed to log message to Splunk: {str(e)}")
        print(f"ERROR: Traceback: {traceback.format_exc()}")


def find_matching_ascii_file(bucket, path_prefix, regex_pattern):

    compiled_regex = re.compile(regex_pattern)

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=path_prefix)

    for page in pages:
        for obj in page.get("Contents", []):
            filename = obj["Key"].split("/")[-1]
            if compiled_regex.match(filename):
                log_message("INFO", f"Found match: {filename}")
                return obj["Key"]

    return None


def check_if_file_empty(bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read().decode("utf-8", errors="ignore")

        # Remove whitespace
        clean_body = body.strip()

        if clean_body == "":
            return True  # Empty
        else:
            return False

    except Exception as e:
        log_message("INFO", f"Error reading file: {e}")
        return False


def lambda_handler(event, context):
    
    log_message("INFO", "Empty file check Lambda execution started", context, event)


    file_key = event.get("file_key", "") 
    bucket = event.get("bucket")
    file_path = event.get("file_path", "")
    file_pattern = event.get("file_pattern", "")

    log_message("INFO", f"file_path: {file_path}", context, event)
    log_message("INFO", f"file_pattern: {file_pattern}", context, event)
    
    if file_path == " " and file_pattern == " ":

        log_message("INFO", f"file_path & file_pattern are blank → fallback to file_key")

        if not file_key or file_key == " ":
            raise ValueError("file_key must be provided when ascii_file_path and ascii_file_pattern are blank.")

        matched_file_key = file_key   # use incoming key directly

    else:
        
        matched_file_key = find_matching_ascii_file(bucket, file_path, file_pattern)

        if not matched_file_key:
            log_message("INFO", f"No matching file found → Treating as NON-EMPTY")
            raise Exception("No matching file found for the configured ASCII pattern")

        print(f"Matched ASCII file: {matched_file_key}")

    # Check file contents
    is_empty = check_if_file_empty(bucket, matched_file_key)

    if is_empty:
        log_message("INFO", f"File content empty → Returning empty_check=True", context, event)
        return {
            "is_file_empty": True,
            "file_name": matched_file_key
        }
    else:
        log_message("INFO", f"File has content — return False", context, event)
        return {"is_file_empty": False,"file_name": matched_file_key}
