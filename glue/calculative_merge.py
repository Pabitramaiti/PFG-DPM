import boto3
import json
import re
from collections import defaultdict
import os
import splunk
import sys
from datetime import datetime, timezone

file_date = datetime.now().strftime("%m%d%Y")
now = datetime.now()
TS_STRING = now.strftime("%H%M%S")

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:dpmdev-di-glue-calculative_merges : " + account_id

def log_message(status, message):
    splunk.log_message({'FileName': inputFileName, 'Status': status, 'Message': message}, get_run_id())

def load_config_from_s3(s3_client, bucket, key):
    try:
        """Load JSON config from S3."""
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except Exception as e:
        log_message('failed', f"Error loading config from S3: {e}")
        raise ValueError(f"Error loading config from S3: {e}")

def get_matching_files(s3_client, bucket, input_path, input_regex):
    try:
        """Fetch all files matching the regex in the given S3 path."""
        matching_files = []
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=input_path)
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if re.match(input_regex, obj['Key'].split('/')[-1]):  # Match filename
                        matching_files.append(obj['Key'])
        
        return matching_files
    except Exception as e:
        log_message('failed', f"Error fetching matching files: {e}")
        raise ValueError(f"Error fetching matching files: {e}")

def load_json_from_s3(s3_client, bucket, key):
    try:
        """Load JSON content from an S3 file."""
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except Exception as e:
        log_message('failed', f"Error loading JSON from S3: {e}")
        raise ValueError(f"Error loading JSON from S3: {e}")

def get_value(data, key_path):
    """Access a nested value in a dictionary based on a dot-separated key path."""
    keys = key_path.split('.')
    for key in keys:
        print(f"key : {key}")
        # Only use get if data is a dictionary
        if isinstance(data, dict):
            data = data.get(key, {})
        else:
            # If the data is not a dictionary, it should be the final value
            return data
    return data

def set_value(data, key_path, value):
    """Set a nested value in a dictionary based on a dot-separated key path."""
    keys = key_path.split('.')
    for key in keys[:-1]:
        # Ensure the current level is a dictionary, create it if necessary
        if not isinstance(data, dict):
            data = {}
        data = data.setdefault(key, {})
    data[keys[-1]] = value

def app_value(data, key_path, value):
    """Set a nested value in a dictionary based on a dot-separated key path."""
    keys = key_path.split('.')

    if (len(keys)==1 and keys[0]=='' ):
        data.extend(value)
    else:
        for key in keys[:-1]:
            print(1)
            # Ensure the current level is a dictionary, create it if necessary
            if not isinstance(data, dict):
                print(2)
                data = {}
            print(3)
            data = data.setdefault(key, {})
        data[keys[-1]].extend(value)
        print(4)

def merge_json_files(s3_client, files, bucket, config):
    try:
        """Merge JSON files based on configured rules."""
        # merged_data = defaultdict(dict)
        
        # Load the first file as the base
        first_file = load_json_from_s3(s3_client, bucket, files[0])
        merged_data=(first_file)
    
        # Process additional files
        if 'corp_breakout' in config:
            if "corp_breakout_regex" in config['corp_breakout']:
                corp_breakout_regex = re.compile(config['corp_breakout']["corp_breakout_regex"])  # Dynamic corp_breakout key pattern
        
        for file in files[1:]:
            data = load_json_from_s3(s3_client, bucket, file)
            
            # Sum up numeric fields
            if 'sum_fields' in config:
                print("sum_fields")
                for field in config['sum_fields']:
                    value1 = get_value(merged_data, field)
                    value2 = get_value(data, field)
                    
                    # If both values are numeric, sum them
                    if isinstance(value1, (int, float)) and isinstance(value2, (int, float)):
                        sum_value = value1 + value2
                    elif isinstance(value1, (int, float)):
                        sum_value = value1  # retain the value from d1 if they are not numeric
                    elif isinstance(value2, (int, float)):
                        sum_value = value2  # retain the value from d1 if they are not numeric
                        
                    set_value(merged_data, field, sum_value)
            
            # Handle dynamic corp_breakout fields
            if 'corp_breakout' in data:
                print("corp_breakout")
                for key, values in data['corp_breakout'].items():
                    if corp_breakout_regex.match(key):
                        if key not in merged_data['corp_breakout']:
                            merged_data['corp_breakout'][key] = defaultdict(int)
                        for subfield, value in values.items():
                            merged_data['corp_breakout'][key][subfield] += value
            
            # Append lists
            if 'append_fields' in config:
                print("append_fields")
                for field in config['append_fields']:
                    value1 = get_value(data, field)
                    # print(f"value to be appended field : *{field}* :: value1 : {value1}")
                    app_value(merged_data, field, value1)
        
        return merged_data
    except Exception as e:
        log_message('failed', f"Error merging JSON files: {e}")
        raise ValueError(f"Error merging JSON files: {e}")

def save_to_s3(s3_client, bucket, key, data):
    """Save merged JSON data to S3."""
    s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(data, indent=2))

def extract_base_filename(filename,regex,input_path,inputFileName):
    """
    Extracts the base filename from the given pattern.
    Example: TEXT0571293.240722.19075702.00001.00002.rpt -> TEXT0571293.240722.19075702.rpt
    """
    pattern = re.compile(regex)
    match = pattern.match(filename)
    
    if "socgen" in input_path.lower():
        base = inputFileName.replace('.json', '')
        name_without_ext = base.replace('_done', '')
        return f"{name_without_ext}.json"
    if match:
        return f"{match.group(1)}.{match.group(match.lastindex)}"
    return "merged_output.json"  # Fallback filename

def set_job_params_as_env_vars():
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            value = sys.argv[i + 1]
            os.environ[key] = value

def main():
    try:
        
        set_job_params_as_env_vars()
        
        global inputFileName
        inputFileName = os.getenv('inputFileName')
        bucket=os.getenv('bucket')
        input_path=os.getenv('input_path')
        input_regex=os.getenv('input_regex')
        output_path=os.getenv('output_path')
        config_key=os.getenv('config_key')
        
        s3_client = boto3.client('s3')
        config = load_config_from_s3(s3_client, bucket, config_key)
        matching_files = get_matching_files(s3_client, bucket, input_path, input_regex)
        
        if not matching_files:
            print("No matching files found.")
            return
        
        merged_data = merge_json_files(s3_client, matching_files, bucket, config)
        
        # Extract base filename from the first matching file
        first_filename = matching_files[0].split('/')[-1]
        output_filename = extract_base_filename(first_filename,input_regex,input_path,inputFileName)
        
        output_key = f"{output_path}{output_filename}"
        if "formatBatchedJson" in config and "addRootkey" in config["formatBatchedJson"]:
            merged_data =  {config["formatBatchedJson"]["addRootkey"]: merged_data}
        save_to_s3(s3_client, bucket, output_key, merged_data)
        log_message('success', f"Merged JSON saved to {output_key}")
    except Exception as e:
        log_message('failed', f"Failed to merge files")
        raise ValueError(f"Failed to merge files")

inputFileName = ''
if __name__ == "__main__":
    main()
