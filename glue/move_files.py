import boto3
import json
import os
import sys
import splunk
from botocore.exceptions import ClientError

# Initialize S3 client
s3 = boto3.client('s3')

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:br-ics-dpmdi-move_files : " + account_id

def log_message(status, message):
    splunk.log_message({'FileName': inputFileName, 'Status': status, 'Message': message}, get_run_id())

# Set job parameters as environment variables
def set_job_params_as_env_vars():
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i + 1]
            os.environ[key] = value

def main():
    
    # Load environment variables
    set_job_params_as_env_vars()
    
    # Define bucket and directory paths
    global inputFileName
    inputFileName = os.getenv('inputFileName')
    bucket_name = os.getenv('bucket_name')
    input_dir = os.getenv('input_dir')
    output_dir = os.getenv('output_dir')
    manifest_file_path = os.getenv('manifest_file_path')
    move_key_flag = os.getenv('move_key_flag')
    move_config = json.loads(os.getenv('move_config'))

    # Check if any of the essential variables are None
    if not bucket_name or not input_dir or not output_dir or not manifest_file_path:
        raise ValueError(f"Environment variables are not properly set: bucket_name={bucket_name}, input_dir={input_dir}, output_dir={output_dir}, manifest_file_path={manifest_file_path}")

    try:
        # Ensure the output directory exists
        ensure_directory_exists(bucket_name, output_dir)

        # Read the content of the manifest file
        response = s3.get_object(Bucket=bucket_name, Key=manifest_file_path)
        raw_content = response['Body'].read().decode('utf-8', errors='replace')

        # Check for BOM and strip if present
        if raw_content.startswith('\ufeff'):
            raw_content = raw_content[1:]

        # Parse the JSON content
        manifest_content = json.loads(raw_content)

        # Move the manifest file to the output directory without renaming
        manifest_dest_path = f'{output_dir}{os.path.basename(manifest_file_path)}'
        if move_file(bucket_name, manifest_file_path, manifest_dest_path):
            log_message('success', f'Manifest file moved to {manifest_dest_path} successfully.')
        else:
            log_message('failed', "Error moving manifest file.")
            raise ValueError("Error moving manifest file.")

        # Process the manifest content using the provided config
        for move_item in move_config:
            print("Stage 1")
            moveFilesDataType = move_item["moveFilesDataType"]
            moveFilesKey = move_item['moveFileskey']
            extract_value_from_json(bucket_name, moveFilesDataType, moveFilesKey, manifest_content,input_dir,output_dir,move_key_flag)
            print("moveFilesDataType:", moveFilesDataType)
            print("moveFilesKey:", moveFilesKey)

    except json.JSONDecodeError as e:
        log_message('failed', f"JSON decoding error: {e}")
        raise ValueError(f"JSON decoding error: {e}")
    except Exception as e:
        raise ValueError(f"An unexpected error occurred: {e}")

def extract_value_from_json(bucket_name, moveFilesDataType, moveFilesKey, manifest_content,input_dir,output_dir,move_key_flag):
    if len(moveFilesKey) == 1:
        if moveFilesDataType[0] == 'json':
            if moveFilesKey[0] == "$foreach":
                # Handle the foreach loop logic here
                for file_key, file_value in manifest_content.items():
                    if (move_key_flag.lower())=="true":
                        move_file(bucket_name, f"{input_dir}{file_key}", f"{output_dir}{file_key}")
                    move_file(bucket_name, f"{input_dir}{file_value}", f"{output_dir}{file_value}")
                    
            else:
                # Move specific file based on the key
                key = moveFilesKey[0]
                move_file(bucket_name, f"{input_dir}{manifest_content[key]}", f"{output_dir}{manifest_content[key]}")
        elif moveFilesDataType[0] == 'list':
            # Handle the list type for the files
            key = moveFilesKey[0]
            if len(key) != 0:
                for index in key:
                    move_file(bucket_name, f"{input_dir}{manifest_content[index]}", f"{output_dir}{manifest_content[index]}")
            else:
                # If the key is empty, move all files in the list
                for value in manifest_content:
                    move_file(bucket_name, f"{input_dir}{value}", f"{output_dir}{value}")
    else:
        data_type = moveFilesDataType[0]
        key = moveFilesKey[0]
        if data_type == 'list':
            if len(key) != 0:
                for index in key:
                    extract_value_from_json(bucket_name, moveFilesDataType[1:], moveFilesKey[1:], manifest_content[index],input_dir,output_dir,move_key_flag)
            else:
                for value in manifest_content:
                    extract_value_from_json(bucket_name, moveFilesDataType[1:], moveFilesKey[1:], value,input_dir,output_dir,move_key_flag)
        elif data_type == 'json':
            if key == "$foreach":
                for keys in manifest_content:
                    extract_value_from_json(bucket_name, moveFilesDataType[1:], moveFilesKey[1:], manifest_content[keys],input_dir,output_dir,move_key_flag)
            else:
                extract_value_from_json(bucket_name, moveFilesDataType[1:], moveFilesKey[1:], manifest_content[key],input_dir,output_dir,move_key_flag)
                
        else:
            raise ValueError(f"Unsupported data type: {data_type}")

# Function to move files
def move_file(bucket_name, source_path, dest_path):
    try:
        s3.head_object(Bucket=bucket_name, Key=source_path)
        s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': source_path}, Key=dest_path)
        s3.delete_object(Bucket=bucket_name, Key=source_path)
        return True
    except ClientError as e:
        log_message('failed', f"Failed to move file from {source_path} to {dest_path}: {e}")
        raise ValueError(f"Failed to move file from {source_path} to {dest_path}: {e}")
    except Exception as e:
        log_message('failed', f"Failed to move file from {source_path} to {dest_path}: {e}")
        raise ValueError(f"An unexpected error occurred: {e}")

# Ensure the output directory exists
def ensure_directory_exists(bucket_name, directory_path):
    try:
        s3.put_object(Bucket=bucket_name, Key=(directory_path if directory_path.endswith('/') else directory_path + '/'))
        log_message('success', f"Directory {directory_path} checked/created successfully.")
    except ClientError as e:
        log_message('failed', f"Failed to ensure directory {directory_path} exists: {e}")
        raise ValueError(f"Failed to ensure directory {directory_path} exists: {e}")

inputFileName = ''
if __name__ == "__main__":
    main()