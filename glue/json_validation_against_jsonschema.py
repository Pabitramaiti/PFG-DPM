import sys
import boto3
import json
import re
import os
from botocore.exceptions import ClientError
from jsonschema import validate, ValidationError, SchemaError
from awsglue.utils import getResolvedOptions
import splunk

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:br_icsdev_dpmtest_json_validation_against_jsonschema:" + account_id

def log_message(status, message):
    splunk.log_message({'FileName':inputFileName,'Status': status, 'Message': message}, get_run_id())
    # splunk.log_message({'status': status, 'message': message}, get_run_id())

# def get_configs():
#     try:
#         log_message('logging',f"extracting input parameters from step function")
#         # args = sys.argv[1:]  # Skip the script name
#         # config = {}
#         # for i in range(0, len(args), 2):
#         #     key = args[i]
#         #     value = args[i + 1] if i + 1 < len(args) else None
#         #     config[key.lstrip('--')] = value
                    
#         args = getResolvedOptions(sys.argv, [
#             'bucketName', 
#             'inputFileName',
#             'jsonSchemaPath', 
#             'jsonSchemaPattern', 
#             'jsonSchemaKey', 
#             'jsonPath', 
#             'jsonPattern', 
#             'jsonKey', 
#             'crossfileValidation'
#         ])
#         return args
#     except Exception as e:
#         log_message('failed', f'Failed to retrieve input parameters from step function due to {str(e)}')
#         raise str(e)


s3 = boto3.client('s3')

def getFileNames(bucket_name, path, pattern):
    regex = re.compile(pattern)
    try:
        def filter_keys_by_pattern(objects, pattern):
            return [obj['Key'] for obj in objects.get('Contents', []) if pattern.search(obj['Key'].split('/')[-1])]
        
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=path)
        files = filter_keys_by_pattern(objects, regex)
        
        if not files:
            error_message = 'No schema files found matching the pattern'
            raise ValueError(error_message)
        
        return files[0]
    except Exception as e:
        log_message('failed',f" failed to get fileNames:{str(e)}")
        raise e

def validate_json(schema, data):
    log_message('logging',f"Validating Json against Json schema:")
    try:
        validate(instance=data, schema=schema)
    except ValidationError as e:
        log_message('failed',f"JSON validation error: {e.message}")
        raise ValueError(f"JSON validation error: {e.message}")
    except SchemaError as e:
        log_message('failed',f"JSON schema error: {e.message}")
        raise ValueError(f"JSON schema error: {e.message}")
    except Exception as e:
        log_message('failed', f"Unexpected error during JSON validation: {str(e)}")
        raise ValueError(f"Unexpected error during JSON validation: {str(e)}")
    
def getJsonValue(bucket_name, object_key):
    try:
        input_response = s3.get_object(Bucket=bucket_name, Key=object_key)
        object_content = input_response['Body'].read()
        input_json_data = json.loads(object_content)
        return input_json_data
    except Exception as e:
        log_message('failed',f"JSON value retrival error: {e.message}")
        raise str(e)

def extract_value_from_json(bucketName,checkFilesDataType, checkFilesKey, input_json_data, checkFilesPath):
    if len(checkFilesKey) == 1:
        if checkFilesDataType[0] == 'json':
            check_file_in_s3(bucketName,input_json_data[checkFilesKey[0]], checkFilesPath)
        elif checkFilesDataType[0] == 'list':
            key = checkFilesKey[0]
            if len(key) != 0:
                for index in key:
                    check_file_in_s3(bucketName,input_json_data[index], checkFilesPath)
            else:
                for value in input_json_data:
                    check_file_in_s3(bucketName,value, checkFilesPath)
    else:
        data_type = checkFilesDataType[0]
        key = checkFilesKey[0]
        if data_type == 'list':
            if len(key) != 0:
                for index in key:
                    extract_value_from_json(bucketName,checkFilesDataType[1:], checkFilesKey[1:], input_json_data[index], checkFilesPath)
            else:
                for value in input_json_data:
                    extract_value_from_json(bucketName,checkFilesDataType[1:], checkFilesKey[1:], value, checkFilesPath)
        elif data_type == 'json':
            extract_value_from_json(bucketName,checkFilesDataType[1:], checkFilesKey[1:], input_json_data[key], checkFilesPath)
        else:
            log_message('failed',f"Unsupported data type: {data_type}")
            raise ValueError(f"Unsupported data type: {data_type}")

def check_file_in_s3(bucketName,filename, checkFilesPath):
    key = checkFilesPath + filename
    try:
        s3.head_object(Bucket=bucketName, Key=key)
    except ClientError as e:
        log_message('failed',f"File not found: {key}")
        raise ValueError(f"File not found: {key}")
        raise e

def set_job_params_as_env_vars():
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            value = sys.argv[i + 1]
            os.environ[key] = value
            print(f'Set environment variable {key} to {value}')


def main():
    
    log_message('logging',f"Validation of Json against Json Schema Glue Job called.")
    # iparm = get_configs()
    set_job_params_as_env_vars()
    
    global inputFileName
    bucketName = os.getenv('bucketName')
    inputFileName = os.getenv('inputFileName')
    jsonSchemaPath = os.getenv('jsonSchemaPath')
    jsonSchemaPattern = os.getenv('jsonSchemaPattern')
    jsonSchemaKey = os.getenv('jsonSchemaKey')
    jsonPath = os.getenv('jsonPath')
    jsonPattern = os.getenv('jsonPattern')
    jsonKey = os.getenv('jsonKey')
    crossfileValidation = json.loads(os.getenv('crossfileValidation'))
    
    jsonSchemaKey = jsonSchemaKey if jsonSchemaKey !=" " else getFileNames(bucketName, jsonSchemaPath, jsonSchemaPattern)
    jsonFilesKey = jsonKey if jsonKey !=" " else getFileNames(bucketName, jsonPath, jsonPattern)
    input_json_data = getJsonValue(bucketName, jsonFilesKey)
    log_message('success',f"json file found at {jsonFilesKey}")
    schema_json_data = getJsonValue(bucketName, jsonSchemaKey)
    log_message('success',f"json schema found at {jsonSchemaKey}")
    validate_json(schema_json_data, input_json_data)
    log_message('success',f"The JSON is valid against JSON Schema.")
    if crossfileValidation:
        for validation in crossfileValidation:
            checkFilesDataType = validation['checkFilesDataType']
            checkFilesKey = validation['checkFileskey']
            checkFilesPath = validation['checkFilesPath']
            extract_value_from_json(bucketName,checkFilesDataType, checkFilesKey, input_json_data, checkFilesPath)
        
        log_message('success',"crossfileValidation : All the files mentioned in json are present in S3.")
    return

inputFileName=''
if __name__ == "__main__":
    main()
