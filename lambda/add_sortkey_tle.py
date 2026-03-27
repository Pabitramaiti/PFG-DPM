import os
import json
import traceback
import boto3
import re
from dpm_splunk_logger_py import splunk

s3 = boto3.client('s3')


def get_matching_s3_keys(bucket, prefix='', pattern=''):
    try:    
        s3 = boto3.client('s3')
        paginator = s3.get_paginator('list_objects_v2')
        keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = os.path.basename(obj['Key'])
                if re.match(pattern, key.rsplit('/', 1)[-1]):
                    keys.append(obj['Key'])
        if not keys:
            raise Exception("No matching S3 keys found.")
        return keys[0]
    except Exception as e:
        message = f"Exception in get_matching_s3_keys: {e}"
        raise Exception(message)

def get_by_path(obj, path):
    """Safely walk through obj by path tuple/list."""
    try:
        for key in path:
            if isinstance(obj, list):
                if not isinstance(key, int) or key >= len(obj):
                    return None
                obj = obj[key]
            elif isinstance(obj, dict):
                obj = obj.get(key)
            else:
                return None
            if obj is None:
                return None
        return obj
    except Exception as e:
        message = f"Exception in get_by_path: {e}"
        raise Exception(message)

def set_by_path(obj, path, value):
    """Safely walk through obj by path tuple/list. and update value"""
    if not path:
        raise ValueError("Path cannot be empty")
    curr = obj
    for i, key in enumerate(path):
        is_last = (i == len(path) - 1)

        if isinstance(curr, dict):
            if is_last:
                curr[key] = value
                return
            if key not in curr or curr[key] is None:
                curr[key] = {} if isinstance(path[i+1], str) else []
            curr = curr[key]

        elif isinstance(curr, list):
            if not isinstance(key, int):
                raise TypeError(f"Expected int index for list, got {key!r}")
            while len(curr) <= key:
                curr.append(None)
            if is_last:
                curr[key] = value
                return
            if curr[key] is None:
                curr[key] = {} if isinstance(path[i+1], str) else []
            curr = curr[key]
        else:
            raise TypeError(f"Cannot traverse into type {type(curr).__name__}")


def TLE_update(source, mapping, context):
    bundle_keys = {}
    for i in get_by_path(source, mapping['root_path']):
        if 'filters' in mapping:
            filter_results = []
            for f in mapping['filters']:
                field_value = get_by_path(i, f['field'])
                if f.get('exclude') == "true":
                    filter_results.append(field_value not in f['value'])
                else:
                    filter_results.append(field_value in f['value'])
            if mapping.get('logical_operator', 'AND') == 'AND':
                if not all(filter_results):
                    continue
            else:
                if not any(filter_results):
                    continue

        input_key = get_by_path(i, mapping['input_path'])
        if input_key in bundle_keys:
            bundle_keys[input_key]+=1
        else:
            bundle_keys[input_key] = 1
        if mapping.get('farmat'):
            set_by_path(i, mapping['output_path'], f"{mapping.get('pre_data','')}{bundle_keys[input_key]:{mapping['farmat']}}{mapping.get('post_data','')}")
        else:
            set_by_path(i, mapping['output_path'], f"{mapping.get('pre_data','')}{bundle_keys[input_key]}{mapping.get('post_data','')}")
    return source


def lambda_handler(event, context):
    print(f"Lambda invoked with event: {json.dumps(event)}")
    global inputFileName

    # Get S3 parameters
    fileKey = event.get("fileKey")
    inputFileName = event.get("inputFileName")
    bucketName = event.get("bucketName")
    filePath = event.get("filePath")
    filePattern = event.get("filePattern")
    outputPath = event.get("outputPath")
    mappingKey = event.get("mappingKey") 
    outputFilePostfix = event.get('outputFilePostfix')
    fileKey = fileKey if fileKey !=" " else get_matching_s3_keys(bucketName, filePath, filePattern)

    fileName = os.path.basename(fileKey)
    outputKey = f"{fileName.rsplit('.', 1)[0]}{outputFilePostfix}.{fileName.rsplit('.', 1)[-1]}"
    outputFile = f"{outputPath}{outputKey}"


    if not bucketName or not fileKey or not outputKey or not mappingKey:
        splunk.log_message({'FileName': inputFileName, 'message': "'bucket', 'inputFileName', and 'outputFileName' are required in event" }, context)

        raise ValueError(f"Error: 'bucket', 'inputFileName', and 'outputFileName' are required in event")

    try:
        # 1️ Read input JSON from S3
        print(f"Reading JSON from s3://{bucketName}/{fileKey}")
        splunk.log_message({'FileName': inputFileName, 'Input Path': f"s3://{bucketName}/{fileKey}"}, context)
        s3_obj = s3.get_object(Bucket=bucketName, Key=fileKey)
        input_json = json.loads(s3_obj['Body'].read().decode('utf-8'))

        # 1️ Read mapping JSON from S3
        print(f"Reading JSON from s3://{bucketName}/{mappingKey}")
        splunk.log_message({'FileName': inputFileName, 'Input Path': f"s3://{bucketName}/{mappingKey}"}, context)
        s3_obj = s3.get_object(Bucket=bucketName, Key=mappingKey)
        mapping = json.loads(s3_obj['Body'].read().decode('utf-8'))

        # 2️ Run TLE_update
        updated_json = TLE_update(input_json, mapping, context)

        # 3️ Write updated JSON back to S3
        print(f"Writing updated JSON to s3://{bucketName}/{outputFile}")
        splunk.log_message({'FileName': inputFileName, 'Output Path': f"s3://{bucketName}/{outputFile}"}, context)
        s3.put_object(
            Bucket=bucketName,
            Key=outputFile,
            Body=json.dumps(updated_json, indent=4)
        )
        print("Processing complete.")
        splunk.log_message({'FileName': inputFileName, 'statusCode': 200, 'message': 'File processed successfully', 'outputFilePath': f"s3://{bucketName}/{outputFile}" }, context)
        return {
            'statusCode': 200,
            'message': 'File processed successfully',
            'outputFilePath': f"s3://{bucketName}/{outputFile}"
        }

    except Exception as e:
        print("Lambda failed: " + str(e))
        print(traceback.format_exc())
        splunk.log_message({'FileName': inputFileName, 'Message': f"Failed to add DOC_SEQUENCE_KEY", 'Error': str(e)}, context)
        raise Exception("Error in triggering Step Function: " + str(e)) from e
