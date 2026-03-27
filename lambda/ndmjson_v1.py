import boto3
import json
import os
import re
import io
import datetime
from dpm_splunk_logger_py import splunk

# initialize s3 client
s3_client = boto3.client('s3')

# Get the table name from environment variable
env = os.environ['SDLC_ENV']

def lambda_handler(event, context):
    splunk.log_message({'Status': 'success', 'Message': f'{context.function_name} invoked'}, context)

    source_bucket = event["bucket"]
    source_key = event["object"]
    ndmFilePath = event["ndmFilePath"]
    ndmFilePattern = event["ndmFilePattern"]
    moveFlag = event["moveFlag"]

    matched_files = []
    if ndmFilePath:
        matched_files = find_ndm_files(source_bucket, ndmFilePath, ndmFilePattern, context)
        if not matched_files:
            error_message = f"No files matching the pattern {ndmFilePattern} were found under the path {ndmFilePath}"
            splunk.log_message({'Status': 'failed', 'Message': error_message}, context)
            raise ValueError(error_message)
        jsonParams_json = event['ndmjsonparams']

    # Collect all mappings here
    all_mappings = []

    for source_key in matched_files:
        key_dict = splitkey(source_key)
        response = copy_file(source_bucket, source_key, jsonParams_json, key_dict, moveFlag, event, context)

        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            splunk.log_message({'InputFileName': key_dict['filename'], 'Status': 'success', 'Message': 'File move successful'}, context)

            # Instead of uploading JSON now, just build mapping object and store it
            mapping = build_json_mapping(source_bucket, source_key, jsonParams_json, key_dict, context)
            all_mappings.append(mapping)
        else:
            splunk.log_message({'InputFileName': key_dict['filename'], 'Status': 'failed', 'Message': 'File move failed'}, context)
            return {
                'statusCode': 500,
                'body': json.dumps('file failed to move to the transmission bucket')
            }

    # Upload one combined JSON after all files processed
    final_output = {"mappings": all_mappings}
    upload_combined_json(source_bucket, jsonParams_json, final_output, context)

    return {
        'statusCode': 200,
        'body': json.dumps('Combined JSON file uploaded successfully!')
    }


def splitkey(source_key):
    words = re.split(r'\/', source_key)
    key = {
        "system": words[0],
        "subfolder": words[1],
        "filename": words[-1]
    }
    return key


# New helper to only *build* JSON entry instead of uploading each time
def build_json_mapping(source_bucket, source_key, jsonParams_json, key_dict, context):
    global env
    output_json = {
        "job_name": "",
        "s3_source": "",
        "archive": "",
        "sizeInMB": "",
        "brmf_destination": "",
        "runtask": "",
        "isGDG": "",
        "snode": "",
        "bros_destination": "",
        "filetype": "",
        "username": "",
        "password": "",
        "length": ""
    }

    fname = os.path.splitext(key_dict["filename"])[0]
    toOnPrem_bucket = source_bucket
    toOnPrem_key = jsonParams_json["to_internal_uri"] + key_dict["filename"]

    snode = jsonParams_json["snode"][env]

    output_json["job_name"] = key_dict["system"] + "_" + jsonParams_json["job_name"] + "_" + fname
    output_json["s3_source"] = "s3://" + toOnPrem_bucket + "/" + toOnPrem_key
    output_json["archive"] = jsonParams_json["archive_uri"]
    output_json["snode"] = snode[0]
    output_json["username"] = snode[1]

    if ('s3_ndm' in jsonParams_json and jsonParams_json['s3_ndm']):
        output_json["bros_destination"] = f"s3://{jsonParams_json['s3_ndm'][env]['bucket']}/{jsonParams_json['s3_ndm'][env]['path']}{toOnPrem_key.split('/')[-1]}"
    else:
        output_json["bros_destination"] = "/dsto/cbu/" + snode[2] + "/" + jsonParams_json["onprem_system"] + "/" + jsonParams_json["onprem_subsystem"] + "/" + jsonParams_json['onprem_folder']

    return output_json


# New small function to upload combined JSON once
def upload_combined_json(source_bucket, jsonParams_json, final_output, context):
    try:
        json_data = json.dumps(final_output)
        toOnPrem_json_bucket = source_bucket
        # create one combined file with timestamp to make unique
        toOnPrem_json_key = jsonParams_json["to_internal_json_uri"] + "combined_" + datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + ".json"

        s3_client.upload_fileobj(
            io.BytesIO(json_data.encode('utf-8')),
            toOnPrem_json_bucket,
            toOnPrem_json_key
        )
        splunk.log_message({'Status': 'success', 'Message': f'Combined JSON created: {toOnPrem_json_key}'}, context)
    except Exception as e:
        splunk.log_message({'Status': 'failed', 'Message': f'Combined JSON creation failed: {str(e)}'}, context)
        raise


def copy_file(source_bucket, source_key, jsonParams_json, key_dict, moveFlag, event, context):
    toOnPrem_bucket = source_bucket
    toOnPrem_key = jsonParams_json["to_internal_uri"] + key_dict["filename"]
    copy_source = {'Bucket': source_bucket, 'Key': source_key}
    response = s3_client.copy_object(CopySource=copy_source, Bucket=toOnPrem_bucket, Key=toOnPrem_key)

    if moveFlag == 'True':
        s3_client.delete_object(Bucket=source_bucket, Key=source_key)

    return response


def find_ndm_files(bucket_name, ndmFilePath, ndmFilePattern, context):
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    matched_files = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=ndmFilePath):
        for obj in page.get('Contents', []):
            file_key = obj['Key']
            filename = file_key.split('/')[-1]
            if re.match(ndmFilePattern, filename):
                matched_files.append(file_key)
    return matched_files if matched_files else None