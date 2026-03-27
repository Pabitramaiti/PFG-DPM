import boto3
import json
import os
import re
from dpm_splunk_logger_py import splunk
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
       
        obj_key = event['ObjectKey']
        
        destination_key = event['DestinationKey']
        bucket_name = event['BucketName']
        file_path = event['file_path']
        file_pattern = event['file_pattern']
        if file_path != ' ':
            obj_key = find_matching_file(bucket_name, file_path, file_pattern,context)
        file_name = obj_key.rsplit('/',1)[1]
        copy_source = {'Bucket':bucket_name, 'Key': obj_key}
        
        s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=destination_key+file_name)
    
        # s3_client.delete_object(Bucket=bucket_name, Key=obj_key)
        response = {
            'statusCode': 200,
            'body': json.dumps(f'{file_name} file processed successfully')
        }

        return response
    except Exception as e:
        splunk.log_message({'Message':f'{e}' , 'Status':'Failed', }, context)
        raise e
    
def find_matching_file(bucket_name, file_path, file_pattern, context):
    # Use paginator to find the first occurrence of file_pattern in the given file_path and bucket_name
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=file_path)

    found_key = None
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                # print(f"Checking key: {key}")
                if re.search(file_pattern, key.rsplit('/',1)[1]):
                    found_key = key
                    return found_key

    if not found_key:
        message = f"No file matching pattern '{file_pattern}' found in '{file_path}' of bucket '{bucket_name}'"
        splunk.log_message({'Message':f'{message}' , 'Status':'Failed', }, context)
        raise Exception(message)