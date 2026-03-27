import json
import boto3
import json
import re
import os
from datetime import datetime
import sys


def lambda_handler(event, context):
    
  
    ##
    bucket_name =  event.get('bucketName')
    file_name = event.get('fileName')
    object_key = event.get('objectKey')  
    
    # TODO implement
    
    s3_client = boto3.client('s3')

    # Get the current date in yyyy-mm-dd format
    current_date = datetime.now().strftime('%Y-%m-%d')
    timestamp = datetime.now().strftime('%Y-%m-%dT%H%M%S')

    # multifile-validation-kafka-trigger_XXXX
    trigger_file_key = f"{object_key}multifile-validation-kafka-trigger_{timestamp}.txt"
   
    s3_client.put_object(Bucket=bucket_name, Key=trigger_file_key, Body=b'hi')
    print('trigger file created'+trigger_file_key)
    return {
        'statusCode': 200,
        'body': json.dumps('Trigger file created successfully  :' )
    }
   
  
