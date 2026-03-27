import json
import boto3
import re
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dpm_splunk_logger_py import splunk


# Initialize AWS clients
s3 = boto3.client('s3')
events = boto3.client('events')
logger = boto3.client('logs')

def move_file_s3(bucket_name, secondaryFilePath, inputTriggerPath, fileName, context, inputFileName):
    source_key = f"{secondaryFilePath}{fileName}"
    destination_key = f"{inputTriggerPath}{fileName}"

    try:
        # Copy file to destination
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': source_key},
            Key=destination_key
        )
        # Delete original file
        s3.delete_object(Bucket=bucket_name, Key=source_key)
        splunk.log_message({'Message': f"Moved {source_key} → {destination_key}", "FileName": inputFileName, "Status": "success"}, context)
    except Exception as e:
        error_message = f"Error moving file from {source_key} to {destination_key}: {str(e)}"
        splunk.log_message({'Message': error_message, "FileName": inputFileName, "Status": "failed"}, context)
        raise Exception(error_message)

def get_file_list(bucket, secondaryFilePath, secondaryFilePattern, context) -> List[str]:
    """
    Retrieve a list of files from the specified S3 bucket and path
    that match the secondary file pattern.
    """
    try:
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=secondaryFilePath)

        matched_files = []
        pattern = re.compile(secondaryFilePattern)
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if pattern.search(key.split('/')[-1]):
                        matched_files.append(key)
        
        return matched_files
    
    except Exception as e:
        error_message = f"Error retrieving file list: {str(e)}"
        splunk.log_message({'Message': error_message, "FileName": "N/A", "Status": "failed"}, context)
        raise Exception(error_message)

def lambda_handler(event, context):
    """
    Main Lambda handler for file-based trigger processing
    """
    print("**********************")
    try:
        global inputFileName
        bucket = event.get("bucket")
        inputFileName = event.get("inputFileName")
        fileKey = event.get("fileKey")
        fileType = event.get("fileType")
        secondaryFilePath = event.get("secondaryFilePath")
        inputTriggerPath = event.get("inputTriggerPath")
        secondaryFilePattern = event.get("secondaryFilePattern")
        print(f"bucket : {bucket}")
        
        fileList = get_file_list(bucket, secondaryFilePath, secondaryFilePattern, context)
        if fileType=="done":
            doneFileName=fileKey.split('/')[-1]+".done"
            if fileList:
                splunk.log_message({'Message': f"Found {len(fileList)} matching data files.", "FileName": inputFileName, "Status": "logging"}, context)
                # Mark the original file as done
                s3.put_object(Bucket=bucket, Key=inputTriggerPath + doneFileName, Body=b'')
            else:
                splunk.log_message({'Message': "No matching data files found.", "FileName": inputFileName, "Status": "logging"}, context)
                s3.put_object(Bucket=bucket, Key=secondaryFilePath + doneFileName, Body=b'')
        else:
            if fileList:
                splunk.log_message({'Message': f"Found {len(fileList)} matching done files. : {fileList[0]}", "FileName": inputFileName, "Status": "logging"}, context)
                move_file_s3(bucket, secondaryFilePath, inputTriggerPath, fileList[0].split('/')[-1], context, inputFileName)
            else:
                splunk.log_message({'Message': "Waiting for done file to be created.", "FileName": inputFileName, "Status": "logging"}, context)
            
    
    except Exception as e:
        error_message = f"Lambda execution error: {str(e)}"
        splunk.log_message({'Message': error_message, "FileName": inputFileName if 'inputFileName' in locals() else "N/A", "Status": "failed"}, context)
        raise Exception(error_message)
    print("**********************")
