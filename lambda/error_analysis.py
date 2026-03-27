import json
import boto3
import os
from dpm_splunk_logger_py import splunk

def log_to_splunk(message_dict, context):
    """Log message to Splunk using the same pattern as transaction lambda"""
    splunk.log_message(message_dict, context)

def get_failure_json_files(bucket_name, folder_path):
    """
    Get all JSON files from the specified S3 folder
    
    Args:
        bucket_name: S3 bucket name
        folder_path: Path to the folder containing failure JSON files
    
    Returns:
        List of S3 object keys
    """
    s3_client = boto3.client('s3')
    json_files = []
    
    try:
        # Ensure folder path ends with /
        if not folder_path.endswith('/'):
            folder_path += '/'
        
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_path)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    # Only include JSON files, not the folder itself
                    if key.endswith('.json') and key != folder_path:
                        json_files.append(key)
        
        return json_files
    except Exception as e:
        print(f"Error listing files in {folder_path}: {str(e)}")
        raise

def read_failure_json(bucket_name, json_key):
    """
    Read and parse failure JSON file from S3
    
    Args:
        bucket_name: S3 bucket name
        json_key: S3 object key
    
    Returns:
        Dictionary with filename and error
    """
    s3_client = boto3.client('s3')
    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=json_key)
        content = response['Body'].read().decode('utf-8')
        failure_data = json.loads(content)
        return failure_data
    except Exception as e:
        print(f"Error reading {json_key}: {str(e)}")
        raise

def lambda_handler(event, context):
    """
    Lambda handler to process failure JSON files and log to Splunk
    
    Event Payload:
        bucketName: S3 bucket name
        inputFileName: Name of the input file being processed
        error_report_path: Array of folder paths to check for failure files
    
    Example event:
    {
        "bucketName": "my-bucket",
        "inputFileName": "JSC.CUSC0965.TXT",
        "error_report_path": ["jhi/confirms/wip/uuid/output/failed_conversion/"]
    }
    """
    
    try:
        # Get values from event payload
        bucket_name = event.get('bucketName')
        input_filename = event.get('inputFileName', 'unknown')
        failure_folders = event.get('error_report_path', [])
        
        if not bucket_name:
            raise ValueError("bucketName is required in event payload")
        
        if not isinstance(failure_folders, list):
            raise ValueError("error_report_path must be an array")
        
        print(f"Bucket: {bucket_name}")
        print(f"Input File: {input_filename}")
        print(f"Checking {len(failure_folders)} folder(s) for failure reports")
        
        total_failures_found = 0
        all_failure_reports = []
        
        # Process each folder
        for folder_path in failure_folders:
            print(f"\nProcessing folder: {folder_path}")
            
            try:
                # Get all JSON files in the folder
                json_files = get_failure_json_files(bucket_name, folder_path)
                
                if not json_files:
                    print(f"No failure JSON files found in {folder_path}")
                    continue
                
                print(f"Found {len(json_files)} failure report(s) in {folder_path}")
                
                # Process each JSON file
                for json_key in json_files:
                    try:
                        print(f"Reading: {json_key}")
                        failure_data = read_failure_json(bucket_name, json_key)
                        
                        failed_filename = failure_data.get('filename', 'unknown')
                        error_message = failure_data.get('error', 'No error message')
                        
                        # Log to Splunk
                        splunk_message = f"Conversion failure detected for file pattern/name: {failed_filename}. Error: {error_message}"
                        log_to_splunk({
                            'FileName': input_filename,
                            'Status': 'failed',
                            'Message': splunk_message
                        }, context)
                        
                        print(f"Logged failure for: {failed_filename}")
                        
                        all_failure_reports.append({
                            'json_file': json_key,
                            'failed_filename': failed_filename,
                            'error_message': error_message
                        })
                        
                        total_failures_found += 1
                        
                    except Exception as e:
                        error_msg = f"Error processing {json_key}: {str(e)}"
                        print(error_msg)
                        log_to_splunk({
                            'FileName': input_filename,
                            'Status': 'failed',
                            'Message': error_msg
                        }, context)
            
            except Exception as e:
                error_msg = f"Error processing folder {folder_path}: {str(e)}"
                print(error_msg)
                log_to_splunk({
                    'FileName': input_filename,
                    'Status': 'failed',
                    'Message': error_msg
                }, context)
        
        # Log summary
        summary_message = f"Processed {len(failure_folders)} folder(s). Found {total_failures_found} failure report(s)."
        print(f"\n{summary_message}")
        log_to_splunk({
            'FileName': input_filename,
            'Status': 'info',
            'Message': summary_message
        }, context)
        
        # Raise exception with all failure details from the JSON files
        if total_failures_found > 0:
            failure_details = []
            for report in all_failure_reports:
                failure_details.append(f"{report['failed_filename']}: {report['error_message']}")
            
            combined_message = f"Found {total_failures_found} conversion failure(s): " + " | ".join(failure_details)
            raise Exception(combined_message)
        else:
            raise Exception("No failure reports found in the specified folders")
        
    except Exception as e:
        error_msg = f"Lambda execution failed: {str(e)}"
        print(error_msg)
        
        try:
            input_filename = event.get('inputFileName', 'unknown')
            log_to_splunk({
                'FileName': input_filename,
                'Status': 'failed',
                'Message': error_msg
            }, context)
        except:
            pass
        
        # Re-raise the exception to fail the Lambda
        raise