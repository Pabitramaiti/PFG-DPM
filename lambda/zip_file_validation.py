import json
import os
import sys
import boto3
from dpm_splunk_logger_py import splunk
from botocore.exceptions import ClientError
from datetime import datetime

def create_missing_file_report(s3_client, bucket, missing_files, expected_count, actual_count,file_key,config_path, context):
    """
    Creates a detailed missing file report and uploads it to S3
    """
    try:
        splunk.log_message({'Status': 'info', 'Message': 'Creating missing file report'}, context)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_key = f"{file_key}missing_files_report_{timestamp}.json"
        
        report_data = {
            "validation_timestamp": datetime.now().isoformat(),
            "validation_status": "FAILED",
            "summary": {
                "total_expected_files": expected_count,
                "total_found_files": actual_count,
                "missing_files_count": len(missing_files)
            },
            "missing_files": missing_files,
            "validation_details": {
                "config_path": config_path,
                "search_location": file_key
            }
        }
        
        # Upload report to S3
        try:
            s3_client.put_object(
                Bucket=bucket,
                Key=report_key,
                Body=json.dumps(report_data, indent=2),
                ContentType='application/json'
            )
            splunk.log_message({'Status': 'success', 'Message': f'Missing file report uploaded to: s3://{bucket}/{report_key}', 'report_key': report_key}, context)
            return report_key
        except Exception as e:
            error_msg = f"Error creating missing file report: {e}"
            splunk.log_message({'Status': 'failed', 'Message': error_msg}, context)
            return None
    except Exception as e:
        splunk.log_message({'Status': 'failed', 'Message': f'Error in create_missing_file_report: {str(e)}'}, context)
        return None

def check_file_exists(s3_client, bucket, s3_key, context):
    """
    Check if a single file exists in S3
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=s3_key)
        splunk.log_message({'Status': 'info', 'Message': f'File exists: {s3_key}'}, context)
        return None  # File exists
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            splunk.log_message({'Status': 'warning', 'Message': f'File missing: {s3_key}'}, context)
            return s3_key  # File missing
        else:
            error_msg = f"Error checking file {s3_key}: {e}"
            splunk.log_message({'Status': 'failed', 'Message': error_msg}, context)
            return None  # Treat errors as file exists to avoid false positives

def lambda_handler(event, context):
    """
    AWS Lambda handler for file existence validation
    """
    try:
        splunk.log_message({'Status': 'info', 'Message': 'Starting file validation process'}, context)
        
        # S3 configuration
        s3_bucket = event["s3_bucket"]
        config_path = event["config_path"]
        transactionId = event["transactionId"]
        
        splunk.log_message({'Message': 'File validation parameters', 's3_bucket': s3_bucket, 'config_path': config_path, 'transactionId': transactionId, 'Status': 'logging'}, context)
        
        # Initialize S3 client
        s3_client = boto3.client('s3')

        # Load config from S3
        try:
            splunk.log_message({'Status': 'info', 'Message': f'Loading config from S3: {config_path}'}, context)
            response = s3_client.get_object(Bucket=s3_bucket, Key=config_path)
            config = json.loads(response['Body'].read().decode('utf-8'))
            splunk.log_message({'Status': 'success', 'Message': 'Config loaded successfully from S3'}, context)
        except ClientError as e:
            error_msg = f"Error loading config from S3: {e}"
            splunk.log_message({'Status': 'failed', 'Message': error_msg}, context)
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'status': 'error',
                    'message': error_msg
                })
            }

        file_key = config["file_validation"]["files"]["file_key"]
        if "wip" in file_key.lower():
            file_key= file_key+ transactionId + "/"
        files_name = config["file_validation"]["files"]["files_name"]
        total_expected = config["file_validation"]["total_expected"]
        
        splunk.log_message({'Message': 'Validation configuration', 'file_key': file_key, 'total_expected': total_expected, 'files_count': len(files_name), 'Status': 'logging'}, context)
            
        # 1. Confirm count matches
        count_mismatch = False
        if len(files_name) != total_expected:
            count_mismatch = True
            error_msg = f"Count mismatch: total_expected={total_expected}, files listed={len(files_name)}"
            splunk.log_message({'Status': 'warning', 'Message': error_msg}, context)
        else:
            splunk.log_message({'Status': 'info', 'Message': 'File count matches total_expected'}, context)

        # 2. Check existence in S3 (sequential)
        splunk.log_message({'Status': 'info', 'Message': f'Starting file existence check for {len(files_name)} files'}, context)
        missing_files = []
        for fname in files_name:
            s3_key = f"{file_key}{fname}"
            result = check_file_exists(s3_client, s3_bucket, s3_key, context)
            if result:
                missing_files.append(result)

        # 3. Report missing files
        if missing_files or count_mismatch:
            # Calculate actual found file count
            found_count = len(files_name) - len(missing_files)
            
            splunk.log_message({'Status': 'warning', 'Message': f'Validation issues detected: {len(missing_files)} missing files, count_mismatch: {count_mismatch}'}, context)
            
            # Create missing file report with actual found count
            report_key = create_missing_file_report(s3_client, s3_bucket, missing_files, total_expected, found_count,file_key,config_path, context)
            print("report_key",report_key)
            
            error_details = []
            if count_mismatch:
                error_details.append(f"Count mismatch: expected {total_expected}, got {len(files_name)}")
            if missing_files:
                error_details.append(f"Missing files: {missing_files}")
            
            for detail in error_details:
                print(f"  {detail}")
            
            splunk.log_message({'Status': 'failed', 'Message': 'File validation failed', 'missing_files_count': len(missing_files), 'found_count': found_count, 'error_details': error_details}, context)
            # --- Clean, final failure response block ---
            error_message_text = f"File Pre-validation failed: Total Expected :{total_expected}  Found Count :{found_count}"
            failure_response = {
                'status': 'failed',
                'JobName': 'br_icsdev_dpmjayant_lmbd_zip_file_validation',
                'ErrorMessage': error_message_text,
                'total_expected': total_expected,
                'files_found': found_count,
                'missing_files': missing_files,
                'count_mismatch': count_mismatch
            }
            return {
                "lambdaError": True,
                "Error": "ValidationFailed",
                "Cause": json.dumps(failure_response)
            }
        else:
            splunk.log_message({'Status': 'success', 'Message': 'All files validated successfully', 'total_expected': total_expected, 'files_found': len(files_name)}, context)
            return {
                "lambdaError": False,
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'success',
                    'message': 'All files validated successfully',
                    'total_expected': total_expected,
                    'files_found': len(files_name),
                    'missing_files': [],
                    'count_mismatch': False
                })
            }

    except Exception as e:
        # Bubble up exceptions so the Lambda invocation fails
        error_msg = f"An error occurred: {e}"
        splunk.log_message({'Status': 'failed', 'Message': error_msg}, context)
        raise