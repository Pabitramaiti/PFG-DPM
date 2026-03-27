import boto3
import os
import json
import sys
import re
import splunk

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:supplementary_file_validation:" + account_id

def log_message(status, message):
    splunk.log_message({'FileName':fileName,'Status': status, 'Message': message}, get_run_id())

def validate_file(s3_client, bucket, file_key, mandatory_fields, primary_keys):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=file_key)
        lines = response['Body'].read().decode('utf-8').splitlines()
        
        if not lines:
            log_message('failed',f" E102: missing minimum detail record (since record is missing, RecNum = NA and no record dump)")
            raise ValueError("E102: missing minimum detail record (since record is missing, RecNum = NA and no record dump)")
    
        header = lines[0].strip().split('|')
        details = [line.strip().split('|') for line in lines[1:-1]]
        trailer = lines[-1].strip().split('|')
        
        if not header[0] == 'H':
            log_message('failed',f" E101: missing header record (since record is missing, RecNum = NA and no record dump)")
            raise ValueError("E101: missing header record (since record is missing, desRecNum = NA and no record dump)")
        if not trailer[0] == 'T':
            log_message('failed',f" Error loading JSON from S3: {str(e)}")
            raise ValueError("E103: missing trailer record (since record is missing, RecNum = NA and no record dump)")
        if not details:
            log_message('failed',f" E102: missing minimum detail record (since record is missing, RecNum = NA and no record dump)")
            raise ValueError("E102: missing minimum detail record (since record is missing, RecNum = NA and no record dump)")
    
        expected_pipe_count = len(header) - 1  
        for detail in details:
            if len(detail) - 1 != expected_pipe_count:
                log_message('failed',f" E105: invalid pipe count (expected: {expected_pipe_count}, actual: {len(detail) - 1})")
                raise ValueError(f"E105: invalid pipe count (expected: {expected_pipe_count}, actual: {len(detail) - 1})")
    
        if len(trailer) < 3 or not trailer[2].isdigit():
            log_message('failed',f" E104: out of balance (expected: {expected_count}, actual: {trailer[2] if len(trailer) > 2 else 'N/A'})")
            raise ValueError(f"E104: out of balance (expected: {expected_count}, actual: {trailer[2] if len(trailer) > 2 else 'N/A'})")

        trailer_count = int(trailer[2])  # Safe conversion after validation
        
        if trailer_count != len(details):
            log_message('failed',f" E104: out of balance (expected: {len(details)}, actual: {trailer_count})")
            raise ValueError(f"E104: out of balance (expected: {len(details)}, actual: {trailer_count})")
    
        # Normalize headers
        header_fields = {field.strip().lower(): index for index, field in enumerate(header)}
    
        # Validate mandatory fields
        missing_fields = [field for field in mandatory_fields if field.strip().lower() not in header_fields]
        if missing_fields:
            log_message('failed',f" Mandatory fields missing in header: {missing_fields}")
            raise ValueError(f"Mandatory fields missing in header: {missing_fields}")
    
        for detail in details:
            for field_name in mandatory_fields:
                field_index = header_fields[field_name.strip().lower()]
                if not detail[field_index].strip():
                    log_message('failed',f" E107: mandatory field '{field_name}' not populated in record: {detail}")
                    raise ValueError(f"E107: mandatory field '{field_name}' not populated in record: {detail}")
    
        # Validate duplicates
        primary_key_indices = [header_fields[key.strip().lower()] for key in primary_keys]
        seen_records = set()
        for detail in details:
            record_key = tuple(detail[idx] for idx in primary_key_indices)  
            if record_key in seen_records:
                log_message('failed',f" E106: duplicate record in file based on primary keys {primary_keys}: {record_key}")
                raise ValueError(f"E106: duplicate record in file based on primary keys {primary_keys}: {record_key}")
            seen_records.add(record_key)
    
        # Validate LoanNumber field length
        loan_number_index = header_fields.get("loannumber", None)
        if loan_number_index is None:
            log_message('failed',f" E108: LoanNumber field not found in header. Available fields: {list(header_fields.keys())}")
            raise ValueError(f"E108: LoanNumber field not found in header. Available fields: {list(header_fields.keys())}")
    
        for detail in details:
            if len(detail[loan_number_index]) != 13:
                log_message('failed',f" E108: key/loan number <> 13 digits in record: {detail}")
                raise ValueError(f"E108: key/loan number <> 13 digits in record: {detail}")
    
        log_message('success',f" File validation successful! Moving file to destination path.")
    except Exception as e:
        raise e

def set_job_params_as_env_vars():
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            value = sys.argv[i + 1]
            os.environ[key] = value
            
def main():
    try:
        s3_client = boto3.client('s3')
        global fileName
        set_job_params_as_env_vars()
        fileName = os.getenv('fileName')
        bucket = os.getenv('s3_bucket')
        file_key = os.getenv('file_key')
        mandatory_fields = json.loads(os.getenv('mandatory_fields'))
        primary_keys = json.loads(os.getenv('primary_keys'))
        run_type = os.getenv('run_type')
        if fileName.endswith(".dat") and run_type.lower() == "test":
            raise ValueError("Prod file (.dat) cannot be processed in test environment.")
        elif fileName.endswith(".tst") and run_type.lower() == "prod":
            raise ValueError("Test/UAT file (.tst) cannot be processed in production environment.")
            
        validate_file(s3_client, bucket, file_key, mandatory_fields, primary_keys)
        log_message('success',f" supplementary_file_validation:{fileName} done successfully")
    except Exception as e:
        log_message('failed',f"Unexpected error in main: {str(e)}")
        raise e
inputFileName=''
if __name__ == "__main__":
    main()
