import json
import sys
import boto3
import os
import time
from s3fs import S3FileSystem
import re
#import splunk
from awsglue.utils import getResolvedOptions
from datetime import datetime, timezone
#from datetime import datetime
import uuid
import splunk

# --- S3 filesystem ---
sfs = S3FileSystem()

#Get execution run-id

def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return f"arn:dpm:glue:glue_positional_validation:" + account_id


# --- Splunk logger (unchanged) ---
def log_event(status: str, message: str, input_file: str):
    job_name = "glue_RecType_Validation"
    if os.getenv("statemachine_name"):
        parts = os.getenv("statemachine_name").split("-")
        if len(parts) >= 3:
            prefix = "_".join(parts[0:3])
            job_name = f"{prefix}_{job_name}"

    event_data = {
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "file_name": input_file,
        "clientName": os.getenv("clientName", ""),
        "clientID": os.getenv("clientID", ""),
        "applicationName": os.getenv("applicationName", ""),
        "step_function": "validation",
        "component": "glue",
        "job_name": job_name,
        "job_run_id": get_run_id(),
        "execution_name": os.getenv("execution_name", ""),
        "execution_id": os.getenv("execution_id", ""),
        "execution_starttime": os.getenv("execution_starttime", ""),
        "statemachine_name": os.getenv("statemachine_name", ""),
        "statemachine_id": os.getenv("statemachine_id", ""),
        "state_name": os.getenv("state_name", ""),
        "state_enteredtime": os.getenv("state_enteredtime", ""),
        "status": status,
        "message": message,
        "aws_account_id": boto3.client("sts").get_caller_identity()["Account"],
    }

    print(json.dumps(event_data))

    try:
        splunk.log_message(event_data, get_run_id())
    except Exception:
        print("[ERROR] glue_RecType_Validation : log_event : Failed to write log to Splunk")

# ---------------------------------------------------------
# Validate Last Line Function
# ---------------------------------------------------------

def validate_last_line(bucket, key, validation_value, run_id):
    file_name = key
    s3 = boto3.client('s3')
    last_line = ""
    status = "FAILED"
    message = ""
    start_time = datetime.utcnow()

    try:
        obj = s3.get_object(Bucket=bucket, Key=key)

        # Read file line by line (memory efficient)
        for line in obj['Body'].iter_lines():
            if line.strip():
                last_line = line.decode('utf-8')

        # Check if validation value is at end of last line
        if last_line == validation_value:
            status = "SUCCESS"
            message = f"Validation successful. Value {validation_value} found at end of file."
            log_event("SUCCESS",message,file_name)
        else:
            status = "FAILED"
            message = f"Validation failed. Value  {validation_value} not found at end of file."
            log_event("FAILED",message,file_name)
            #raise ValueError so gluejob fails if validation value not found and goes to transaction fail lambda
            raise ValueError("Validation failed") 

    except Exception as e:
        status = "FAILED"
        #to catch all other failures in gluejob
        message = "Error during validation: " + str(e) 
        log_event("FAILED",message,file_name)
        raise e
        

    end_time = datetime.utcnow()

    return status

# ---------------------------------------------------------
# Main Execution
# ---------------------------------------------------------
def main():
    

# ---------------------------------------------------------
# Read Glue Job Arguments
# ---------------------------------------------------------

    args = getResolvedOptions(sys.argv, [
    
        'input_bucket',
        'input_file',
        'validation_value'
    ])


    input_bucket = args['input_bucket']
    input_file = args['input_file']
    validation_value = args['validation_value']

    run_id = get_run_id()

    validation_status = validate_last_line(
        input_bucket,
        input_file,
        validation_value,
        run_id
    )
if __name__ == '__main__':
    main()
