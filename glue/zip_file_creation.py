import boto3
import re
import zipfile
import io
import os
import sys
import ast
import splunk
import json
import time
import traceback
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed

AWS_ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

def get_job_name():
    job_name = "glue_zip_file_creation"
    statemachine_name = os.getenv("statemachine_name")
    if statemachine_name:
        parts = statemachine_name.split("-")
        if len(parts) >= 3:
            prefix = "_".join(parts[0:3])
            job_name = f"{prefix}_{job_name}"
    return job_name


def get_run_id():
    return f"arn:dpm:glue:{get_job_name()}:{AWS_ACCOUNT_ID}"

def log_message(status, message):
    # Function to log messages in splunk
    splunk.log_message({'FileName': input_file_name, 'Status': status, 'Message': message}, get_run_id())

def set_job_params_as_env_vars():
    messages = []
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i+1]
            os.environ[key] = value
            messages.append(f"{key}={value}")

    log_event("INFO", f"Env vars set from job params: {', '.join(messages)}")

def is_valid_email(email: str) -> bool:
    """Return True if email looks like a valid address."""
    if not email:
        return False
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(email_regex, email) is not None
         
# --- Splunk logger ---
def log_event(status: str, message: str, zip_file_name=" "):

    event_data = {
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "transactionId": os.getenv("transactionId", ""),
        "clientName": os.getenv("clientName", ""),
        "clientID": os.getenv("clientID", ""),
        "applicationName": os.getenv("applicationName", ""),
        "order_ids": [],
        "step_function": "compression",
        "component": "glue",
        "job_name": get_job_name(),
        "job_run_id": get_run_id(),
        "compressed_file": zip_file_name,
        "execution_name": os.getenv("execution_name", ""),
        "execution_id": os.getenv("execution_id", ""),
        "execution_starttime": os.getenv("execution_starttime", ""),
        "statemachine_name": os.getenv("statemachine_name", ""),
        "statemachine_id": os.getenv("statemachine_id", ""),
        "state_name": os.getenv("state_name", ""),
        "state_enteredtime": os.getenv("state_enteredtime", ""),
        "status": status,
        "message": message,
        "aws_account_id": AWS_ACCOUNT_ID,
    }

    print(json.dumps(event_data))

    try:
        splunk.log_message(event_data, get_run_id())
    except Exception as e:
        print(f"[ERROR] glue_zip_file_creation : log_event : Failed to write log to Splunk: {str(e)}")

    if status == "FAILED" and is_valid_email(os.getenv("teamsId")):

        glue_link = (
            f"https://us-east-1.console.aws.amazon.com/gluestudio/home?region=us-east-1"
            f"#/editor/job/{event_data['job_name']}/runs"
        )

        stepfn_link = (
            f"https://us-east-1.console.aws.amazon.com/states/home?region=us-east-1"
            f"#/v2/executions/details/{event_data['execution_id']}"
        )

        subject = f"[ALERT] Glue job Failure - {event_data['job_name']} ({status})"

        body_text = (
            f"Glue job failed.\n\n"
            f"Event Time: {event_data['event_time']}\n"
            f"Client: {event_data['clientName']} ({event_data['clientID']})\n"
            f"Job: {event_data['job_name']}\n"
            f"Order Ids: {event_data['order_ids']}\n"
            f"TransactionId: {event_data['transactionId']}\n"
            f"Status: {event_data['status']}\n"
            f"Message: {event_data['message']}\n"
            f"Glue Job: {glue_link}\n"
            f"Step Function Execution: {stepfn_link}\n"
        )

        body_html = f"""<html>
        <body>
          <h3>Glue Job Failure Notification</h3>
          <ul>
            <li><b>Event Time:</b> {event_data['event_time']}</li>
            <li><b>Client:</b> {event_data['clientName']} ({event_data['clientID']})</li>
            <li><b>Job Name:</b> {event_data['job_name']}</li>
            <li><b>Order Ids:</b> {event_data['order_ids']}</li>
            <li><b>TransactionId:</b> {event_data['transactionId']}</li>
            <li><b>Status:</b> {event_data['status']}</li>
            <li><b>Message:</b> {event_data['message']}</li>
          </ul>
          <p><a href="{glue_link}">View Glue Job</a></p>
          <p><a href="{stepfn_link}">View Step Function Execution</a></p>
        </body>
        </html>"""

        sender = os.getenv("ALERT_SENDER_EMAIL", "noreply@broadridge.com")
        ses_client = boto3.client("ses", region_name="us-east-1")

        try:
            response = ses_client.send_email(
                Destination={"ToAddresses": [os.getenv("teamsId")]},
                Message={
                    "Body": {
                        "Html": {"Charset": "UTF-8", "Data": body_html},
                        "Text": {"Charset": "UTF-8", "Data": body_text},
                    },
                    "Subject": {"Charset": "UTF-8", "Data": subject},
                },
                Source=sender,
            )
            print(f"Email sent! sender: {sender}")
            print("SES send_email full response:\n", json.dumps(response, indent=2))
        except ClientError as e:
            print(f"[ERROR] Failed to send email: {str(e)}")

def main():
    # Main method
    try:
        
        set_job_params_as_env_vars()  
        log_event("STARTED", f"glue_zip_file_creation job started processing transactionId : {os.getenv('transactionId', '')}", f"{os.getenv('zipDestination', '')}{os.getenv('zipFileName', '')}")
        
        global input_file_name
        input_file_name = os.getenv('inputFileName')
        
        # Extract the environment variables
        bucket = os.getenv('s3BucketInput')
        s3_bucket_output = os.getenv('s3BucketOutput')
        file_path_list = ast.literal_eval(os.getenv('filePathList'))
        file_pattern_list = ast.literal_eval(os.getenv('filePatternList'))
        zip_destination = os.getenv('zipDestination')
        zip_type = os.getenv('zipType')
        zip_file_name = os.getenv('zipFileName')
        
        # Ensure we have valid paths and patterns
        if file_path_list is None or file_pattern_list is None:
            raise ValueError("Missing file paths or patterns")
       
        # Initialize S3 client
        s3 = boto3.client('s3')

        # Create an in-memory zip file
        zip_buffer = io.BytesIO()
        
        zip_file = zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED)
        
        matched_file_objects = []
        
        # Function to match and zip the files from S3
        def zip_matching_file(bucket, file_path_list, file_pattern_list, zip_file, s3_client):
            matched_file_objects = []
            all_keys = []

            for prefix, pattern in zip(file_path_list, file_pattern_list):
                paginator = s3_client.get_paginator('list_objects_v2')
                files_found = False
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    for file in page.get('Contents', []):
                        filename = file['Key'].split('/')[-1]
                        if re.match(pattern, filename):
                            all_keys.append(file['Key'])
                    if page.get('Contents'):
                        files_found = True
                if not files_found or not all_keys:
                    log_message('failed',f"No file matching pattern {pattern} in {prefix}")
                    raise FileNotFoundError(f"No file matching pattern {pattern} in {prefix}")
                log_event("INFO", f"glue_zip_file_creation : Listing objects in {bucket}/{prefix}", f"{zip_destination}{zip_file_name}")

            # --- Parallel S3 downloads + zip writes ---
            start_time = time.time()
            processed = 0
            total = len(all_keys)

            def _fetch_file(key):
                obj = s3_client.get_object(Bucket=bucket, Key=key)
                return key, obj['Body'].read()

            with ThreadPoolExecutor(max_workers=16) as executor:
                futures = {executor.submit(_fetch_file, k): k for k in all_keys}
                for future in as_completed(futures):
                    key, data = future.result()
                    zip_file.writestr(os.path.basename(key), data)
                    matched_file_objects.append(key)

                    processed += 1
                    if processed % 500 == 0 or processed == total:
                        elapsed = round(time.time() - start_time, 2)
                        log_message("progress", f"Processed {processed}/{total} files in {elapsed}s")

            duration = round(time.time() - start_time, 2)
            log_message("success", f"Added {processed} files to ZIP in {duration} seconds")

            return matched_file_objects

        # Get the files matching the patterns
        try:
            matched_file_objects = zip_matching_file(bucket, file_path_list, file_pattern_list, zip_file, s3)
            
        except FileNotFoundError as e:
            raise FileNotFoundError(e)
    
        # Determine the zip file name
        if zip_type == "UR_ZIP":
            # Determine the final ZIP file name
            tar_files = [f for f in matched_file_objects if f.endswith('.tar')]
            if tar_files:
                file_base_name = os.path.basename(tar_files[0])
        elif zip_type == "NORMAL_ZIP":
            file_base_name = zip_file_name
        else:
            raise ValueError("Files are not in the correct format")
    
        zip_file_name = file_base_name + '.zip'
        
        # Closing the zip file
        zip_file.close()
    
        # Move the buffer position to the beginning
        zip_buffer.seek(0)
    
        # Upload the zip file back to S3
        s3.upload_fileobj(
            Fileobj=zip_buffer,
            Bucket=s3_bucket_output,
            Key=f"{zip_destination}{zip_file_name}"
        )
        
        log_event("ENDED", f"glue_zip_file_creation job created zip file and uploaded to {zip_destination}{zip_file_name}", f"{zip_destination}{zip_file_name}")
        
    except Exception as e:
        log_event("FAILED", f"glue_zip_file_creation : Zip file creation failed: Error: {traceback.format_exc()} : Exception : {str(e)}", f"{os.getenv('zipDestination', '')}{os.getenv('zipFileName', '')}")
        raise e


# Calling the main method
input_file_name=''
if __name__ == '__main__':
    main()
