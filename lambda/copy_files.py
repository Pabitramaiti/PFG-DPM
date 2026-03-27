import json
import boto3
import csv
import io
from dpm_splunk_logger_py import splunk
import re
import os
import traceback
import hashlib
from datetime import datetime, timezone
from botocore.exceptions import ClientError

# Initialize S3 client
s3 = boto3.client('s3')
AWS_ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

client_name = None
clientID = None
application_name = None
execution_name = None
execution_id = None
execution_starttime = None
statemachine_name = None
statemachine_id = None
state_name = None
state_enteredtime = None
teamsId = " "


def get_job_name():
    job_name = "copy_files"
    if statemachine_name:
        parts = statemachine_name.split("-")
        if len(parts) >= 3:
            prefix = "_".join(parts[0:3])
            job_name = f"{prefix}_{job_name}"
    return job_name


def get_run_id():
    return f"arn:dpm:glue:{get_job_name()}:{AWS_ACCOUNT_ID}"

def is_valid_email(email: str) -> bool:
    """Return True if email looks like a valid address."""
    if not email:
        return False
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(email_regex, email) is not None

def log_event(status, message, context):

    job_name = "lmbd_copy_files"
    if statemachine_name:
        parts = statemachine_name.split("-")
        if len(parts) >= 3:
            prefix = "_".join(parts[0:3])
            job_name = f"{prefix}_{job_name}"

    event_data = {
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "clientName": client_name,
        "clientID": clientID,
        "applicationName": application_name,
        "step_function": "copy_files",
        "component": "lambda",
        "job_name": get_job_name(),
        "job_run_id": get_run_id(),
        "execution_name": execution_name,
        "execution_id": execution_id,
        "execution_starttime": execution_starttime,
        "statemachine_name": statemachine_name,
        "statemachine_id": statemachine_id,
        "state_name": state_name,
        "state_enteredtime": state_enteredtime,
        "status": status,
        "message": message,
        "aws_account_id": AWS_ACCOUNT_ID,
    }

    print(json.dumps(event_data))

    try:
        splunk.log_message(event_data, context)
    except Exception as e:
        print(f"[ERROR] lmbd_copy_files : log_event : Failed to write log to Splunk: {str(e)}")

    if status == "FAILED" and is_valid_email(teamsId):

        lambda_link = (
            f"https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1"
            f"#/functions/{event_data['job_name']}"
        )

        stepfn_link = (
            f"https://us-east-1.console.aws.amazon.com/states/home?region=us-east-1"
            f"#/v2/executions/details/{event_data['execution_id']}"
        )

        subject = f"[ALERT] Lambda Function Failure - {event_data['job_name']} ({status})"

        body_text = (
            f"Lambda job failed.\n\n"
            f"Event Time: {event_data['event_time']}\n"
            f"Client: {event_data['clientName']} ({event_data['clientID']})\n"
            f"Job: {event_data['job_name']}\n"
            f"Status: {event_data['status']}\n"
            f"Message: {event_data['message']}\n"
            f"Lambda Function: {lambda_link}\n"
            f"Step Function Execution: {stepfn_link}\n"
        )

        body_html = f"""<html>
        <body>
          <h3>Lambda Job Failure Notification</h3>
          <ul>
            <li><b>Event Time:</b> {event_data['event_time']}</li>
            <li><b>Client:</b> {event_data['clientName']} ({event_data['clientID']})</li>
            <li><b>Job Name:</b> {event_data['job_name']}</li>
            <li><b>Status:</b> {event_data['status']}</li>
            <li><b>Message:</b> {event_data['message']}</li>
          </ul>
          <p><a href="{lambda_link}">View Lambda Function</a></p>
          <p><a href="{stepfn_link}">View Step Function Execution</a></p>
        </body>
        </html>"""

        sender = os.getenv("ALERT_SENDER_EMAIL", "noreply@broadridge.com")
        ses_client = boto3.client("ses", region_name="us-east-1")

        try:
            response = ses_client.send_email(
                Destination={"ToAddresses": [teamsId]},
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


def lambda_handler(event, context):
    try:
        if not event:
            raise ValueError("Event data is missing")

        log_event("STARTED", f"lmbd_copy_files job started with event: {event}", context)

        global client_name, clientID, application_name, execution_name, execution_id, execution_starttime
        global statemachine_name, statemachine_id, state_name, state_enteredtime, teamsId

        client_name = event.get("clientName", " ")
        clientID = event.get("clientID", " ")
        application_name = event.get("applicationName", " ")
        execution_name = event.get("execution_name", " ")
        execution_id = event.get("execution_id", "")
        execution_starttime = event.get("execution_starttime", " ")
        statemachine_name = event.get("statemachine_name", " ")
        statemachine_id = event.get("statemachine_id", " ")
        state_name = event.get("state_name", " ")
        state_enteredtime = event.get("state_enteredtime", " ")
        teamsId = event.get("teamsId", "")

        bucket = event.get("bucket", " ")
        objectKey = event.get("objectKey", " ")
        fileId = event.get("fileId", " ")
        key = event.get("key", " ")
        file_name = event.get("fileName", " ")
        sfInputFile = event.get("sfInputFile", " ")
        copyInfo = json.loads(event.get("copyInfo", []))

        # --- MAIN COPY/MOVE LOGIC ---
        for info in copyInfo:
            source_folder = info.get("source_folder", "").strip()
            destination_folder = info.get("destination_folder", "").strip()
            copy_type = info.get("copy_type", "copy").lower().strip()
            ext_type = info.get("ext_type", "").strip()
            file_reg = info.get("file_reg", "").strip()

            if not source_folder or not destination_folder:
                log_event("WARNING", f"Missing folder info in copyInfo: {info}", context)
                continue

            # Determine what files to look for
            matching_keys = []

            # If file_reg == 'input_file', match file_name + "." + ext_type
            if file_reg == "input_file" and file_name:
                if ext_type:
                    expected_key = f"{source_folder}{file_name}.{ext_type}"
                else:
                    expected_key = f"{source_folder}{file_name}"

                # check if exists
                try:
                    s3.head_object(Bucket=bucket, Key=expected_key)
                    matching_keys.append(expected_key)
                except ClientError:
                    log_event("INFO", f"File not found: {expected_key}, skipping.", context)

            # If file_reg seems like a regex, match all objects whose Key matches
            elif file_reg and file_reg != "input_file":
                regex_pattern = re.compile(file_reg)
                paginator = s3.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=bucket, Prefix=source_folder):
                    for obj in page.get('Contents', []):
                        if regex_pattern.search(obj['Key']):
                            matching_keys.append(obj['Key'])

            # If file_reg empty, just check if file_name exists in source folder
            else:
                if file_name:
                    expected_key = f"{source_folder}{file_name}"
                    try:
                        s3.head_object(Bucket=bucket, Key=expected_key)
                        matching_keys.append(expected_key)
                    except ClientError:
                        log_event("INFO", f"No file found for {expected_key}, skipping.", context)
                else:
                    log_event("INFO", "No file_name provided, skipping copy/move.", context)
                    continue

            log_event("INFO", f"Found {(matching_keys)} matching files for config: {info}", context)
            if not matching_keys:
                log_event("INFO", f"No matching files found for config: {info}", context)
                continue

            # --- Perform copy/move ---
            for src_key in matching_keys:
                dest_key = src_key.replace(source_folder, destination_folder, 1)
                try:
                    s3.copy_object(
                        Bucket=bucket,
                        CopySource=f"{bucket}/{src_key}",
                        Key=dest_key
                    )
                    log_event("INFO", f"Copied {src_key} to {dest_key}", context)

                    # If it's a move, delete the source
                    if copy_type == "move":
                        s3.delete_object(Bucket=bucket, Key=src_key)
                        log_event("INFO", f"Moved (deleted source) {src_key}", context)

                except Exception as e:
                    log_event("FAILED", f"Error copying {src_key} to {dest_key}: {str(e)}", context)

        log_event("SUCCESS", "All requested copy/move operations completed.", context)
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Copy/move completed successfully"})
        }

    except Exception as e:
        error_message = f"Lambda execution failed: Exception: {str(e)}\nTraceback: {traceback.format_exc()}"
        log_event("FAILED", error_message, context)
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(e)})
        }