import json
import boto3
from dpm_splunk_logger_py import splunk
import re
import os
import traceback
# import hashlib
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

        bucket = event.get("bucket", "").strip()
        file_name = event.get("fileName", "").strip()
        copy_info = json.loads(event.get("copyInfo", "[]"))

        if not bucket:
            raise ValueError("Bucket name missing")

        if not file_name:
            raise ValueError("fileName missing")

        log_event("STARTED", f"Lambda rename files job started with event: {event}", context)

        for info in copy_info:
            source_folder = info.get("source_folder", "").strip()
            ext_type = info.get("ext_type", "").strip()
            file_reg = info.get("file_reg", "").strip()
            new_name_prefix = info.get("new_name_prefix", "").strip()
            new_name_date_format = info.get("new_name_date_format", "%Y-%m-%dT%H%M%S")
            new_name_ext = info.get("new_name_ext", "").strip()

            use_utc = info.get("use_utc", True)
            if isinstance(use_utc, str):
                use_utc = use_utc.lower() == "true"

            if not source_folder:
                raise Exception("Missing source_folder in config")

            if file_reg != "input_file":
                log_event("INFO", "Skipping non input_file mode", context)

            now = datetime.now(timezone.utc) if use_utc else datetime.now()
            date_time_suffix = now.strftime(new_name_date_format)

            # Build new filename using provided extension
            new_file_name = f"{new_name_prefix}{date_time_suffix}.{new_name_ext}"

            src_key = f"{source_folder}{file_name}"
            dest_key = f"{source_folder}{new_file_name}"

            log_event("INFO", f"Renaming {src_key} → {dest_key}", context)

            # Backup existing file if present
            try:
                s3.head_object(Bucket=bucket, Key=dest_key)
                dest_key_exists = True
            except ClientError as e:
                dest_key_exists = False
                log_event("INFO", f"{dest_key} doesn't exists,{e.response['Error']['Code']}", context)

            if dest_key_exists:
                base, ext = dest_key.rsplit(".", 1)
                backup_key = f"{base}_{date_time_suffix}_old.{ext}"
                log_event("WARNING", f"Destination exists. Backing up to {backup_key}", context)
                # Copy existing to backup
                s3.copy_object(
                    Bucket=bucket,
                    CopySource={"Bucket": bucket, "Key": dest_key},
                    Key=backup_key
                )
                # Delete existing dest file safely
                s3.delete_object(Bucket=bucket, Key=dest_key)

                s3.copy_object(
                    Bucket=bucket,
                    CopySource={"Bucket": bucket, "Key": src_key},
                    Key=dest_key
                )
            log_event("SUCCESS", f"copy file from {src_key} to {dest_key } completed.", context)
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

