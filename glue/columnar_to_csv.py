import sys
import boto3
from s3fs import S3FileSystem
from awsglue.utils import getResolvedOptions
import re
import json
import traceback
import os
from datetime import datetime, timezone
from botocore.exceptions import ClientError
import splunk
import ast

# --- S3 filesystem ---
sfs = S3FileSystem()


def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return f"arn:dpm:glue:glue_positional_to_json:" + account_id


def is_valid_email(email: str) -> bool:
    """Return True if email looks like a valid address."""
    if not email:
        return False
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(email_regex, email) is not None


# --- Splunk logger ---
def log_event(status: str, message: str, input_file: str):
    job_name = "glue_positional_to_json"
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
    except Exception as e:
        print(f"[ERROR] glue_positional_to_json : log_event : Failed to write log to Splunk: {str(e)}")

    if status == "FAILED" and is_valid_email(os.getenv("teamsId")):

        glue_link = (
            f"https://us-east-1.console.aws.amazon.com/gluestudio/home?region=us-east-1"
            f"#/editor/job/{event_data.get('job_name')}/runs"
        )

        stepfn_link = (
            f"https://us-east-1.console.aws.amazon.com/states/home?region=us-east-1"
            f"#/v2/executions/details/{event_data.get('execution_id')}"
        )

        subject = f"[ALERT] Glue job Failure - {event_data.get('job_name')} ({status})"

        body_text = (
            f"Glue job failed.\n\n"
            f"Event Time: {event_data.get('event_time')}\n"
            f"Client: {event_data.get('clientName')} ({event_data.get('clientID')})\n"
            f"Job: {event_data.get('job_name')}\n"
            f"Input File: {event_data.get('file_name')}\n"
            f"Status: {event_data.get('status')}\n"
            f"Message: {event_data.get('message')}\n"
            f"Glue Job: {glue_link}\n"
            f"Step Function Execution: {stepfn_link}\n"
        )

        body_html = f"""<html>
        <body>
          <h3>Glue Job Failure Notification</h3>
          <ul>
            <li><b>Event Time:</b> {event_data.get('event_time')}</li>
            <li><b>Client:</b> {event_data.get('clientName')} ({event_data.get('clientID')})</li>
            <li><b>Job Name:</b> {event_data.get('job_name')}</li>
            <li><b>Input File:</b> {event_data.get('file_name')}</li>
            <li><b>Status:</b> {event_data.get('status')}</li>
            <li><b>Message:</b> {event_data.get('message')}</li>
          </ul>
          <p><a href="{glue_link}">View Glue Job</a></p>
          <p><a href="{stepfn_link}">View Step Function Execution</a></p>
        </body>
        </html>"""

        sender = os.getenv("ALERT_SENDER_EMAIL", "noreply@broadridge.com")
        try:
            ses_client = boto3.client("ses", region_name="us-east-1")
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
            print(f"Email sent! MessageId: {response['MessageId']}")
        except ClientError as e:
            print(f"[ERROR] Failed to send email::Error={traceback.format_exc()}::Exception Msg={e}")


def is_trailer(line, config):
    ids = config.get("trailer_identifiers")
    delim = config.get("trailer_delimiter") or config.get("delimiter")
    if ids:
        for ident in ids:
            if line.startswith(ident) and safe_split(line, delim)[0] == ident:
                return True
    return False


def set_job_params_as_env_vars():
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    messages = []
    messages.append(f"Set environment variable:")
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i + 1]
            os.environ[key] = value
            messages.append(f"{key}={value}")

    log_event("INFO", f"glue_positional_to_json::Environment variables set from job parameters: {', '.join(messages)}",
              os.getenv('s3_input_file'))


def get_csv_output_path(input_s3_path, output_bucket):
    # Extract file name and replace extension
    file_name = os.path.basename(input_s3_path)
    base_name, ext = os.path.splitext(file_name)
    new_file_name = base_name + ".csv"
    # Optionally, preserve folder structure if needed
    output_s3_path = f"s3://{output_bucket}/{new_file_name}"
    return output_s3_path


def process_columnar_file(input_s3_path, output_s3_path, delimiter=" "):
    sfs = S3FileSystem()
    matched_codes = set()

    # Read input file
    with sfs.open(input_s3_path, "r") as infile:
        for line in infile:
            record = line.strip()

            if len(record) <= 4:
                continue

            if record[0:3] == "LGL" and record[4] in ("1", "9"):
                matched_codes.add(record[0:5])

    # Write output file
    with sfs.open(output_s3_path, "w") as outfile:
        outfile.write("GLOBAL_IND\n")
        for code in sorted(matched_codes):
            outfile.write(f"{code}\n")

def get_json(json_str, input_file_name):
    j = json.loads(json_str)
    for key in j.keys():
        if key in input_file_name:
            return j.get(key)
    raise ValueError(f"No matching config key for {input_file_name}")


def main():
    file_name = None
    try:
        set_job_params_as_env_vars()
        input_file = os.getenv("s3_input_file")
        input_bucket = os.getenv("s3_bucket_input")
        output_bucket = os.getenv("s3_bucket_output")
        output_file = os.getenv("s3_output_file")
        config_str = os.getenv("config_str")

        file_name = input_file.split("/")[-1]
        log_event("STARTED",
                  f"Started processing {file_name}, infile={input_file}, outfile={output_file}, config={config_str}",
                  file_name)

#####        cfg = get_json(config_str, file_name)
#####        log_event("INFO", f"Configuration loaded: {cfg}", file_name)

####        output_file = get_csv_output_path(input_file, output_bucket)
        process_columnar_file(input_file, output_file, delimiter=" ")
        log_event("ENDED", f"Successfully converted {file_name}, outfile={output_file}", file_name)

    except Exception as e:
        log_event("FAILED", f"Fatal error: {traceback.format_exc()}::{e}", file_name or "N/A")
        raise


if __name__ == "__main__":
    main()
