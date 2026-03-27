import os
import sys
import boto3
import json
import splunk
import io
import re
import traceback
from datetime import datetime, timezone
from botocore.exceptions import BotoCoreError, ClientError
from s3fs import S3FileSystem

job_start_time = datetime.now(timezone.utc)
sfs = S3FileSystem()
inputfile = None

def get_run_id():
    try:
        account_id = boto3.client("sts").get_caller_identity()["Account"]
        return f"arn:dpm:glue:columnervalidator:{account_id}"
    except Exception as e:
        print("[ERROR] Failed to get run id/account:", e)
        raise e

def is_valid_email(email: str) -> bool:
    """Return True if email looks like a valid address."""
    if not email:
        return False
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(email_regex, email) is not None

# --- Splunk logger ---
def log_event(status: str, message: str):

    job_name = "glue_columnervalidator"
    statemachine_name = os.getenv("statemachine_name")
    if statemachine_name:
        parts = statemachine_name.split("-")
        if len(parts) >= 3:
            prefix = "_".join(parts[0:3])
            job_name = f"{prefix}_{job_name}"

    event_data = {
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "file_name": inputfile,
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
        print(f"[ERROR] glue_columnervalidator : log_event : Failed to write log to Splunk: {str(e)}")

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

def validate_file_type(file_type):
    if file_type not in ("driver", "aux"):
        raise ValueError(f"File type {file_type} unrecognized. Must be 'driver' or 'aux'.")

def process_and_validate_s3_file(s3_uri, client_rec_type, header_configs, trailer_configs, allowEmptyLines, minimumRecordLength):
    """
    Reads S3 file line-by-line, validates header & trailer without loading entire file into memory.
    Also validates:
        - Empty line behavior based on `allowEmptyLines`
        - Each line must be at least `minimumRecordLength`

    Returns (total_count, client_count)
    """
    bucket, key = s3_uri.replace("s3://", "", 1).split("/", 1)

    try:
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucket, Key=key)
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(f"Could not retrieve S3 file {s3_uri}: {e}") from e

    total_count = 0
    client_count = 0

    # Pre-extract header/trailer config details
    header_expected = header_configs['recType']['value'] if header_configs['present'] else []
    header_start = header_configs['recType']['startPosition'] if header_configs['present'] else None
    header_end = (header_configs['recType']['endPosition'] + 1) if header_configs['present'] else None
    header_required_count = int(header_configs.get('recordCount', 0)) if header_configs['present'] else 0
    header_found = []

    trailer_expected = trailer_configs['recType']['value'] if trailer_configs['present'] else []
    trailer_start = trailer_configs['recType']['startPosition'] if trailer_configs['present'] else None
    trailer_end = (trailer_configs['recType']['endPosition'] + 1) if trailer_configs['present'] else None
    trailer_required_count = int(trailer_configs.get('recordCount', 0)) if trailer_configs['present'] else 0
    trailer_found = []

    with io.TextIOWrapper(obj['Body']) as f:
        for line_num, raw_line in enumerate(f, start=1):
            stripped_line = raw_line.rstrip("\n")

            # --- Empty line validation ---
            if not stripped_line.strip():
                if allowEmptyLines:
                    continue  # just skip it
                else:
                    raise ValueError(
                        f"Line {line_num}: Empty line encountered but allowEmptyLines=False"
                    )

            # --- Minimum record length validation ---
            if len(stripped_line) < minimumRecordLength:
                raise ValueError(
                    f"Line {line_num}: Record too short "
                    f"(length={len(stripped_line)}, required={minimumRecordLength})"
                )

            total_count += 1
            line = raw_line.rstrip("\n")

            # Count client-specific records
            if line.startswith(client_rec_type):
                client_count += 1

            # Detect header
            if header_configs['present']:
                rec_code = line[header_start:header_end]
                if rec_code in header_expected:
                    header_found.append(rec_code)

            # Detect trailer
            if trailer_configs['present']:
                rec_code = line[trailer_start:trailer_end]
                if rec_code in trailer_expected:
                    trailer_found.append(rec_code)

    # --- Header Validation ---
    if header_configs['present']:
        if header_required_count and len(header_found) != header_required_count:
            raise ValueError(f"Header Validation: Found {len(header_found)}, expected {header_required_count}.")
        if set(header_found) != set(header_expected):
            raise ValueError(f"Header Validation mismatch. Found {set(header_found)}, expected {set(header_expected)}.")

    # --- Trailer Validation ---
    if trailer_configs['present']:
        if trailer_required_count and len(trailer_found) != trailer_required_count:
            raise ValueError(f"Trailer Validation: Found {len(trailer_found)}, expected {trailer_required_count}.")
        if set(trailer_found) != set(trailer_expected):
            raise ValueError(f"Trailer Validation mismatch. Found {set(trailer_found)}, expected {set(trailer_expected)}.")

    return total_count, client_count

def send_params(configs, s3_uri, record_bucket, file_name, output_path, rec_counts, is_valid):
    try:
        output_file_uri = f"s3://{record_bucket}/{output_path}/{file_name}.json"
        rec_counts_total, rec_counts_client = rec_counts
        status = "SUCCESSFUL" if is_valid else "UNSUCCESSFUL"

        params = {
            "body": {
                "STEP": "VALIDATION",
                "FILE_TYPE": configs['fileType'],
                "TOTAL_RECORDS_COUNT": str(rec_counts_total),
                "STATEMENT_RECORDS_COUNT": str(rec_counts_client),
                "STATUS": status,
                "S3_URI": s3_uri
            }
        }

        # Write output file
        write_output_file(json.dumps(params, indent=2), output_file_uri)

    except Exception as e:
        raise Exception(f"send_params failed: {e}") from e

def write_output_file(params_json, output_file_uri):
    with sfs.open(output_file_uri, 'w') as out_file:
        out_file.write(params_json)

def validate(s3_uri, configs):
    validate_file_type(configs['fileType'])
    client_rec_type = configs.get("statementRecType", "")
    header_configs = configs['header']
    trailer_configs = configs['trailer']
    allowEmptyLines = configs.get("allowEmptyLines", True)
    minimumRecordLength = configs.get("minimumRecordLength", 21)

    try:
        total_count, client_count = process_and_validate_s3_file(
            s3_uri, client_rec_type, header_configs, trailer_configs, allowEmptyLines, minimumRecordLength
        )
    except Exception as e:
        raise e
    else:
        return (total_count, client_count), True

def set_job_params_as_env_vars():
    messages = []
    messages.append(f"Set environment variable:")
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i+1]
            os.environ[key] = value
            messages.append(f"{key}={value}")

    log_event("INFO", f"glue_columnervalidator::Environment variables set from job parameters: {', '.join(messages)}")

def main():
    try:
        # --- Setup job parameters from Step Functions ---
        set_job_params_as_env_vars()

        # Validate required environment variables
        required_vars = ['client_config', 's3_bucket_input', 's3_key_input', 's3_output_path']
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

        configs = json.loads(os.getenv('client_config'))
        record_bucket = os.getenv('s3_bucket_input')
        record_key = os.getenv('s3_key_input')
        output_path = os.getenv('s3_output_path')

        if not record_key:
            raise ValueError("s3_key_input is empty or None")

        s3_uri = f"s3://{record_bucket}/{record_key}"
        file_name = record_key.rsplit("/", 1)[-1]

        # Set global input file name for logging
        global inputfile
        inputfile = file_name

        # STARTED log to Splunk
        log_event("STARTED", f"Glue job started processing file {file_name}")

    except Exception as e:
        # Parameter setup failure
        raise Exception(f"Parameter setup failed: {e}") from e

    try:
        # --- Run validation ---
        valid_rec_counts, is_valid = validate(s3_uri, configs)

        # Send results back to S3
        send_params(configs, s3_uri, record_bucket, file_name, output_path, valid_rec_counts, is_valid)

    except Exception as e:
        # Validation step failure
        log_event("FAILED", f"glue_columnervalidator::Fatal Error={traceback.format_exc()}::Exception::{e}")
        raise

    else:
        # If all good, log ENDED with record counts
        rec_counts_total, rec_counts_client = valid_rec_counts
        log_event("ENDED", f"Glue job completed successfully. Records total={rec_counts_total}, client={rec_counts_client}")

if __name__ == '__main__':
    main()