# glue_multifile_validation_report
# Creates and emails the multifile validation report that contains a list of all AUX file recieved and required for processing

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
    return f"arn:dpm:glue:glue_multifile_validation_report:" + account_id


def is_valid_email(email: str) -> bool:
    """Return True if email looks like a valid address."""
    if not email:
        return False
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(email_regex, email) is not None


# --- Splunk logger ---
def log_event(status: str, message: str, input_file: str):
    job_name = "glue_multifile_validation_report"
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
        print(f"[ERROR] glue_multifile_validation_report : log_event : Failed to write log to Splunk: {str(e)}")

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

def validate_required_config(config, file_name):
    """Fail fast on required vs optional keys"""
    required = ["delimiter", "header_delimiter", "header_record"]
    for k in required:
        if not config.get(k):
            msg = f"Missing required config key: {k}"
            raise ValueError(msg)

    # Optional sanity: ensure lists if provided
    for list_key in ["header_identifiers", "trailer_identifiers"]:
        if config.get(list_key) and not isinstance(config[list_key], list):
            raise ValueError(f"{list_key} must be a list")

    log_event("INFO",
              f"Config validated successfully, delimiter={config.get('delimiter')}, header_delimiter={config.get('header_delimiter')}, header_record={config.get('header_record')}, header_identifiers={config.get('header_identifiers')}, trailer_identifiers={config.get('trailer_identifiers')}",
              file_name)


# --- Utility: safe splitter ---
def safe_split(line: str, delimiter):
    """
    Split a line safely:
    - If delimiter is string: normal split
    - If delimiter is dict with 'fixed_ranges': explicit ranges (strict validation)
    config = {
        "delimiter": {
            "fixed_ranges": [
                [0, 8],
                [15, 20],
                [30, 150]
            ]
        }
    }
    """
    line = line.rstrip("\n\r")

    # Case 1: delimiter missing ? whole line
    if not delimiter:
        return [line]

    # Case 2: explicit fixed ranges
    if isinstance(delimiter, dict) and "fixed_ranges" in delimiter:
        ranges = delimiter["fixed_ranges"]
        fields = []
        for start, end in ranges:
            # Validate start and end
            if not (isinstance(start, int) and isinstance(end, int)):
                raise ValueError(f"Invalid range [{start}:{end}] � start and end must be integers")
            if start < 0 or end < 0:
                raise ValueError(f"Invalid range [{start}:{end}] � indices must be non-negative")
            if start >= end:
                raise ValueError(f"Invalid range [{start}:{end}] � start must be less than end")
            if end > len(line):
                raise ValueError(
                    f"Configured range [{start}:{end}] exceeds line length {len(line)} :: line='{line}'"
                )
            fields.append(line[start:end])
        return fields

    # Case 3: simple fixed widths (contiguous)
    if isinstance(delimiter, dict) and "fixed_widths" in delimiter:
        widths = delimiter["fixed_widths"]
        fields, cursor = [], 0
        for w in widths:
            if not isinstance(w, int) or w <= 0:
                raise ValueError(f"Invalid fixed width '{w}' � must be a positive integer")
            if cursor + w > len(line):
                raise ValueError(
                    f"Configured fixed width '{w}' at position {cursor} exceeds line length {len(line)} :: line='{line}'"
                )
            fields.append(line[cursor:cursor + w])
            cursor += w
        return fields

    # Case 4: variable ranges (allow 'end' for last field)
    if isinstance(delimiter, dict) and "variable_ranges" in delimiter:
        ranges = delimiter["variable_ranges"]
        fields = []
        line_len = len(line)
        for start, end in ranges:
            if end == "end":
                end = line_len
            if not (isinstance(start, int) and isinstance(end, int)):
                raise ValueError(f"Invalid variable range [{start}:{end}] � must be int or 'end'")
            if start < 0 or end < 0:
                raise ValueError(f"Invalid range [{start}:{end}] for line length {line_len}")
            # If start==end, treat as empty field instead of failure
            if start == end:
                fields.append('')
            else:
                if start > line_len:
                    # start is beyond line length, just pad empty too
                    fields.append('')
                else:
                    end = min(end, line_len)
                    fields.append(line[start:end])
        return fields

    # Case 5: special_config (custom business logic, fully configurable)
    if isinstance(delimiter, dict) and "special_config" in delimiter:
        cfg = delimiter.get("special_config", {})

        # --- CONFIG PARAMETERS ---
        # Valid firm IDs to match
        valid_firms = cfg.get("firm_ids", ["001"])
        # Mapping for prefixes and corresponding message definitions
        prefix_map = cfg.get("prefix_map", {})
        # Sub firm id position (start, end)
        sub_firm_range = tuple(cfg.get("sub_firm_range", (2, 5)))

        # --- Step 1: Sub-firm validation ---
        start, end = sub_firm_range
        sub_firm_id = line[start:end] if len(line) >= end else ""
        if sub_firm_id not in valid_firms:
            return []  # firm doesn't match, nothing to process

        # --- Step 2: Prefix check and extraction ---
        line_len = len(line)
        prefix = line[:2]
        if prefix not in prefix_map:
            return []

        s2, span = prefix_map[prefix]
        start_pos, end_pos = span
        s1 = line[start_pos:end_pos] if end_pos <= line_len else line[start_pos:]

        # --- Step 3: Construct s3 list dynamically ---
        parts = line.split()
        if not parts or len(parts) <= 1:
            s3_list = []
        else:
            payload_parts = parts[1:]  # skip header part
            s3_list = []
            for p in payload_parts:
                fourth_char = p[0] if p else ""
                # Format: L + prefix + <fourth_char> + <last char of firm id>
                code = f"L{prefix}{fourth_char}{sub_firm_id[-1]}"
                s3_list.append(code)

        return [s1, s2, sub_firm_id, s3_list]

    # Case 6: string delimiter is only whitespace
    if isinstance(delimiter, str) and delimiter.strip() == "":
        return re.split(r"\s+", line.strip())

    # Case 7: normal string delimiter
    return line.split(delimiter)

def get_json(json_str):
    return json.loads(json_str)

def generate_report_header(configs):
    # Generate report title:
    report_title_underline = "=" * len(configs.get('title'))
    report_title = f"{configs.get('title')}\n{report_title_underline}\n\n"
    # Generate report header with run date, time, and body text:
    return (
        f"RUN DATE:{configs.get('run_date')}    TIME:{configs.get('run_time')}\n"
        f"{report_title}"
        f"{configs.get('body_text')}"
        f"FILES RECEIVED\n========================================\n"
    )

def get_files_from_s3(bucket, key):
    # Get the input file from the S3 bucket:
    s3_client = boto3.client('s3')
    input_file = s3_client.get_object(Bucket=bucket,Key=key)
    # Get the contents of the report and read the values (in csv format):
    input_report = input_file['Body'].read().decode('utf-8')
    rows = input_report.split("\n")
    file_list = []
    # Get the index of the filename field from the header row:
    filename = rows[0].split(",").index('"filename"')
    rows.pop(0)
    # For each row, excluding the header, get the filename:
    for row in rows:
        values = row.split(",")
        file_list.append(values[filename].replace('"', '')) # remove unnecessary quotation marks before appending
    return file_list

def add_files_to_report(report, file_list):
    for file in file_list:
        report += f"{file}\n"
    return report

def email_report(report, configs):
    # Get the email info from the configs and send the email using the report as the body:
    if is_valid_email(configs.get('email_to_addr')):
        subject = f"{configs.get('email_subject')}"
        sender = "noreply@broadridge.com"
        try:
            ses_client = boto3.client("ses", region_name="us-east-1")   # use Simple Email Service to send email
            response = ses_client.send_email(
                Destination={"ToAddresses": [configs.get('email_to_addr')]},
                Message={
                    "Body": {
                        "Text": {"Charset": "UTF-8", "Data": report},
                    },
                    "Subject": {"Charset": "UTF-8", "Data": subject},
                },
                Source=sender,
            )
            print(f"Email sent! MessageId: {response['MessageId']}")
        except ClientError as e:
            print(f"[ERROR] Failed to send email::Error={traceback.format_exc()}::Exception Msg={e}")
            
def archive_report(report, bucket, key, filename):
    # Create a new file containing the report and put into the output folder:
    output_filename = re.sub("csv", "txt", filename)
    output_key = key + output_filename
    s3_resource = boto3.resource("s3")
    s3_resource.Object(bucket, output_key).put(Body=report)

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

    log_event("INFO", f"glue_multifile_validation_report::Environment variables set from job parameters: {', '.join(messages)}",
              os.getenv('s3_input_file'))


def main():
    file_name = None
    try:
        # Set environment variables:
        set_job_params_as_env_vars()
        input_file = os.getenv("inputFileName")
        bucket = os.getenv("bucket")
        input_folder = os.getenv("inputFolder")
        output_folder = os.getenv("outputFolder")
        output_file = os.getenv("s3_output_file")
        email_configs = os.getenv("email_config_str")
        
        # Generate the report header info from the configs:
        cfg = get_json(email_configs)
        report_header = generate_report_header(cfg)
        
        # Get and add the filenames to the report:
        input_key = input_folder + input_file
        file_list = get_files_from_s3(bucket, input_key)
        multifile_validation_report = add_files_to_report(report_header, file_list)
        
        # Email the report using info from the configs, then archive:
        email_report(multifile_validation_report, cfg)
        archive_report(multifile_validation_report, bucket, output_folder, input_file)

    except Exception as e:
        log_event("FAILED", f"Fatal error: {traceback.format_exc()}::{e}", file_name or "N/A")
        raise


if __name__ == "__main__":
    main()