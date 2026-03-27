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
    if not email:
        return False
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(email_regex, email) is not None


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


def validate_required_config(config, file_name):
    required = ["delimiter", "header_delimiter", "header_record"]
    for k in required:
        if not config.get(k):
            msg = f"Missing required config key: {k}"
            raise ValueError(msg)
    for list_key in ["header_identifiers", "trailer_identifiers"]:
        if config.get(list_key) and not isinstance(config[list_key], list):
            raise ValueError(f"{list_key} must be a list")
    log_event("INFO",
              f"Config validated successfully, delimiter={config.get('delimiter')}, header_delimiter={config.get('header_delimiter')}, header_record={config.get('header_record')}, header_identifiers={config.get('header_identifiers')}, trailer_identifiers={config.get('trailer_identifiers')}",
              file_name)


def safe_split(line: str, delimiter):
    line = line.rstrip("\n\r")
    if not delimiter:
        return [line]
    if isinstance(delimiter, dict) and "fixed_ranges" in delimiter:
        ranges = delimiter["fixed_ranges"]
        fields = []
        for start, end in ranges:
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
            if start == end:
                fields.append('')
            else:
                if start > line_len:
                    fields.append('')
                else:
                    end = min(end, line_len)
                    fields.append(line[start:end])
        return fields
    if isinstance(delimiter, dict) and "special_config" in delimiter:
        cfg = delimiter.get("special_config", {})
        valid_firms = cfg.get("firm_ids", ["001"])
        prefix_map = cfg.get("prefix_map", {})
        sub_firm_range = tuple(cfg.get("sub_firm_range", (2, 5)))
        start, end = sub_firm_range
        sub_firm_id = line[start:end] if len(line) >= end else ""
        if sub_firm_id not in valid_firms:
            return []
        line_len = len(line)
        prefix = line[:2]
        if prefix not in prefix_map:
            return []
        s2, span = prefix_map[prefix]
        start_pos, end_pos = span
        s1 = line[start_pos:end_pos] if end_pos <= line_len else line[start_pos:]
        parts = line.split()
        if not parts or len(parts) <= 1:
            s3_list = []
        else:
            payload_parts = parts[1:]
            s3_list = []
            for p in payload_parts:
                fourth_char = p[0] if p else ""
                code = f"L{prefix}{fourth_char}{sub_firm_id[-1]}"
                s3_list.append(code)
        return [s1, s2, sub_firm_id, s3_list]
    if isinstance(delimiter, str) and delimiter.strip() == "":
        return re.split(r"\s+", line.strip())
    return line.split(delimiter)


def is_header(line, config):
    ids = config.get("header_identifiers")
    delim = config.get("header_delimiter") or config.get("delimiter")
    if ids:
        for ident in ids:
            if line.startswith(ident) and safe_split(line, delim)[0] == ident:
                return True
    return False


def is_trailer(line, config):
    trailers = config.get("trailer_identifiers")
    delimiter = config.get("trailer_delimiter") or config.get("delimiter")
    position = 1
    try:
        position = int(config.get("trailer_position"))
    except:
        position = 1
    if trailers:
        for trailer in trailers:
            if trailer in line:
                field_value = safe_split(line.strip(), delimiter)[position-1]
                return trailer in field_value
    return False


def convert_file_to_json(input_bucket, input_file, config, file_name, output_bucket=None, output_file=None):
    """
    Convert input file lines into JSON objects and stream output to S3 if output_bucket/output_file provided.
    Output format:
    {
        "json_data": [ {record1}, {record2}, ... ]
    }
    """
    delimiter = config.get("delimiter")
    header_delimiter = config.get("header_delimiter")
    header_record = config.get("header_record")
    file_path = f"s3://{input_bucket}/{input_file}"

    header_fields = [
        header_field.strip().strip('"')
        for header_field in safe_split(header_record.strip(), header_delimiter or delimiter)
    ]
    column_count = len(header_fields)
    log_event(
        "INFO",
        f"header_fields={header_fields}, delimiter={delimiter}, "
        f"header_delimiter={header_delimiter}, column_count={column_count}",
        file_name
    )

    line_number = 0
    record_count = 0

    output_stream = None
    if output_bucket and output_file:
        output_path = f"s3://{output_bucket}/{output_file}"
        output_stream = sfs.open(output_path, "w")
        output_stream.write('{"json_data": [\n')

    with sfs.open(file_path, 'r', encoding='windows-1252') as f:
        for raw in f:
            line_number += 1
            line = raw.strip()
            if not line:
                continue
            if is_header(line, config) or is_trailer(line, config):
                continue

            values = [
                str(value).strip().strip('"')
                for value in safe_split(line, delimiter)
            ]

            if config.get("skip_empty_line", False):
                if not values or all(v == '' for v in values):
                    continue

            expected_cols = len(header_fields)
            if len(values) != expected_cols:
                allow_varlen = config.get("allow_variable_length", False)
                if len(values) < expected_cols and allow_varlen:
                    missing = expected_cols - len(values)
                    values.extend([''] * missing)
                else:
                    msg = f"Line {line_number}: Column mismatch (expected={expected_cols}, got={len(values)}) :: line={line}"
                    raise Exception(msg)

            record = dict(zip(header_fields, values))

            if isinstance(record.get("MESSAGE_KEY"), str):
                try:
                    if record["MESSAGE_KEY"].startswith("[") and record["MESSAGE_KEY"].endswith("]"):
                        record["MESSAGE_KEY"] = ast.literal_eval(record["MESSAGE_KEY"])
                except Exception:
                    pass

            if output_stream:
                if record_count > 0:
                    output_stream.write(",\n")
                output_stream.write(json.dumps(record))
            record_count += 1

            if record_count % 100000 == 0:
                log_event("INFO", f"Processed {record_count} data records so far...", file_name)

    if output_stream:
        output_stream.write("\n]}")
        output_stream.close()
        log_event("INFO", f"Conversion complete: total_lines={line_number}, total_records={record_count}", file_name)
        return None
    else:
        # Fallback: not recommended for large files
        return {"json_data": []}


def write_output_file(output_bucket, output_file, json_result, file_name):
    output_path = f"s3://{output_bucket}/{output_file}"
    log_event("INFO", f"Writing combined JSON output to {output_path}", file_name)
    with sfs.open(output_path, "w") as out:
        out.write(json.dumps(json_result, indent=2))
    log_event("INFO", f"JSON output written to {output_path}", file_name)


def get_json(json_str, input_file_name):
    j = json.loads(json_str)
    for key in j.keys():
        if key in input_file_name:
            return j.get(key)
    raise ValueError(f"No matching config key for {input_file_name}")


def set_job_params_as_env_vars():
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

        cfg = get_json(config_str, file_name)
        log_event("INFO", f"Configuration loaded: {cfg}", file_name)

        validate_required_config(cfg, file_name)

        # Refactored: stream output directly to S3
        convert_file_to_json(input_bucket, input_file, cfg, file_name, output_bucket, output_file)

        log_event("ENDED", f"Successfully converted {file_name}, outfile={output_file}", file_name)

    except Exception as e:
        log_event("FAILED", f"Fatal error: {traceback.format_exc()}::{e}", file_name or "N/A")
        raise


if __name__ == "__main__":
    main()
