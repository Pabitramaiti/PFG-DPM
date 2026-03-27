import sys
import boto3
from s3fs import S3FileSystem
from awsglue.utils import getResolvedOptions
import re
import json
import traceback
import os
from datetime import datetime, timezone
import pytz
from botocore.exceptions import ClientError
import splunk

# --- S3 filesystem ---
sfs = S3FileSystem()


def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return f"arn:dpm:glue:glue_positional_validation:" + account_id


def is_valid_email(email: str) -> bool:
    """Return True if email looks like a valid address."""
    if not email:
        return False
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(email_regex, email) is not None


# --- Splunk logger (unchanged) ---
def log_event(status: str, message: str, input_file: str):
    job_name = "glue_positional_validation"
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
        print("[ERROR] glue_positional_validation : log_event : Failed to write log to Splunk")

    if status == "FAILED" and is_valid_email(os.getenv("teamsId")):
        glue_link = f"https://us-east-1.console.aws.amazon.com/gluestudio/home?region=us-east-1#/editor/job/{event_data.get('job_name')}/runs"
        stepfn_link = f"https://us-east-1.console.aws.amazon.com/states/home?region=us-east-1#/v2/executions/details/{event_data.get('execution_id')}"
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
            ses_client.send_email(
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
        except ClientError:
            print("[ERROR] Failed to send alert email")


# --- Early validation of config ---
def validate_required_config(config, file_name):
    """
    Required: delimiter, header_delimiter, header_record, return_report_layout
    Optional: header_identifiers, trailer_delimiter, trailer_identifiers, trailer_record_count_index
    """
    required_keys = ["delimiter", "header_delimiter", "header_record", "return_report_layout"]
    for k in required_keys:
        if not config.get(k):
            msg = f"Missing required config key '{k}'"
            log_event("FAILED", msg, file_name)
            raise ValueError(msg)

    # Validate optionals if present
    if config.get("trailer_record_count_index") and not str(config.get("trailer_record_count_index")).isdigit():
        raise ValueError(f"Invalid trailer_record_count_index: {config.get('trailer_record_count_index')}")

    if config.get("header_identifiers") and not isinstance(config.get("header_identifiers"), list):
        raise ValueError("header_identifiers must be a list if provided")

    if config.get("trailer_identifiers") and not isinstance(config.get("trailer_identifiers"), list):
        raise ValueError("trailer_identifiers must be a list if provided")

    log_event("INFO", "Config validation passed (required + optional)", file_name)


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

    # Case 1: delimiter missing → whole line
    if not delimiter:
        return [line]

    # Case 2: explicit fixed ranges
    if isinstance(delimiter, dict) and "fixed_ranges" in delimiter:
        ranges = delimiter["fixed_ranges"]
        fields = []
        for start, end in ranges:
            # Validate start and end
            if not (isinstance(start, int) and isinstance(end, int)):
                raise ValueError(f"Invalid range [{start}:{end}] — start and end must be integers")
            if start < 0 or end < 0:
                raise ValueError(f"Invalid range [{start}:{end}] — indices must be non-negative")
            if start >= end:
                raise ValueError(f"Invalid range [{start}:{end}] — start must be less than end")
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
                raise ValueError(f"Invalid fixed width '{w}' — must be a positive integer")
            if cursor + w > len(line):
                raise ValueError(
                    f"Configured fixed width '{w}' at position {cursor} exceeds line length {len(line)} :: line='{line}'"
                )
            fields.append(line[cursor:cursor+w])
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
                raise ValueError(f"Invalid variable range [{start}:{end}] — must be int or 'end'")
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

    # Case 5: string delimiter is only whitespace
    if isinstance(delimiter, str) and delimiter.strip() == "":
        return re.split(r"\s+", line.strip())

    # Case 6: normal string delimiter
    return line.split(delimiter)


# --- Config adjust ---
def validate_denest_config(config, file_name):
    log_event("INFO", f"Parsing config for {file_name}", file_name)
    headers = config.get("header_identifiers", [])
    header_record = config.get("header_record", "")
    if headers:
        config["layout_header_identifier"] = headers[0]
    elif header_record:
        delim = config.get("header_delimiter") or config.get("delimiter")
        config["layout_header_identifier"] = safe_split(header_record.strip(), delim)[0]
    trailers = config.get("trailer_identifiers")
    if trailers:
        config["trailer_record_count_identifier"] = trailers[0]


# --- Header/trailer checks ---
def is_header(line, config):
    headers = config.get("header_identifiers")
    delimiter = config.get("header_delimiter") or config.get("delimiter")
    if headers:
        for header in headers:
            if line.startswith(header):
                return safe_split(line.strip(), delimiter)[0] == header
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


# --- File validator ---
def validate_file(input_bucket, input_file, config, file_name):
    validate_denest_config(config, file_name)
    delimiter = config.get("delimiter")
    header_delimiter = config.get("header_delimiter")
    trailer_delimiter = config.get("trailer_delimiter")
    trailer_idx = int(config.get("trailer_record_count_index", 0))
    trailer_id = config.get("trailer_record_count_identifier")
    layout_header_identifier = config["layout_header_identifier"]
    header_record = config.get("header_record")

    file_path = f"s3://{input_bucket}/{input_file}"
    log_event("INFO", f"Streaming validation started for {file_path}, delimiter='{delimiter}, header_delimiter='{header_delimiter}', trailer_delimiter='{trailer_delimiter}', layout_header_identifier='{layout_header_identifier}', trailer_id='{trailer_id}', trailer_idx='{trailer_idx}', header_record='{header_record}'", file_name)

    header_count = 0
    trailer_count = 0
    data_count = 0

    expected_cols = 0
    record_count_per_trailer = 0
    line_number = 0

    # Derive expected columns from header_record if provided
    if header_record:
        delim = header_delimiter or delimiter
        expected_cols = len(safe_split(header_record.strip(), delim))

    with sfs.open(file_path, 'r', encoding='windows-1252') as f:
        for raw_line in f:
            line_number += 1
            line = raw_line.strip()
            if not line:
                continue

            if is_header(line, config):
                header_count += 1
                tokens = safe_split(line, header_delimiter or delimiter)
                if tokens and tokens[0] == layout_header_identifier:
                    expected_cols = len(tokens)
                log_event("INFO", f"Line {line_number}: Detected header -> expected_cols={expected_cols}", file_name)

            elif trailer_id and is_trailer(line, config):
                trailer_count += 1
                parts = safe_split(line, trailer_delimiter or delimiter)
                if trailer_idx and len(parts) >= trailer_idx:
                    try:
                        record_count_per_trailer = int(parts[trailer_idx - 1])
                    except ValueError:
                        msg = f"Line {line_number}: Invalid trailer record count value -> {parts[trailer_idx - 1]}"
                        raise Exception(msg)
                log_event("INFO", f"Line {line_number}: Detected trailer -> count_from_trailer={record_count_per_trailer}", file_name)

            else:  # data row
                if expected_cols == 0:
                    msg = f"Line {line_number}: No record layout defined before data encountered"
                    raise Exception(msg)

                row = safe_split(line, delimiter)
                if len(row) != expected_cols:
                    allow_varlen = config.get("allow_variable_length", False)

                    if allow_varlen:
                        if len(row) < expected_cols:
                            # Pad missing fields
                            missing = expected_cols - len(row)
                            row.extend([''] * missing)

                        elif len(row) > expected_cols:
                            pass
                    else:
                        msg = f"Line {line_number}: Layout mismatch (expected={expected_cols}, got={len(row)}) -> line={line}"
                        raise Exception(msg)

                data_count += 1
                if data_count % 100000 == 0:
                    log_event("INFO", f"Processed {data_count} data records (at line {line_number})", file_name)

    # ------------------------------------------------
    # TRAILER COUNT VALIDATION
    # ------------------------------------------------
    if trailer_idx and record_count_per_trailer:
        count_type = config.get("trailer_count_type", "TOTAL")

        if count_type == "DATA_ONLY":
            actual = data_count
        else:  # TOTAL
            actual = header_count + data_count + trailer_count

        if actual != record_count_per_trailer:
            msg = (
                f"Record count mismatch: expected={record_count_per_trailer}, "
                f"count_type={count_type} "
                f"actual={actual} "
                f"(header={header_count}, data={data_count}, trailer={trailer_count})"
            )
            config["status"] = "FAILED"
            log_event("FAILED", msg, file_name)
            raise Exception(msg)

    # -----------------------------
    # Final results
    # -----------------------------
    config["record_count"] = data_count
    config["record_count_per_trailer"] = record_count_per_trailer
    config["header_count"] = header_count
    config["trailer_count"] = trailer_count
    config["status"] = "SUCCESS"

    log_event("INFO", f"Validation complete: header={header_count}, data={data_count}, trailer={trailer_count}, total_lines={line_number}, input_file_trailer_count={record_count_per_trailer}", file_name)


# --- Get field ---
def get_field(variable_name, config):
    fields = config.get("fields")
    return fields.get(variable_name) if fields else None


# --- Output prep ---
def prepare_output_content(config):
    file_name = config.get("file_name", "")
    stage = config.get("stage", "")
    status = config.get("status", "")
    record_count = config.get("record_count", 0)
    record_count_per_trailer = config.get("record_count_per_trailer", 0)
    layout = config.get("return_report_layout")
    if not layout:
        raise Exception("return_report_layout is not configured.")

    now = datetime.now()
    def dt_fmt(fmt): return now.strftime(fmt)
    def dt_est(fmt): return datetime.now(pytz.timezone("US/Eastern")).strftime(fmt)
    
    substitutions = {
        "date": lambda: dt_fmt("%m/%d/%y"),
        "time": lambda: dt_fmt("%H:%M"),
        "datetime": lambda: dt_est("%m/%d/%Y %I:%M %p %Z"),
        "report_period": lambda: dt_fmt("%m/%Y"),
        "record_count": lambda: str(record_count),
        "record_count_per_trailer": lambda: str(record_count_per_trailer),
        "file_name": lambda: file_name,
        "stage": lambda: stage,
        "status": lambda: status,
    }

    var_pattern = re.compile(r"\{([^}]*)\}")
    expr_pattern = re.compile(r"\((.*?)\)")

    report_json = {}
    for key, template in layout.items():
        line = template
        for variable in var_pattern.findall(template):
            lowvar = variable.lower()
            if lowvar in substitutions:
                line = line.replace(f"{{{variable}}}", substitutions[lowvar]())
        for expr in expr_pattern.findall(line):
            try:
                line = line.replace(f"({expr})", str(eval(expr)))
            except Exception:
                pass
        report_json[key] = line

    log_event("INFO", f"Prepared report JSON: {json.dumps(report_json)}", file_name)
    return report_json


# --- Write output ---
def write_output_file(output_bucket, output_file, report_json, file_name):
    output_file_uri = "s3://" + output_bucket + "/" + output_file
    log_event("INFO", f"Writing report to {output_file_uri}", file_name)
    with sfs.open(output_file_uri, 'w') as out_file:
        out_file.write(json.dumps(report_json, indent=2))


# --- Config JSON loader ---
def get_json(json_str, input_file_name):
    try:
        json_content = json.loads(json_str)
        for key in json_content.keys():
            if key in input_file_name:
                return json_content.get(key)
        raise ValueError(f"No matching key found in config for file {input_file_name}")
    except Exception as e:
        raise Exception(f"Config JSON parse failed: {e}")


def set_job_params_as_env_vars():
    messages = []
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i+1]
            os.environ[key] = value
            messages.append(f"{key}={value}")
    log_event("INFO", f"Env vars set from job params: {', '.join(messages)}", os.getenv('s3_input_file'))


# --- main ---
def main():
    file_name = None
    try:
        set_job_params_as_env_vars()
        input_file = os.getenv('s3_input_file')
        input_bucket = os.getenv('s3_bucket_input')
        output_bucket = os.getenv('s3_bucket_output')
        output_file = os.getenv('s3_output_file')
        return_report_config_str = os.getenv('s3_return_report_config')

        file_name = input_file[input_file.rfind('/') + 1:] if input_file else "N/A"
        log_event("STARTED", f"Started processing input_file={input_file}", file_name)
        stage = os.environ.get('stage')

        return_report_config_json = get_json(return_report_config_str, file_name)
        validate_required_config(return_report_config_json, file_name)
        return_report_config_json["file_name"] = file_name
        return_report_config_json["stage"] = stage

        validate_file(input_bucket, input_file, return_report_config_json, file_name)
        report_json = prepare_output_content(return_report_config_json)
        write_output_file(output_bucket, output_file, report_json, file_name)
        log_event("ENDED", "Successfully processed", file_name)

    except Exception as e:
        log_event("FAILED", f"Fatal Error={traceback.format_exc()}::{e}", file_name or "N/A")
        raise


if __name__ == '__main__':
    main()