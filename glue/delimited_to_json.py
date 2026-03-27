import sys
import boto3
import os
import json
import collections
from awsglue.utils import getResolvedOptions
from s3fs import S3FileSystem
import re
from datetime import datetime, timezone
from botocore.exceptions import ClientError
import traceback
# Importing necessary modules
import splunk
from keyIndex import *

# Initialize S3FileSystem object
sfs = S3FileSystem()

client_flag = None
client_config_str = None
meta_data_config_buc = None
meta_data_config_file = None
output_file_bucket = None
header_record = None
trailer_record = None
main_record_start = None
sub_record_start = None
output_path = None


def get_run_id():
    """
    Function to retrieve the AWS account ID and generate a unique run ID for the Glue job.
    """
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:delimited_to_json:" + account_id


def is_valid_email(email: str) -> bool:
    """Return True if email looks like a valid address."""
    if not email:
        return False
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(email_regex, email) is not None


# --- Splunk logger ---
def log_event(status: str, message: str, input_file: str):
    job_name = "glue_delimited_to_json"
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
        print(f"[ERROR] glue_delimited_to_json : log_event : Failed to write log to Splunk: {str(e)}")

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


def client_check(config, file_name):
    log_event("INFO", f"Checking the client for further processing, config={file_name}", file_name)

    global client_flag
    client_config_str = config
    client_flag = config.get("system_flag")
    if (client_flag):
        parse_client_config(client_config_str)


def parse_client_config(client_config_str):
    global meta_data_config_buc, meta_data_config_file, main_record_start, sub_record_start, output_path, header_record, trailer_record

    meta_data_config_buc = client_config_str.get("meta_data_file_bucket")
    meta_data_config_file = client_config_str.get("meta_data_file")
    output_path = client_config_str.get("output_file_path")
    header_record = client_config_str.get("header")
    trailer_record = client_config_str.get("trailer")
    main_record_start = client_config_str.get("main_field")
    sub_record_start = client_config_str.get("sub_field")


def process_client(input_bucket, input_file, meta_data_config_file, delimiter, output_bucket, output_file, file_name):
    global output_file_bucket
    config_file_path = "s3://" + input_bucket + "/" + meta_data_config_file
    input_file_path = "s3://" + input_bucket + "/" + input_file
    delimiter_value = delimiter

    with sfs.open(config_file_path, 'r') as config_file:
        config = json.load(config_file)

    header_values = None
    accounts = []
    current_account = None
    header_json = None
    trailer_json = None

    with sfs.open(input_file_path, 'r', encoding='utf-8') as input_file:
        lines = input_file.readlines()
        trailer_line = lines.pop(-1)

        for i, line in enumerate(lines):
            # Replace \| with | and \\ with a space in record values
            record_values = [value.strip().replace("\\|", "|").replace("\\", " ") for value in
                             safe_split(line, delimiter_value)]
            record_type = record_values[0]

            if record_type == "B":
                # Process header values
                header_values = [value.replace("\\|", "|").replace("\\", " ") for value in record_values]
                header_json = {"HEADER": dict(zip(config["file_header"], header_values))}
            else:
                # Process account records
                account_values = [value.replace("\\|", "|").replace("\\", " ") for value in record_values]
                account_json = dict(zip(config["file_account"], account_values))
                if current_account:
                    accounts.append(current_account)
                current_account = account_json

    # Replace \| with | and \\ with a space in trailer values
    trailer_values = [value.strip().replace("\\|", "|").replace("\\", " ") for value in
                      safe_split(trailer_line, delimiter_value)]
    trailer_json = {"TRAILER": dict(zip(config["file_trailer"], trailer_values))}

    if current_account:
        accounts.append(current_account)

    output_json = {
        header_record: header_json["HEADER"],
        main_record_start: accounts,
        trailer_record: trailer_json["TRAILER"]
    }

    output_file_bucket = input_bucket
    output_file_path = get_client_output_path(output_file, output_path, file_name)
    with sfs.open(output_file_path, 'w') as output_file:
        json.dump(output_json, output_file, indent=4)

    log_event("INFO", f"JSON file has been generated successfully, output_file_path={output_file_path}", file_name)


def get_client_output_path(output_file, output_path, file_name):
    existing_path = output_file.rsplit('/', 1)[0]
    final_output_path = output_file.replace(existing_path, output_path)
    output_file_path = "s3://" + output_file_bucket + "/" + final_output_path

    log_event("INFO", f"output_file={output_file}, output_path={output_path}, output_file_path={output_file_path}",
              file_name)

    return output_file_path


def validate_denest_config(config, file_name):
    """
    Function to parse the config file and save required information into the respective variables.
    Args:
        config : input file specific report layout config extracted from the clients config file
    """
    log_event("INFO", f"validate_denest_config::config={config}", file_name)

    # identifier for the primary header defining the record layout
    layout_header_identifier = ""
    # list of headers identifiers configured
    headers = []
    # list of trailer identifi
    # ers configured
    trailers = []

    headers = config.get("header_identifiers")
    header_record = config.get("header_record")
    if not headers and not header_record:
        message = "There are no header identifiers specified in the config file"
        raise ValueError(message)
    if headers:
        layout_header_identifier = headers[0]
        config["headers"] = headers
        config["layout_header_identifier"] = layout_header_identifier
    if header_record:
        config["header_record"] = header_record

    trailers = config.get("trailer_identifiers")
    if trailers:
        config["trailers"] = trailers
    else:
        log_event("INFO", "There are no trailers specified in the config file", file_name)

    trailer_delimiter = config.get("trailer_delimiter")

    if config.get("duplicate_removal"):
        duplicate_removal = config.get("duplicate_removal")
        field_name = duplicate_removal.get("field_name")
        removal_condition = duplicate_removal.get("removal_condition")
        pre_cond_field = duplicate_removal.get("pre_condition")
        pre_cond_field_name = ''
        pre_cond_field_value = ''
        if pre_cond_field:
            pre_cond_field_name = pre_cond_field.get('field_name')
            pre_cond_field_value = pre_cond_field.get('value')

        if removal_condition != "non_first":
            message = "value of removal_condition configured under duplicate removal is invalid"
            raise ValueError(message)

        if not field_name or not removal_condition:
            message = "Attributes required for removing duplicates are missing in the config file"
            raise ValueError(message)

        if pre_cond_field and (not pre_cond_field_name or not pre_cond_field_value):
            message = "PreCondition for duplicate removal is configured but not the corresponding field and/or value"
            raise ValueError(message)

        config["removal_field_name"] = field_name
        config["removal_condition"] = removal_condition
        config["pre_cond_field_name"] = pre_cond_field_name
        config["pre_cond_field_value"] = pre_cond_field_value


def safe_split(line: str, delimiter: str):
    line = line.strip()

    # Case 1: delimiter is empty string → return whole line as-is
    if delimiter == "":
        return [line]

    # Case 2: delimiter is only whitespace (spaces/tabs/newlines)
    if delimiter.strip() == "":
        return re.split(r"\s+", line)

    # Case 3: otherwise split literally on delimiter
    return line.split(delimiter)


def process_file(input_bucket, input_file, delimiter, config, file_name):
    """
    Function to process the input file from S3, convert it to JSON, and store the data.
    """
    log_event("INFO",
              f"Starting process_file::input_bucket{input_bucket}::input_file={input_file}::delimiter={delimiter}",
              file_name)

    validate_denest_config(config, file_name)

    log_event("INFO", f"config obj afer validate_denest_config::config={config}", file_name)

    avl_tree = KITree()
    storage = DataStorage([])

    count = 0
    keys = []
    # file_info = sfs.info(f'{input_bucket}/{input_file}')
    # if file_info['size'] == 0:
    #     raise ValueError(
    #         f"File open from S3 bucket failed with error: Input file {input_file} in S3 bucket {input_bucket} is empty.")

    headers = config.get("header_identifiers")
    trailers = config.get("trailer_identifiers")
    trailer_delimiter = config.get("trailer_delimiter")
    layout_header_identifier = config.get("layout_header_identifier")
    header_record = config.get("header_record")
    config["delimiter"] = delimiter

    if config.get("duplicate_removal"):
        removal_condition = config.get("removal_condition")
        removal_field_name = config.get("removal_field_name")
        duplicates_encountered = set()
        pre_cond_field_name = ''
        pre_cond_field_value = ''

        if config.get("pre_cond_field_name") and config.get("pre_cond_field_value"):
            pre_cond_field_name = config["pre_cond_field_name"]
            pre_cond_field_value = config["pre_cond_field_value"]

    configured_trailers_count = 0
    configured_headers_count = 0
    headers_count = 0
    trailers_count = 0

    if trailers:
        configured_trailers_count = len(trailers)
    if headers:
        configured_headers_count = len(headers)

    if header_record:
        keys = safe_split(header_record.strip(), delimiter)
        layout_header_identifier = keys[0]
        log_event("INFO",
                  f'header_record={header_record}, layout_header_identifier={layout_header_identifier}, keys={keys}',
                  file_name)

    file_path = "s3://" + input_bucket + "/" + input_file

    with sfs.open(file_path, 'r', encoding='windows-1252') as file:
        for line in file:
            # line = line.encode('ascii', 'ignore').decode('ascii')
            # line = " ".join(line.split())
            # Replace \| with | and \\ with a space in all fields
            record_parts = [value.replace("\\|", "|").replace("\\", " ") for value in
                            safe_split(line.strip(), delimiter)]

            if configured_headers_count > headers_count and is_header(line, config):
                headers_count += 1
                if line.startswith(layout_header_identifier) and len(record_parts) > 1:
                    keys = record_parts
                    # is_header = False
            elif configured_trailers_count > trailers_count and is_trailer(line, config):
                trailers_count += 1
                log_event("INFO", f'line is a trailer, line={line}', file_name)
            elif len(record_parts) > 1:
                ordered_dict = collections.OrderedDict(zip(keys, record_parts))
                file_name = os.path.basename(input_file)
                for key, value in ordered_dict.items():
                    ordered_dict[key] = value
                    ####################    ICSBRCCPPI-3439 3604 ########################
                    if "ETS50300" in file_name:
                        if key == "50303-DS-DESC-TABLE":
                            try:
                                # Parse the JSON string into a Python object
                                ordered_dict[key] = json.loads(value)
                            except json.JSONDecodeError:
                                # Handle cases where the value isn't valid JSON
                                ordered_dict[key] = value
                        else:
                            ordered_dict[key] = value
                    #################################################
                json_data = json.dumps(ordered_dict, indent=2)
                data = json.loads(json_data)

                # Added this section of logic for the removal duplicates.  This also works when dupliate removal is not configured.
                skip_row = False
                column_value = ''
                pre_cond_column_value = ''

                if config.get("duplicate_removal") and removal_condition == "non_first":
                    if data[removal_field_name]:
                        column_value = data[removal_field_name]
                    if pre_cond_field_name and data[pre_cond_field_name]:
                        pre_cond_column_value = data[pre_cond_field_name]

                    if column_value != '':
                        # when duplicate input file is specified consider only those values while removing duplicates.
                        # Check if precondition is met (if configured)
                        precondition_met = True
                        if pre_cond_field_name and pre_cond_field_value:
                            precondition_met = (pre_cond_field_value == pre_cond_column_value)

                            # Only process duplicates if precondition is met
                            if precondition_met:
                                if column_value not in duplicates_encountered:
                                    duplicates_encountered.add(column_value)
                                else:
                                    skip_row = True
                            # If precondition is not met, don't process for duplicates (don't skip the row)
                        else:
                            if column_value not in duplicates_encountered:
                                duplicates_encountered.add(column_value)
                            else:
                                skip_row = True
                if not skip_row:
                    storage.add_record(data)

            elif len(record_parts) > 0:
                ordered_dict = collections.OrderedDict(zip(keys, record_parts))
                file_name = os.path.basename(input_file)
                json_data = json.dumps(ordered_dict, indent=2)
                data = json.loads(json_data)
                storage.add_record(data)

    return storage.get_data()


def write_output_file(converted_data_contents, output_bucket, output_file):
    """
    Function to write the converted JSON data contents to an output file in S3.
    """
    output_file_uri = "s3://" + output_bucket + "/" + output_file
    if not converted_data_contents:
        message = f"Write operation to S3 bucket {output_bucket} failed with error: Contents to output file {output_file} is empty."
        raise ValueError(message)
    else:
        with sfs.open(output_file_uri, 'w') as out_file:
            out_file.write("{")
            out_file.write('"json_data": ')
            out_file.write(json.dumps(converted_data_contents, indent=2).replace('\\"', ''))
            out_file.write("}")


def is_header(line, config):
    """
    Check if the line is of the type header
    Args:
        line : a line from the datafile
    Returns:
        true if it is a header
        false otherwise
    """
    headers = config.get("headers")
    delimiter = config.get("delimiter")
    header_delimiter = config.get("header_delimiter")

    if headers:
        for header in headers:
            if line.startswith(header):
                if header_delimiter:
                    if safe_split(line.strip(), header_delimiter)[0] == header:
                        return True
                    else:
                        return False
                else:
                    if safe_split(line.strip(), delimiter)[0] == header:
                        return True
    return False


def is_trailer(line, config):
    """
    Check if the line is of the type trailer
    Args:
        line : a line from the datafile
    Returns:
        true if it is a header
        false otherwise
    """
    trailers = config.get("trailers")
    delimiter = config.get("delimiter")
    trailer_delimiter = config.get("trailer_delimiter")

    if trailers:
        for trailer in trailers:
            if line.startswith(trailer):
                if trailer_delimiter:
                    if safe_split(line.strip(), trailer_delimiter)[0] == trailer:
                        return True
                    else:
                        return False
                else:
                    if safe_split(line.strip(), delimiter)[0] == trailer:
                        return True
    return False


def get_json(json_str, input_file_name):
    """
    Read the string into a JSON object
    Args:
        json_str : a string representation of the json object
        input_file_name : name of the input data file
    Returns:
        json object that the string represents
    """
    try:
        json_content = json.loads(json_str)
        for key in json_content.keys():
            if key in input_file_name:
                config = json_content.get(key)
                break

        if config is None:
            raise ValueError(f"No matching key found in input_file_name: {input_file_name}")

        return config
    except Exception as e:
        message = f"Conversion of configuration string into json failed::json_str={json_str} : {str(e)}"
        raise Exception(message) from e


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

    log_event("INFO", f"glue_delimited_to_json::Environment variables set from job parameters: {', '.join(messages)}",
              os.getenv('s3_input_file'))


# Main function to orchestrate the entire process
def main():
    file_name = None  # Initialize at start
    try:
        avl_tree = KITree()
        storage = DataStorage([])
        set_job_params_as_env_vars()
        message = "retrieving parameters"
        splunk.log_message({'Status': 'logging', 'Message': message}, get_run_id())

        input_bucket = os.getenv('s3_bucket_input')
        input_file = os.getenv('s3_input_file')
        output_bucket = os.getenv('s3_bucket_output')
        output_file = os.getenv('s3_output_file')
        delimiter = os.getenv('delimiter')
        config_str = os.getenv("config_str")

        if not input_bucket or not output_bucket or not delimiter or not input_file or not output_file or not config_str:
            message = f"glue_delimited_to_json::Some or all of the attributes required for the module are missing::ERROR::input_bucket={input_bucket}, input_file={input_file}, output_bucket={output_bucket}, output_file={output_file}, delimiter={delimiter}, config_str={config_str}"
            raise ValueError(message)

        file_name = input_file[input_file.rfind('/') + 1:len(input_file)]
        log_event("STARTED",
                  f"glue_delimited_to_json::Retrieved parameters: input_bucket={input_bucket}, input_file={input_file}, output_bucket={output_bucket}, output_file={output_file}, delimiter={delimiter}, config_str={config_str}",
                  file_name)

        conversion_config_json = get_json(config_str, file_name)
        # Process the input file
        client_check(conversion_config_json, file_name)

        if (client_flag):
            process_client(input_bucket, input_file, meta_data_config_file, delimiter, output_bucket, output_file,
                           file_name)
            message = "process_client::File open from S3 bucket is successful."
            log_event("INFO", f"glue_delimited_to_json::message={message}", file_name)
        else:
            converted_data = process_file(input_bucket, input_file, delimiter, conversion_config_json, file_name)
            message = "process_file::File open from S3 bucket is successful."
            log_event("INFO", f"glue_delimited_to_json::message={message}", file_name)

        # Write the converted JSON data to an output file in S3
        if (not client_flag):
            write_output_file(converted_data, output_bucket, output_file)

        log_event("ENDED", f"glue_delimited_to_json::successfully_processed and generated output_file={output_file}",
                  file_name)

    except Exception as e:
        log_event("FAILED", f"glue_delimited_to_json::Fatal Error: {traceback.format_exc()}::Exception::{str(e)}",
                  file_name)
        raise Exception(message) from e


if __name__ == '__main__':
    program = os.path.basename(sys.argv[0])
    program_name = program.split('.')[0]

    main()