import sys
import boto3
from s3fs import S3FileSystem
from awsglue.utils import getResolvedOptions
import re
import json
from collections import Counter
from datetime import datetime, timezone
from botocore.exceptions import ClientError
import traceback
import splunk
import os
import pytz

# simplefilestorage to get data in s3 object in bytes
sfs = S3FileSystem()


def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return f"arn:dpm:glue:glue_delimited_validation:" + account_id

def validate_denest_config(config, file_name):
    """
    Function to parse the config file and save required information into the respective variables.ti
    Args:
        config : input file specific report layout config extracted from the clients config file
    """

    # fields configured in the config file
    fields = {}
    # dictionary object to store the index of the column and its corresponding values needed to compose the output content.
    index_values_dict = {}
    # delimiter of the data file
    delimiter = ""
    # identifier for the primary header defining the record layout
    layout_header_identifier = ""
    # index in the trailer where total records is specified
    trailer_record_count_index = 0
    # identifier for the trailer defining the total records in the file
    trailer_record_count_identifier = ""
    # list of headers identifiers configured
    headers = []
    # list of trailer identifiers configured
    trailers = []
    # record layout configured as header ( when files do not contain header)
    header_record = ""

    log_event("INFO", f"validate_denest_config::parsing the configuration object::config={config}", file_name)

    delimiter = config.get("delimiter")

    if not delimiter:
        message = f"{program_name}::Either delimiter is not specified in the dynamo DB config file or it is empty::delimiter={delimiter}"
        raise ValueError(message)

    headers = config.get("header_identifiers")
    header_record = config.get("header_record")
    if not headers and not header_record:
        message = f"{program_name}::Either header_identifiers or header_record is not specified in the dynamo DB config file or both are empty::headers={headers}::header_record={header_record}"
        raise ValueError(message)
    if headers:
        layout_header_identifier = headers[0]
        config["headers"] = headers
        config["layout_header_identifier"] = layout_header_identifier
    if header_record:
        config["header_record"] = header_record
        layout_header_identifier = safe_split(header_record.strip(), delimiter)[0]
        config["layout_header_identifier"] = layout_header_identifier

    trailers = config.get("trailer_identifiers")
    if trailers:
        trailer_record_count_identifier = trailers[0]
        config["trailers"] = trailers
        config["trailer_record_count_identifier"] = trailer_record_count_identifier

    # create an array with all the index and values that can be populating when reading data file.
    # These values will be needed for further processing.
    fields = config.get("fields")
    if config.get("fields"):
        for name, field in fields.items():
            if field.get("index"):
                index = field.get("index")
                if index not in index_values_dict.keys():
                    index_values_dict[int(index)] = {}
            if field.get("pre_condition"):
                pre_field = field.get("pre_condition")
                pre_field_index = pre_field.get("index")
                if pre_field_index not in index_values_dict.keys():
                    index_values_dict[int(pre_field_index)] = {}
    config["fields"] = fields
    config["index_values_dict"] = index_values_dict


def validate_file(input_bucket, input_file, config, file_name):
    """
    Function to check if the total number of lines and the number from the trailer record match.
    Args:
        input_bucket : Name of the input bucket.
        input_file : File key of the input data file.
        config : input file specific report layout config extracted from the clients config file
    Returns:
        For success writes out the generated output.
        For failures raise an exception
    """

    # Call to parse the config file
    validate_denest_config(config, file_name)
    delimiter = config.get("delimiter")
    fields = config.get("fields")
    trailer_record_count_index = 0
    trailer_record_count_identifier = config.get("trailer_record_count_identifier")
    headers = config.get("headers")
    trailers = config.get("trailers")
    layout_header_identifier = config.get("layout_header_identifier")
    index_values_dict = config.get("index_values_dict")
    header_record = config.get("header_record")
    trailer_delimiter = config.get("trailer_delimiter")
    header_delimiter = config.get("header_delimiter")

    if config.get("trailer_record_count_index"):
        trailer_record_count_index = int(config.get("trailer_record_count_index"))

    record_count = 0
    file_info = sfs.info(f'{input_bucket}/{input_file}')

    if file_info['size'] == 0:
        message = f"{program_name} file open failed : Input file {input_file} in s3 bucket {input_bucket} is empty"
        raise ValueError(message)

    # number of records as per the trailer record
    record_count_per_trailer = 0
    headers_count = 0
    column_count = 0
    trailers_count = 0
    configured_trailers_count = 0
    configured_headers_count = 0

    if trailers:
        configured_trailers_count = len(trailers)
    if headers:
        configured_headers_count = len(headers)

    if header_record:
        column_count = len(safe_split(header_record.strip(), delimiter))

    file_path = "s3://" + input_bucket + "/" + input_file

    log_event("INFO", f"validate_file::header_record={header_record}, column_count={column_count}, layout_header_identifier={layout_header_identifier}, file_path={file_path}", file_name)

    # read datafile and store into an object
    try:
        with sfs.open(file_path, 'r', encoding='windows-1252') as file:
            ####        with sfs.open(file_path, 'r') as file:
            lines = file.readlines()
    except FileNotFoundError as e:
        message = f"{program_name} file not found : Input file {input_file} in s3 bucket {input_bucket}"
        raise Exception(message) from e

    for line in lines:
        if line.strip():
            # check if the line is the layout header
            if configured_headers_count > headers_count and is_header(line, config):
                headers_count += 1
                if header_delimiter:
                    if safe_split(line.strip(), header_delimiter)[0] == layout_header_identifier:
                        column_count = len(safe_split(line.strip(), header_delimiter))
                        log_event("INFO", f"validate_file::record layout header_delimiter={header_delimiter} and column_count={column_count}", file_name)
                else:
                    if safe_split(line.strip(), delimiter)[0] == layout_header_identifier:
                        column_count = len(safe_split(line.strip(), delimiter))
                        log_event("INFO", f"validate_file::record layout delimiter={header_delimiter} and ={column_count}", file_name)

            # check if the line is the trailer with record count
            elif configured_trailers_count > trailers_count and is_trailer(line, config):
                trailers_count += 1
                if trailer_record_count_index != 0 and trailer_record_count_identifier:               
                    if trailer_delimiter and safe_split(line.strip(), trailer_delimiter)[0] == trailer_record_count_identifier:
                        trailer_line_elements = safe_split(line.strip(), trailer_delimiter)
                        record_count_per_trailer = int(trailer_line_elements[trailer_record_count_index - 1])
                    else:
                        trailer_line_elements = safe_split(line.strip(), delimiter)
                        record_count_per_trailer = int(trailer_line_elements[trailer_record_count_index - 1])
            else:
                if column_count == 0:
                    raise Exception(f"{program_name} no record layout specified to validate the data")
                else:
                    row_data = safe_split(line.strip(), delimiter)
                    if len(row_data) != column_count:
                        raise Exception(f"{program_name} validation failed due to record not meeting the layout requirement::line={line}::row_data={row_data}::expected_column_count={column_count}::actual_column_count={len(row_data)}")

                    record_count += 1
                    # loop though the index field dictionary and save the values into corresponding field
                    for index in index_values_dict.keys():
                        index_values_dict.get(index)[record_count] = row_data[index - 1]
                    config["record_count"] = record_count

    # validate the record count if the validation is required
    if not trailers or not trailer_record_count_index:
        config["status"] = "SUCCESS"
        message = f"{program_name} no trailer validation details are provided, hence skipping the record count validation."
        log_event("INFO", f"validate_file::{message}", file_name)
    else:
        if record_count_per_trailer != 0 and record_count_per_trailer != record_count:
            message = f"validate_file::validation failed as the total record count does not match the count per the trailer::record_count_per_trailer={record_count_per_trailer}::actual_record_count={record_count}"
            config["status"] = "FAILED"
            raise Exception(message)
        else:
            message = f"validate_file::validation passed as the total record count does matches the count per the trailer::record_count_per_trailer={record_count_per_trailer}::actual_record_count={record_count}"
            config["status"] = "SUCCESS"
            log_event("INFO", f"validate_file::{message}", file_name)
    
    log_event("INFO", f"validate_file::record_count={record_count}, record_count_per_trailer={record_count_per_trailer}, headers_count={headers_count}, trailers_count={trailers_count}, configured_headers_count={configured_headers_count}, configured_trailers_count={configured_trailers_count}, config.get(status)={config.get('status')}", file_name)

def validate_file_janusletters(input_bucket, input_file, config, file_name):
    """
    Special validator for Janus Letters style files:
      - Ignores HDR/TRAILER column mismatches
      - Sets expected column_count from the first LETTER row
      - Validates data rows against that column_count
      - Compares actual count with trailer's reported count
    """
    # Parse config
    validate_denest_config(config, file_name)
    delimiter = config.get("delimiter")
    trailer_record_count_index = int(config.get("trailer_record_count_index") or 0)
    trailer_record_count_identifier = config.get("trailer_record_count_identifier")
    index_values_dict = config.get("index_values_dict")
    trailer_delimiter = config.get("trailer_delimiter")

    file_path = f"s3://{input_bucket}/{input_file}"
    file_info = sfs.info(f'{input_bucket}/{input_file}')

    if file_info['size'] == 0:
        message = f"{program_name}: file {input_file} is empty"
        raise ValueError(message)
    else:
        log_event("INFO", f"validate_file_janusletters::file_path={file_path}, delimiter={delimiter}, trailer_record_count_index={trailer_record_count_index}, trailer_record_count_identifier={trailer_record_count_identifier}, trailer_delimiter={trailer_delimiter}, file_info={file_info}", file_name)

    record_count = 0
    record_count_per_trailer = 0
    column_count = 0   # will be set from first LETTER row

    try:
        with sfs.open(file_path, 'r', encoding='windows-1252') as file:
            lines = file.readlines()
    except FileNotFoundError as e:
        message = f"{program_name}::file not found {input_file}::Exception::{str(e)}"
        raise Exception(message) from e

    for line in lines:
        if not line.strip():
            continue

        row_data = safe_split(line.strip(), delimiter)

        # If it's a trailer row
        if trailer_record_count_index and trailer_record_count_identifier and is_trailer(line, config):
            elems = safe_split(line.strip(), trailer_delimiter or delimiter)
            record_count_per_trailer = int(elems[trailer_record_count_index - 1])
            continue

        # If it's a header or trailer line → skip column enforcement
        if is_header(line, config) or is_trailer(line, config):
            continue

        # Data (LETTER) rows
        if column_count == 0:
            # First LETTER row sets the expected column count
            column_count = len(row_data)
            log_event("INFO", f"validate_file_janusletters::inferred data column_count={column_count} from first data row", file_name)

        if len(row_data) != column_count:
            msg = f"{program_name}::validate_file_janusletters::validation failed, expected {column_count}, got {len(row_data)} in line={line}"
            raise Exception(msg) from e

        record_count += 1
        for idx in index_values_dict.keys():
            index_values_dict[idx][record_count] = row_data[idx - 1]
        config["record_count"] = record_count

    # Compare actual data row count vs trailer's reported count
    if trailer_record_count_index and record_count_per_trailer:
        if record_count_per_trailer != record_count:
            msg = f"{program_name}: trailer count {record_count_per_trailer} != actual count {record_count}"
            raise Exception(msg)
        else:
            msg = f"{program_name}: record count validated successfully (count={record_count})"
            log_event("INFO", f"validate_file_janusletters::{msg}", file_name)

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

def is_valid_email(email: str) -> bool:
    """Return True if email looks like a valid address."""
    if not email:
        return False
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(email_regex, email) is not None

# --- Splunk logger ---
def log_event(status: str, message: str, input_file: str):

    job_name = "glue_delimited_validation"
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
        print(f"[ERROR] glue_delimited_validation : log_event : Failed to write log to Splunk: {str(e)}")

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

def evaluate_fields(config):
    """
    Function to evaluate the fields that are configured .
    Returns:
        populates the fields with the values needed for output
    """
    fields = config.get("fields")
    if fields:
        for field in fields.values():
            if field.get("type") == "count":
                field["count"] = get_count(field, config)
            elif field.get("type") == "duplicate":
                get_duplicate_field_details(field, config)
            else:
                message = f"{program_name} invalid type for the return report field."
                raise Exception(message)


def prepare_output_content(config):
    """
    Function to prepare the output content.
    Args:
        config : config based on which output is prepared.
    Returns:
        output content
    """
    fields = config.get("fields")
    record_count = config.get("record_count")
    file_name = config.get("file_name")
    stage = config.get("stage")
    status = config.get("status")
    return_report_lines = config.get("return_report_layout")
    record_count_per_trailer = config.get("record_count_per_trailer")
    support_email = config.get("support_email")

    return_report_lines = config.get("return_report_layout")
    if not return_report_lines:
        message = f"{program_name} return_report_layout is not configured."
        raise Exception(message)

    now = datetime.now()
    pattern = r'\{([^}]*)\}'
    report_json = {}
    for key, report_line in return_report_lines.items():
        variables = re.findall(pattern, report_line.strip())
        for variable in variables:
            if variable.lower() == 'date':
                dt_string = now.strftime("%m/%d/%y")
                report_line = report_line.replace("{" + variable + "}", dt_string)
            elif variable.lower() == 'time':
                dt_string = now.strftime("%H:%M")
                report_line = report_line.replace("{" + variable + "}", dt_string)
            elif variable.lower() == 'datetime':
                dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
                report_line = report_line.replace("{" + variable + "}", dt_string)
            elif variable.lower() == 'lpldatetime':
                now = datetime.now(pytz.timezone('US/Eastern'))
                dt_string = now.strftime("%m/%d/%Y %I:%M %p %Z")
                report_line = report_line.replace("{" + variable + "}", dt_string)
            elif variable.lower() == 'report_period':
                dt_string = now.strftime("%m/%Y")
                report_line = report_line.replace("{" + variable + "}", dt_string)
            elif variable.lower() == 'record_count':
                report_line = report_line.replace("{" + variable + "}", str(record_count))
            elif variable.lower() == 'file_name':
                report_line = report_line.replace("{" + variable + "}", str(file_name))
            elif variable.lower() == 'stage':
                report_line = report_line.replace("{" + variable + "}", str(stage))
            elif variable.lower() == 'support_email':
                report_line = report_line.replace("{" + variable + "}", str("For any support or questions contact : " + support_email))
            elif variable.lower() == 'status':
                report_line = report_line.replace("{" + variable + "}", str(status))
            elif variable.lower() == 'record_count_per_trailer':
                report_line = report_line.replace("{" + variable + "}", str(record_count_per_trailer))
            elif variable.lower() == 'lpl_email_receipt' and (file_name.startswith("REPINFO_LOGO") or file_name.startswith("LPL_INSERT_LIST2") or file_name.startswith("STMTINSERTFILE")):
                report_line = report_line.replace("{" + variable + "}", str("THIS REPORT VERIFIES THE RECEIPT OF A FILE AND CHECKS RECORD COUNT BUT DOES NOT VALIDATE THE DATA."))
            elif variable.endswith("_size"):
                variable_name = variable.split("_size")[0]
                # in case of size treat the size as the value to add to the report.
                if config.get(variable_name):
                    variable_value = len(config.get(variable_name))
                    report_line = report_line.replace("{" + variable + "}", str(variable_value))
                elif get_field(variable_name, config):
                    field = get_field(variable_name, config)
                    report_line = report_line.replace("{" + variable + "}", len(field.get("values")))
                else:
                    report_line = report_line.replace("{" + variable + "}", '0')
            elif variable.endswith("_count"):
                variable_name = variable.split("_count")[0]
                field = get_field(variable_name, config)
                if field:
                    report_line = report_line.replace("{" + variable + "}", str(field.get("count")))
            elif variable.endswith("_values"):
                variable_name = variable.split("_values")[0]
                field = get_field(variable_name, config)
                if field:
                    value_string = ''
                    for value in field.get("values"):
                        value_string += value + '\n'
                    report_line = report_line.replace("{" + variable + "}", value_string)
            else:
                message = f"{program_name} invalid variable attribute in the output layout."
                raise Exception(message)

        evalpattern = r'\((.*?)\)'
        expressions = re.findall(evalpattern, report_line.strip())
        for expression in expressions:
            report_line = report_line.replace("(" + expression + ")", str(eval(expression)))
        report_json[key] = report_line

    message = f"{program_name} generated report text"
    log_event("INFO", f"glue_delimited_validation::prepare_output_content::report_json={json.dumps(report_json)}", file_name)
    return report_json

def write_output_file(output_bucket, output_file, report_json, file_name):
    """
    Write the text to the output file
    Args:
        output_bucket : bucket where the file needs to be written
        output_file : name of the file that needs to be written
        report_json : text to be written to the file
    Returns:
        the field matching the name
    """
    output_file_uri = "s3://" + output_bucket + "/" + output_file
    log_event("INFO", f"glue_delimited_validation::writing the output to the file::output_file_uri={output_file_uri}", file_name)
    with sfs.open(output_file_uri, 'w') as out_file:
        out_file.write(json.dumps(report_json, indent=2))
        out_file.close()


def get_field(variable_name, config):
    """
    Get the field object representing the field name
    Args:
        variable_name : name of the field
    Returns:
        the field matching the name
    """
    fields = config.get("fields")
    return fields.get(variable_name)


def get_count(field, config):
    """
    Get the number of rows that meet the conditions in the field
    Args:
        field : dict object representing the field from the config file
    Returns:
        number of records meeting the conditions.
    """
    count = 0
    index_values_dict = config.get("index_values_dict")
    field_value_to_validate_with = field.get("value")
    values_dict = index_values_dict.get(int(field.get("index")))
   
    for value in values_dict.values():
        # Handle blank or null field_value_to_validate_with
        if field_value_to_validate_with is None or field_value_to_validate_with.strip() == "":
            if value is None or value.strip() == "":
                count += 1
        else:
            # Compare non-blank values
            if value is not None and value.strip().lower() == field_value_to_validate_with.strip().lower():
                count += 1
    return count


def get_duplicate_field_details(field, config):
    """
    Get details regarding the duplicates of the configured field object.
    Args:
        field : dict object representing the field from the config file
    Returns:
        field object populated with teh results of this function.
    """
    index_values_dict = config.get("index_values_dict")
    all_values_dict = index_values_dict.get(int(field.get("index")))
    pre_cond_field = field.get("pre_condition")
    if pre_cond_field:
        pre_cond_field_index = pre_cond_field.get('index')
        pre_cond_field_value = pre_cond_field.get('value')
        pre_cond_field_values_dict = index_values_dict.get(int(pre_cond_field_index))

    all_qualified_values = []
    for position, value in all_values_dict.items():
        if len(value) != 0 and value.lower() != 'null':
            if pre_cond_field:
                if pre_cond_field_values_dict.get(position) == pre_cond_field_value:
                    all_qualified_values.append(value)

    item_counts = Counter(all_qualified_values)
    duplicates = []
    duplicate_count = 0
    total_duplicate_count = 0
    for item, count in item_counts.items():
        if count > 1 and item.strip() != "":
            duplicates.append(item)
            duplicate_count += 1
            #keeping the first one and hence incrementing by one less
            total_duplicate_count += count-1
    field["values"] = duplicates
    field["total_count"] = total_duplicate_count
    field["count"] = duplicate_count


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
            raise ValueError(f"{program_name} no matching key found in input_file_name: {input_file_name}")

        return config
    except Exception as e:
        message = f"{program_name} conversion of configuration string into json failed::json_str={json_str}::Exception::{str(e)}"
        raise Exception(message)


def set_job_params_as_env_vars():
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    messages = []
    messages.append(f"Set environment variable:")
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i+1]
            os.environ[key] = value
            messages.append(f"{key}={value}")

    log_event("INFO", f"glue_delimited_validation::Environment variables set from job parameters: {', '.join(messages)}", os.getenv('s3_input_file'))


# main:
def main():
    file_name = None  # Initialize at start
    try:
        set_job_params_as_env_vars()
        input_file = os.getenv('s3_input_file')
        input_bucket = os.getenv('s3_bucket_input')
        output_bucket = os.getenv('s3_bucket_output')
        output_file = os.getenv('s3_output_file')
        return_report_config_str = os.getenv('s3_return_report_config')

        file_name = input_file[input_file.rfind('/') + 1:len(input_file)]
        log_event("STARTED", f"glue_delimited_validation::started_processing return_report_config_str={return_report_config_str}, output_file={output_file}, input_file={input_file}, input_bucket={input_bucket}, output_bucket={output_bucket}", file_name)
        stage = os.environ.get('stage')

        return_report_config_json = get_json(return_report_config_str, file_name)
        log_event("INFO", f"glue_delimited_validation::loaded return_report_config_json={json.dumps(return_report_config_json)}", file_name)
        return_report_config_json["file_name"] = file_name
        return_report_config_json["stage"] = stage

        if "JANUS_LETTERS" in input_file:
            log_event("INFO", f"glue_delimited_validation::Using Janus Letters validator for {input_file}", file_name)
            validate_file_janusletters(input_bucket, input_file, return_report_config_json, file_name)

            # Copy input file to to_transmission/
            dest_key = input_file.replace("input/", "to_transmission/", 1)
            boto3.client("s3").copy_object(Bucket=input_bucket, CopySource={"Bucket": input_bucket, "Key": input_file}, Key=dest_key)
            log_event("INFO", f"glue_delimited_validation::Copied verified file {input_file} to {dest_key}", file_name)

        else:
            log_event("INFO", f"glue_delimited_validation::Using standard validator for {input_file}", file_name)
            validate_file(input_bucket, input_file, return_report_config_json, file_name)

        log_event("INFO", f"glue_delimited_validation::file validation completed, now evaluating fields", file_name)
        evaluate_fields(return_report_config_json)

        log_event("INFO", f"glue_delimited_validation::fields evaluated, now preparing output content", file_name)
        report_json = prepare_output_content(return_report_config_json)

        log_event("INFO", f"glue_delimited_validation::output content prepared, now writing to output file", file_name)
        write_output_file(output_bucket, output_file, report_json, file_name)

        log_event("ENDED", f"glue_delimited_validation::successfully_processed", file_name)

    except Exception as e:
        log_event("FAILED", f"glue_delimited_validation::Fatal Error={traceback.format_exc()}::Exception::{e}", file_name)
        raise e


if __name__ == '__main__':

    program = os.path.basename(sys.argv[0])
    program_name = program.split('.')[0]
    main()