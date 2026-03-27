import os
import sys
import json
import tempfile
import boto3
import re
import traceback
from botocore.exceptions import BotoCoreError, ClientError
from datetime import datetime, timezone
from s3fs import S3FileSystem
import splunk

job_start_time = datetime.now(timezone.utc)
sfs = S3FileSystem()
inputfile = None
TS_STRING = datetime.now().strftime("%Y%m%dT%H%M%S%f")[:-3]  # YYYYMMDDTHHMMSSmmm
file_date = datetime.now().strftime("%m%d%Y")


def get_run_id():
    try:
        account_id = boto3.client("sts").get_caller_identity()["Account"]
        return f"arn:dpm:glue:debatch:{account_id}"
    except Exception as e:
        print(f"[ERROR] Failed to get run id/account: {e}")
        raise e


def is_valid_email(email: str) -> bool:
    """Return True if email looks like a valid address."""
    if not email:
        return False
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(email_regex, email) is not None


# --- Splunk logger ---
def log_event(status: str, message: str):
    job_name = "glue_debatch"
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
        "step_function": "debatch",
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
        print(f"[ERROR] glue_debatch : log_event : Failed to write log to Splunk: {str(e)}")

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


def parse_client_config(config):
    try:
        if isinstance(config, str):
            config = json.loads(config)
        log_event("INFO", f"Parsed client config: {list(config.keys())}")
        return (
            config['recordType'],
            config['headers'],
            config['trailers'],
            config['accountInfo'],
            config['outputHeaders'],
            config['outputTrailers'],
            config['outputRecords'],
            config.get('comboRecords'),
            config.get('trig_folder'),
            config.get('createTrig', True),
            config.get('only_combo', True),
            config.get('useAccountNumberDebatch', False)
        )
    except Exception as e:
        message = f"Error parsing client config: {str(e)}"
        raise Exception(message) from e


def is_header(line, headers, record_type):
    try:
        return any(line[record_type['recordStart']:record_type['recordEnd'] + 1] == h for h in headers)
    except Exception as e:
        log_event("WARN",
                  f"glue_debatch::Error determining header::{str(e)}::line={line}::headers={headers}::record_type={record_type}")
        return False


def is_trailer(line, trailers, record_type):
    try:
        return any(line[record_type['recordStart']:record_type['recordEnd'] + 1] == t for t in trailers)
    except Exception as e:
        log_event("WARN",
                  f"glue_debatch::Error determining trailer::{str(e)}::line={line}::trailers={trailers}::record_type={record_type}")
        return False


def get_account(line, account_info):
    try:
        return line[account_info['accountStart']:account_info['accountEnd'] + 1].strip()
    except Exception as e:
        message = f"glue_debatch::Error extracting account number::{str(e)}::line={line}::account_info={account_info}"
        raise Exception(message) from e


def upload_to_s3(local_file, s3uri, label="file"):
    try:
        sfs.put(local_file, s3uri)
        os.remove(local_file)
    except Exception as e:
        message = f"Error uploading {label} to S3: {str(e)}, local_file={local_file}, s3uri={s3uri}"
        if os.path.exists(local_file):
            try:
                os.remove(local_file)
            except Exception as ex2:
                log_event("INFO", f"Could not remove file {local_file} after failed upload: {ex2}")
        raise Exception(message) from e


def print_and_log_error(message, **kwargs):
    print(f"[ERROR] {message}", kwargs if kwargs else "")
    log = {'Status': 'failed', 'Message': message}
    log.update(kwargs)
    try:
        splunk.log_message(log, get_run_id())
    except Exception as e:
        print(f"[ERROR] glue_debatch : print_and_log_error : Failed to write error to Splunk: {str(e)}")


def print_and_log_info(message, **kwargs):
    print(f"[INFO] {message}", kwargs if kwargs else "")
    log = {'Status': 'info', 'Message': message}
    log.update(kwargs)
    try:
        splunk.log_message(log, get_run_id())
    except Exception as e:
        print(f"[ERROR] glue_debatch : print_and_log_info : Failed to write log to Splunk: {str(e)}")


def create_trig_file(trig_folder, create_trig_flag):
    try:
        if not create_trig_flag:
            log_event("INFO", "create_trig_flag is False; skipping TRIG file creation.")
            return

        if not trig_folder:
            log_event("INFO", "trig_folder not provided; skipping TRIG file creation.")
            return

        timestamp = datetime.now().strftime("%Y-%m-%dT%H%M%S")
        filename = f"multifile-validation-kafka-trigger_{timestamp}.txt"

        tmpf = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
        tmpf.close()

        # Normalize S3 path
        if not trig_folder.startswith("s3://"):
            out_bucket = os.getenv('s3_bucket_output')
            s3uri = f"s3://{out_bucket}/{trig_folder}/{filename}"
        else:
            s3uri = f"{trig_folder}/{filename}"

        upload_to_s3(tmpf.name, s3uri, label="TRIG")
        log_event("INFO", f"TRIG file created and uploaded, filename={filename}, s3uri={s3uri}")
    except Exception as e:
        fname = locals().get("filename", "unknown_trigfile")
        log_event("WARN", f"Failed to create TRIG file={fname}: {str(e)}")


def write_final_list(written_files, debatch_kafka_path, filename):
    """
    Writes all file names generated in output records into a file 'final_list_files.txt'
    and uploads it to the debatch_kafka location, handling both relative and absolute S3 paths.
    """
    try:
        if not debatch_kafka_path:
            print_and_log_error("debatch_kafka path not provided. Skipping final list write.")
            return

        base_name = filename.rsplit(".", 1)[0]

        # Create temp file
        tmpf = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
        tmpf.write("\n".join(written_files))
        tmpf.write("\n")
        tmpf.close()

        # --- Normalize S3 path like headers/trailers ---
        if not debatch_kafka_path.startswith("s3://"):
            out_bucket = os.getenv('s3_bucket_output')
            s3uri = f"s3://{out_bucket}/{debatch_kafka_path}/{base_name}_final_list_files.txt"
        else:
            s3uri = f"{debatch_kafka_path}/{base_name}_final_list_files.txt"

        upload_to_s3(tmpf.name, s3uri, label="final_list")
        print_and_log_info(
            "Final list file created and uploaded",
            file="final_list_files.txt", s3uri=s3uri,
            record_file_count=len(written_files)
        )

    except Exception as e:
        print_and_log_error(f"Error writing final list file: {str(e)}")
        log_event("FAILED", f"glue_debatch Error writing final list file: {str(e)}")
        raise


def write_brx_done_json(written_files, brx_path, filename):
    """
    Writes a JSON file that lists all debatched output files
    in the format:
    {
      "BASENAME.TXT": {
        "files": [
          {
            "debatchedfilename": "<file>.in.txt",
            "dataFile": "<file>.out.json",
            "reportFile": "<file>.out.xml"
          },
          ...
        ]
      }
    }
    """
    try:
        if not brx_path:
            print_and_log_error("brx_path not provided, skipping BRX done JSON write.")
            return

        # get base name without extension
        base_name = filename.rsplit(".", 1)[0]
        base_key = base_name.upper() + ".TXT"  # your spec says BASENAME.TXT as key

        # Build JSON object
        files_list = []
        for f in written_files:
            files_list.append({
                "debatchedfilename": f"{f}",
                "dataFile": f"{f}.json",
                # "reportFile":        f"{f}.out.xml"
            })

        json_obj = {
            base_key: {
                "files": files_list
            }
        }

        # Dump JSON to temp file
        tmpf = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
        json.dump(json_obj, tmpf, indent=4)
        tmpf.write("\n")
        tmpf.close()

        # --- Normalize S3 destination ---
        if not brx_path.startswith("s3://"):
            out_bucket = os.getenv('s3_bucket_output')
            s3uri = f"s3://{out_bucket}/{brx_path}/{base_name}_done.json"
        else:
            s3uri = f"{brx_path}/{base_name}_done.json"

        upload_to_s3(tmpf.name, s3uri, label="brx_done_json")
        print_and_log_info(
            "BRX Done JSON created and uploaded",
            file=f"{base_name}_done.json",
            s3uri=s3uri,
            record_file_count=len(written_files)
        )

    except Exception as e:
        print_and_log_error(f"Error writing BRX done JSON: {str(e)}")
        log_event("FAILED", f"glue_debatch Error writing BRX done JSON: {str(e)}")
        raise


def account_number_split_and_upload(input_file_path, file_name,
                                    output_headers, output_trailers, output_records, output_folder,
                                    record_type, headers, trailers, account_info, debatch_kafka_path, done_folder_path,
                                    done_file_flag, is_seperate_header_required, is_seperate_trailer_required,
                                    combo_records=None):
    header_lines = []
    trailer_lines = []
    written_files = []
    account_seq = {}
    account_records_buffer = {}
    header_count = 0
    trailer_count = 0
    record_count = 0
    error_count = 0
    first_001_line = None

    client_id_str = f"{os.getenv('clientID', '').zfill(7)}"

    # Determine if combo logic applies
    has_combo = bool(combo_records and len(combo_records) > 1)

    print_and_log_info(f"Opening input file: {input_file_path}")
    try:
        with sfs.open(input_file_path, 'r', encoding='utf-8') as data:
            for line_number, line in enumerate(data, 1):

                # --- Skip empty or whitespace-only lines ---
                if not line.strip():
                    print_and_log_info(f"Skipping empty/whitespace line {line_number}")
                    continue

                # --- Skip lines shorter than minimum record length ---
                if len(line) < account_info['accountEnd'] + 1:
                    print_and_log_info(f"Skipping line {line_number} due to insufficient length", line_length=len(line))
                    continue

                # --- Skip unwanted metadata lines ---       
                if line.startswith("REC-CNT=") or line.startswith("DATE="):
                    print_and_log_info(f"Skipping unwanted metadata line {line_number}: {line.strip()}")
                    continue
                try:
                    line_type = line[record_type['recordStart']:record_type['recordEnd'] + 1]

                    # --- capture first 001 record ---
                    if line_type == "001" and first_001_line is None:
                        first_001_line = line.rstrip("\n")

                    # Check for header
                    if is_header(line, headers, record_type):
                        header_lines.append(line)
                        header_count += 1
                        # print_and_log_info("Header line detected", line_number=line_number, value=line.strip())
                        continue

                    # Check for trailer
                    if is_trailer(line, trailers, record_type):
                        trailer_lines.append(line)
                        trailer_count += 1
                        # print_and_log_info("Trailer line detected", line_number=line_number, value=line.strip())
                        continue

                    # Extract account number
                    account = get_account(line, account_info) or "UNKNOWN"

                    # === Combo record buffering ===
                    if has_combo and line_type in combo_records:
                        if account not in account_records_buffer:
                            account_records_buffer[account] = []
                        account_records_buffer[account].append(line)
                        continue  # Skip creating individual files for now

                    # === Accumulate record lines per account ===
                    if account not in account_records_buffer:
                        account_records_buffer[account] = []
                    account_records_buffer[account].append(line)

                except Exception as line_ex:
                    error_count += 1
                    print_and_log_error(f"Error in line processing: {str(line_ex)}", line_number=line_number)
                    log_event("FAILED",
                              f"glue_debatch error in line processing : {str(line_ex)} at line {line_number} in {input_file_path}")
                    raise

                # === Write one file per account ===
        for account, lines in account_records_buffer.items():
            try:
                base_name = file_name.rsplit(".", 1)[0]

                s3fname = f"{account}_{client_id_str}_{file_name}"
                s3uri = f"{output_records}/{s3fname}" if output_records.startswith('s3://') \
                    else f"s3://{output_records}/{s3fname}"

                if write_with_fln_header_socgen(lines, s3uri, s3fname, first_001_line, client_id_str, header_lines,
                                                label="account_records"):
                    written_files.append(s3fname)
                    record_count += len(lines)
                else:
                    error_count += 1
            except Exception as e:
                error_count += 1
                print_and_log_error(f"Error writing account file for {account}: {str(e)}")
                log_event("FAILED", f"glue_debatch Error writing account file for {account}: {str(e)}")
                raise

        # Header file
        if header_lines and str(is_seperate_header_required).strip().lower() in ["true", "1", "yes"]:
            try:
                tmpf = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
                tmpf.writelines(header_lines)
                tmpf.close()
                header_file_name = f"header_{file_name}"
                s3uri = f"{output_headers}/{header_file_name}" if output_headers.startswith(
                    's3://') else f"s3://{output_headers}/{header_file_name}"
                upload_to_s3(tmpf.name, s3uri, label="header")
                print_and_log_info("Header file created and uploaded", file=header_file_name, s3uri=s3uri,
                                   header_count=header_count)
            except Exception as e:
                error_count += 1
                print_and_log_error(f"Error writing header file: {str(e)}")
        else:
            print_and_log_info("No header lines found; header file not created.")

        # Trailer file
        if trailer_lines and str(is_seperate_trailer_required).strip().lower() in ["true", "1", "yes"]:
            try:
                tmpf = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
                tmpf.writelines(trailer_lines)
                tmpf.close()
                trailer_file_name = f"trailer_{file_name}"
                s3uri = f"{output_trailers}/{trailer_file_name}" if output_trailers.startswith(
                    's3://') else f"s3://{output_trailers}/{trailer_file_name}"
                upload_to_s3(tmpf.name, s3uri, label="trailer")
                print_and_log_info("Trailer file created and uploaded", file=trailer_file_name, s3uri=s3uri,
                                   trailer_count=trailer_count)
            except Exception as e:
                error_count += 1
                print_and_log_error(f"Error writing trailer file: {str(e)}")
        else:
            print_and_log_info("No trailer lines found; trailer file not created.")

        # Reconciliation report file
        try:
            recon_file_name = f"{file_name}.recon_rpt"
            tmpf = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
            tmpf.write("\n".join(written_files))
            tmpf.write("\n")
            tmpf.close()
            s3uri = f"{output_folder}/{recon_file_name}" if output_folder.startswith(
                's3://') else f"s3://{output_folder}/{recon_file_name}"
            upload_to_s3(tmpf.name, s3uri, label="recon_report")
            print_and_log_info("Recon report file created and uploaded", file=recon_file_name, s3uri=s3uri,
                               record_file_count=record_count)
        except Exception as e:
            error_count += 1
            print_and_log_error(f"Error writing recon report: {str(e)}")
            raise

        print_and_log_info(
            f"Processing complete for {input_file_path}. Records: {record_count}, Headers: {header_count}, Trailers: {trailer_count}, Errors: {error_count}"
        )

        try:
            if debatch_kafka_path:
                write_final_list(written_files, debatch_kafka_path, file_name)

            if done_file_flag and done_folder_path:
                write_brx_done_json(written_files, done_folder_path, file_name)

        except Exception as e:
            error_count += 1
            print_and_log_error(f"Error writing final list to kafka path: {str(e)}")
            log_event("FAILED", f"glue_debatch Error writing final list to kafka path : {str(e)}")
            raise

        if error_count > 0:
            f"Completed with {error_count} errors for {input_file_path}."

        else:
            print_and_log_info(
                f"Completed successfully for {input_file_path}."
            )

        return record_count, header_count, trailer_count, error_count
    except Exception as e:
        log_event("FAILED", f"glue_debatch error in split_and_upload : {str(e)}")
        print_and_log_error(f"Fatal error in split_and_upload: {str(e)}")
        raise


def split_and_upload(input_file_path, file_name,
                     output_headers, output_trailers, output_records, output_folder,
                     record_type, headers, trailers, account_info, combo_records=None, only_combo=True):
    header_lines = []
    trailer_lines = []
    written_files = []
    account_seq = {}
    account_records_buffer = {}
    header_count = 0
    trailer_count = 0
    record_count = 0
    error_count = 0
    first_001_line = None
    first_002_line = None
    first_001_lines = []
    first_002_lines = []

    client_id_str = f"{os.getenv('clientID', '').zfill(7)}"

    # Determine if combo logic applies
    has_combo = bool(combo_records and len(combo_records) > 1)

    log_event("INFO", f"Starting split_and_upload::input_file={input_file_path}, output_headers={output_headers}, "
                      f"output_trailers={output_trailers}, output_records={output_records}, output_folder={output_folder}, "
                      f"has_combo={has_combo}, record_type={record_type}, account_info={account_info}, combo_records={combo_records}, "
                      f"headers={headers}, trailers={trailers}, client_id={client_id_str}, file_name={file_name}")
    try:
        with sfs.open(input_file_path, 'r', encoding='utf-8') as data:
            for line_number, line in enumerate(data, 1):

                # --- Skip empty or whitespace-only lines ---
                if not line.strip():
                    log_event("INFO", f"Skipping empty/whitespace line {line_number} in {input_file_path}")
                    continue

                # --- Skip lines shorter than minimum record length ---
                if len(line) < account_info['accountEnd'] + 1:
                    log_event("INFO", f"Skipping line {line_number} due to insufficient length in {input_file_path}")
                    continue

                try:
                    line_type = line[record_type['recordStart']:record_type['recordEnd'] + 1]

                    # --- capture first 001 record ---
                    if line_type == "001" and first_001_line is None:
                        first_001_line = line.rstrip("\n")

                    # --- capture first 002 record with sub firm 001
                    if line_type == "002" and first_002_line is None and line[9:12] == "001":
                        first_002_line = line.rstrip("\n")

                    if line_type == "001":
                        first_001_lines.append(line.rstrip('\n'))
                    if line_type == "002":
                        first_002_lines.append(line.rstrip('\n'))

                    # Check for header
                    if is_header(line, headers, record_type):
                        header_lines.append(line)
                        header_count += 1
                        continue

                    # Check for trailer
                    if is_trailer(line, trailers, record_type):
                        trailer_lines.append(line)
                        trailer_count += 1
                        continue

                    # Extract account number
                    account = get_account(line, account_info) or "UNKNOWN"

                    # === Combo record buffering ===
                    if has_combo and line_type in combo_records:
                        if account not in account_records_buffer:
                            account_records_buffer[account] = []
                        account_records_buffer[account].append(line)
                        continue  # Skip creating individual files for now

                    if only_combo:
                        # Skip writing individual record files
                        continue

                    # === Default behavior for non-combo lines ===
                    seq = account_seq.get(account, 1)
                    seqnum = str(seq).zfill(7)
                    s3fname = f"{account}_{client_id_str}_{file_name}_{line_type}_{seqnum}.txt"
                    s3uri = f"{output_records}/{s3fname}" if output_records.startswith(
                        's3://') else f"s3://{output_records}/{s3fname}"

                    if write_with_fln_header(line, s3uri, s3fname, first_001_lines, first_002_lines, client_id_str,
                                             label="record"):
                        written_files.append(s3fname)
                        account_seq[account] = seq + 1
                        record_count += 1
                    else:
                        error_count += 1

                except Exception as line_ex:
                    error_count += 1
                    message = f"Error processing line {line_number} in {input_file_path}: {str(line_ex)}"
                    raise Exception(message) from line_ex

        # === process combo records ===
        if has_combo:
            for account, lines in account_records_buffer.items():
                # Determine if a combo is valid (needs >1 unique record types)
                rec_types_for_account = set(
                    l[record_type['recordStart']:record_type['recordEnd'] + 1]
                    for l in lines
                )
                if len(rec_types_for_account) > 1:
                    # Create combo file
                    try:
                        tmpf = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
                        tmpf.writelines(lines)
                        tmpf.close()
                        s3fname = f"{account}_{client_id_str}_{file_name}_COMBO.txt"
                        s3uri = f"{output_records}/{s3fname}" if output_records.startswith('s3://') \
                            else f"s3://{output_records}/{s3fname}"

                        if write_with_fln_header(lines, s3uri, s3fname, first_001_lines, first_002_lines, client_id_str,
                                                 label="combo_record"):
                            written_files.append(s3fname)
                            record_count += len(lines)
                        else:
                            error_count += 1
                    except Exception as e:
                        error_count += 1
                        message = f"Error creating combo file for account {account}: {str(e)}"
                        raise Exception(message) from e

                else:
                    # Fallback - behave as normal individual files
                    seq = account_seq.get(account, 1)
                    for line in lines:
                        line_type = line[record_type['recordStart']:record_type['recordEnd'] + 1]
                        seqnum = str(seq).zfill(7)
                        s3fname = f"{account}_{client_id_str}_{file_name}_{line_type}_{seqnum}.txt"
                        s3uri = f"{output_records}/{s3fname}" if output_records.startswith('s3://') \
                            else f"s3://{output_records}/{s3fname}"
                        try:
                            if write_with_fln_header(line, s3uri, s3fname, first_001_lines, first_002_lines,
                                                     client_id_str, label="record"):
                                written_files.append(s3fname)
                                seq += 1
                                record_count += 1
                            else:
                                error_count += 1
                        except Exception as e:
                            error_count += 1
                            message = f"Error processing/writing non-combo fallback for account {account}: {str(e)}"
                            raise Exception(message) from e
                    account_seq[account] = seq

        # Header file
        if header_lines:
            try:
                tmpf = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
                tmpf.writelines(header_lines)
                tmpf.close()
                header_file_name = f"header_{file_name}.txt"
                s3uri = f"{output_headers}/{header_file_name}" if output_headers.startswith(
                    's3://') else f"s3://{output_headers}/{header_file_name}"
                upload_to_s3(tmpf.name, s3uri, label="header")
                log_event("INFO",
                          f"Header file created and uploaded: {header_file_name} to {s3uri}, header_count={header_count}")
            except Exception as e:
                error_count += 1
                log_event("WARN", f"Error writing header file : {str(e)}")
        else:
            log_event("INFO", "No header lines found; header file not created.")

        # Trailer file
        if trailer_lines:
            try:
                tmpf = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
                tmpf.writelines(trailer_lines)
                tmpf.close()
                trailer_file_name = f"trailer_{file_name}.txt"
                s3uri = f"{output_trailers}/{trailer_file_name}" if output_trailers.startswith(
                    's3://') else f"s3://{output_trailers}/{trailer_file_name}"
                upload_to_s3(tmpf.name, s3uri, label="trailer")
                log_event("INFO",
                          f"Trailer file created and uploaded: {trailer_file_name} to {s3uri}, trailer_count={trailer_count}")
            except Exception as e:
                error_count += 1
                log_event("WARN", f"Error writing trailer file : {str(e)}")
        else:
            log_event("INFO", "No trailer lines found; trailer file not created.")

        # Reconciliation report file
        try:
            recon_file_name = f"{file_name}.recon_rpt"
            tmpf = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
            tmpf.write("\n".join(written_files))
            tmpf.write("\n")
            tmpf.close()
            s3uri = f"{output_folder}/{recon_file_name}" if output_folder.startswith(
                's3://') else f"s3://{output_folder}/{recon_file_name}"
            upload_to_s3(tmpf.name, s3uri, label="recon_report")
            log_event("INFO",
                      f"Recon report created and uploaded: {recon_file_name} to {s3uri}, record_file_count={record_count}")
        except Exception as e:
            error_count += 1
            message = f"Error writing recon report file : {str(e)}"
            raise Exception(message) from e

        log_event("INFO",
                  f"Processing complete for {input_file_path}. Records: {record_count}, Headers: {header_count}, Trailers: {trailer_count}, Errors: {error_count}")
        if error_count > 0:
            log_event("WARN", f"Completed with {error_count} errors for {input_file_path}.")

        else:
            log_event("INFO", f"Completed successfully for {input_file_path}.")

        return record_count, header_count, trailer_count, error_count
    except Exception as e:
        message = f"Fatal error in split_and_upload: {str(e)}"
        raise Exception(message) from e


def write_with_fln_header(lines, s3uri, file_name, first_001_lines, first_002_lines, client_id_str, label="record"):
    """
    Writes a file with the FLN header and all 001/002 lines.
    - first_001_lines and first_002_lines should be lists of strings.
    """
    try:
        tmpf = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')
        fln_filename = "FLN" + file_name + "_" + TS_STRING
        tmpf.write(fln_filename + "\n")
        tmpf.write("CLI" + client_id_str + "\n")
        # Write all 001 lines
        if first_001_lines:
            for l in first_001_lines:
                tmpf.write(l.rstrip('\n') + "\n")
        # Write all 002 lines
        if first_002_lines:
            for l in first_002_lines:
                tmpf.write(l.rstrip('\n') + "\n")
        # Write the rest of the file contents
        if isinstance(lines, list):
            tmpf.writelines(lines)
        else:
            tmpf.write(lines)
        tmpf.close()

        upload_to_s3(tmpf.name, s3uri, label=label)
        return True
    except Exception as e:
        message = f"Error writing {label} file with FLN header: {str(e)}"
        raise Exception(message) from e


def write_with_fln_header_socgen(lines, s3uri, file_name, first_001_line, client_id_str, header_lines, label="record"):
    """Writes a file with the FLN 3-line header inside output_records folder"""
    try:
        tmpf = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8')

        fln_filename = "FLN" + file_name + "_" + TS_STRING
        # tmpf.write(fln_filename + "\n")
        # tmpf.write("CLI" + client_id_str + "\n")
        # tmpf.write(first_001_line + "\n")
        tmpf.writelines(header_lines)
        tmpf.write((first_001_line or " ") + "\n")
        # Then write rest of file contents
        if isinstance(lines, list):
            tmpf.writelines(lines)
        else:
            tmpf.write(lines)
        tmpf.close()

        upload_to_s3(tmpf.name, s3uri, label=label)
        return True
    except Exception as e:
        print_and_log_error(f"Error writing file with FLN header: {str(e)}")
        log_event("FAILED", f"glue_debatch Error writing file with FLN header : {str(e)}")
        raise


def set_job_params_as_env_vars():
    messages = []
    messages.append(f"Set environment variable:")
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i + 1]
            os.environ[key] = value
            messages.append(f"{key}={value}")

    log_event("INFO", f"glue_debatch::Environment variables set from job parameters: {', '.join(messages)}")


def main():
    try:
        set_job_params_as_env_vars()

        in_bucket = os.getenv('s3_bucket_input')
        in_file = os.getenv('s3_input_file')
        output_folder = os.getenv('s3_output_path')
        config = os.getenv('config')
        # debatch_kafka = os.getenv("debatch_kafka")
        # done_folder_path=os.getenv('done_folder_path')
        # done_file_flag=os.getenv('done_file_flag')
        debatch_kafka = os.getenv("debatch_kafka", " ")
        done_folder_path = os.getenv('done_folder_path', " ")
        done_file_flag = os.getenv('done_file_flag', "False")
        is_seperate_trailer_required = os.getenv('is_seperate_trailer_required', "False")
        is_seperate_header_required = os.getenv('is_seperate_header_required', "False")

        if not in_bucket or not in_file or not config:
            msg = "Missing required configuration/environment variable."
            raise ValueError(msg)

        file_name = in_file.rsplit("/", 1)[-1]
        global inputfile
        inputfile = file_name

        # START log
        log_event("STARTED",
                  f"glue_debatch started processing {file_name} with input_bucket={in_bucket}, output_folder={output_folder}, config keys={list(json.loads(config).keys()) if config else 'N/A'}")

        # Parse config
        (record_type, headers, trailers, account_info, output_headers, output_trailers, output_records,
         combo_records, trig_folder, create_trig_flag, only_combo, use_account_number_debatch) = parse_client_config(
            config)

        input_file_path = f"s3://{in_bucket}/{in_file}"

        # Normalize S3 paths
        if not output_headers.startswith('s3://'):
            out_bucket = os.getenv('s3_bucket_output')
            output_headers = f"s3://{out_bucket}/{output_headers}"
        if not output_trailers.startswith('s3://'):
            out_bucket = os.getenv('s3_bucket_output')
            output_trailers = f"s3://{out_bucket}/{output_trailers}"
        if not output_records.startswith('s3://'):
            out_bucket = os.getenv('s3_bucket_output')
            output_records = f"s3://{out_bucket}/{output_records}"
        if not output_folder.startswith('s3://'):
            out_bucket = os.getenv('s3_bucket_output')
            output_folder = f"s3://{out_bucket}/{output_folder}"

        # Run split/upload
        if use_account_number_debatch in ["True", "true",
                                          True]:  # Split will happen based on account number , refer to bpt52
            record_count, header_count, trailer_count, error_count = account_number_split_and_upload(
                input_file_path, file_name,
                output_headers, output_trailers, output_records, output_folder,
                record_type, headers, trailers, account_info, debatch_kafka, done_folder_path, done_file_flag,
                is_seperate_header_required, is_seperate_trailer_required, combo_records
            )
        else:
            record_count, header_count, trailer_count, error_count = split_and_upload(
                input_file_path, file_name,
                output_headers, output_trailers, output_records, output_folder,
                record_type, headers, trailers, account_info, combo_records, only_combo
            )
            # --- Create TRIG file if required ---
            try:
                create_trig_file(trig_folder, create_trig_flag)
            except Exception as trig_ex:
                log_event("WARN", f"glue_debatch TRIG creation failed: {str(trig_ex)}")

        # --- Job completion ---
        if error_count > 0:
            msg = f"glue_debatch completed with {error_count} errors"
            raise Exception(msg)
        else:
            msg = f"glue_debatch succeeded. Records: {record_count}, Headers: {header_count}, Trailers: {trailer_count}"
            log_event("ENDED", msg)

    except Exception as e:
        log_event("FAILED", f"glue_debatch::Fatal Error={traceback.format_exc()}::Exception::{e}")
        raise e


if __name__ == '__main__':
    main()
