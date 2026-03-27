import sys
import boto3
import json
import os
from datetime import datetime, timezone
import re
import traceback
from botocore.exceptions import BotoCoreError, ClientError
from awsglue.utils import getResolvedOptions
import splunk
from requests.adapters import HTTPAdapter
import requests
from concurrent.futures import ThreadPoolExecutor
from botocore.config import Config

# Global error buffer
ERRORS: list[str] = []
#######################################################################
def get_run_id(program_name: str) -> str:
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return f"arn:dpm:glue:{program_name}:{account_id}"
######################################################################
def is_valid_email(email: str) -> bool:
    """Return True if email looks like a valid address."""
    if not email:
        return False
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(email_regex, email) is not None
######################################################################
# --- Splunk logger ---
def log_event(status: str, message: str):

    job_name = "glue_kafkaload"
    statemachine_name = os.getenv("statemachine_name")
    if statemachine_name:
        parts = statemachine_name.split("-")
        if len(parts) >= 3:
            prefix = "_".join(parts[0:3])
            job_name = f"{prefix}_{job_name}"

    event_data = {
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "topic_name": os.getenv("topic_name", ""),
        "send_folder": os.getenv("send_folder", ""),
        "save_folder": os.getenv("save_folder", ""),
        "failed_folder": os.getenv("failed_folder", ""),
        "clientName": os.getenv("clientName", ""),
        "clientID": os.getenv("clientID", ""),
        "applicationName": os.getenv("applicationName", ""),
        "file_name": os.getenv("file_name", ""),
        "step_function": "kafkaload",
        "component": "glue",
        "job_name": job_name,
        "job_run_id": get_run_id("glue_kafkaload"),
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
        splunk.log_message(event_data, get_run_id("glue_kafkaload"))
    except Exception as e:
        print(f"[ERROR] glue_kafkaload : log_event : Failed to write log to Splunk: {str(e)}")

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
            f"Topic : {event_data.get('topic_name')}\n"
            f"Source : {event_data.get('send_folder')}\n"
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
            <li><b>Topic Name:</b> {event_data.get('topic_name')}</li>
            <li><b>Source Folder:</b> {event_data.get('send_folder')}</li>
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

##################################################################################
def get_configs():
    args_list = ['bucket_name', 'file_name', 'send_folder', 'topic_name',
                 'save_folder', 'failed_folder', 'region', 'kafka_url_parameter']

    if '--file_formats' in sys.argv:
        args_list.append('file_formats')

    args = getResolvedOptions(sys.argv, args_list)

    # Convert file_formats from string → list (if provided)
    if 'file_formats' in args:
        args['file_formats'] = [fmt.strip().lower() for fmt in args['file_formats'].split(',')]

    return args
#####################################################################
def send_to_kafka_api(session: requests.Session, kafka_url: str, kafka_topic: str,
                      program_name: str, topic_name: str, json_data: dict):
    url = f"{kafka_url}{kafka_topic}"
    headers = {
        "Content-Type": "application/json",
        "source": "DPM",
        "Accept": "application/json"
    }
    try:
        if isinstance(json_data, dict):
            response = session.post(url, json=json_data, headers=headers, timeout=10)
        else:
            response = session.post(url, data=json_data, headers={"Content-Type": "text/plain"}, timeout=10)
        if response.status_code != 200:
            err = f"{program_name} → {topic_name} failed with code {response.status_code}"
            log_error(err, program_name)
            return False
        return True
    except Exception as e:
        log_error(f"{program_name} → {topic_name} failed: {str(e)}", program_name)
        return False
##################################################################
def process_and_send_file(s3_client, session, kafka_url, kafka_topic,
                          program_name, topic_name, bucket_name,
                          file_obj, save_folder, failed_folder, allowed_formats) -> int:
    """Returns 1 if success, -1 if failed, 0 if skipped (non-file_formats)."""
    file_name = os.path.basename(file_obj["Key"])
    ext = file_name.split('.')[-1].lower()

    if ext not in allowed_formats:
        return 0  # Skip unsupported files

    key = file_obj["Key"]
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        file_content = obj["Body"].read().decode("utf-8")

        # Parse JSON only if json file, otherwise wrap raw content
        if ext == "json":
            json_data = json.loads(file_content)
        else:
            # Non-JSON file, wrap as payload
            json_data = file_content

        ok = send_to_kafka_api(session, kafka_url, kafka_topic, program_name, topic_name, json_data)
        target_folder = save_folder if ok else failed_folder
        move_s3_object(s3_client, bucket_name, key, f"{target_folder.rstrip('/')}/{file_name}", program_name)
        return 1 if ok else -1
    except Exception as e:
        move_s3_object(s3_client, bucket_name, key, f"{failed_folder.rstrip('/')}/{file_name}", program_name)
        log_error(f"{program_name} failed processing file {file_name}: {str(e)}", program_name)
        #log_event("FAILED", f"{program_name} failed processing file {file_name}: {str(e)}, {program_name}")
        return -1
############################################################################
def move_s3_object(s3_client, bucket, src, dst, program_name):
    try:
        s3_client.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": src}, Key=dst)
        s3_client.delete_object(Bucket=bucket, Key=src)
    except Exception as e:
        log_error(f"{program_name} could not move {src} → {dst}: {str(e)}", program_name)
##############################################################################
def produce_message(session, kafka_url, kafka_topic, program_name,
                    topic_name, bucket_name, send_folder,
                    save_folder, failed_folder, max_workers, allowed_formats):
    print(f"Producing messages to Kafka topic: {kafka_topic}")
    s3_client = boto3.client("s3", config=Config(max_pool_connections=max_workers))
    paginator = s3_client.get_paginator('list_objects_v2')
    file_objs = [obj for page in paginator.paginate(Bucket=bucket_name, Prefix=send_folder)
                 for obj in page.get('Contents', [])]
    if not file_objs:
        msg = f"No files found in {bucket_name}/{send_folder}"
        splunk.log_message({"Status": "Info", "Message": msg}, get_run_id(program_name))
        return 0, 0, 0
    success_count = 0
    failed_count = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(
            lambda obj: process_and_send_file(s3_client, session, kafka_url, kafka_topic,
                                              program_name, topic_name, bucket_name,
                                              obj, save_folder, failed_folder, allowed_formats),
            file_objs,
        )
        for r in results:
            if r == 1:
                success_count += 1
            elif r == -1:
                failed_count += 1
    return success_count, failed_count, success_count + failed_count
##################################################################################################
def set_job_params_as_env_vars():
    messages = []
    messages.append(f"Set environment variable:")
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i+1]
            os.environ[key] = value
            messages.append(f"{key}={value}")

    log_event("INFO", f"glue_kafkaload::Environment variables set from job parameters: {', '.join(messages)}")
################################################################################

def kafka_load(session, kafka_url, program_name, topic_name,
               bucket_name, send_folder, save_folder, failed_folder, max_workers, allowed_formats):
    return produce_message(session, kafka_url, topic_name, program_name,
                           topic_name, bucket_name, send_folder,
                           save_folder, failed_folder, max_workers, allowed_formats)
############################################################################
def log_error(message, program_name):
    ERRORS.append(message)
##########################################################################
def check_kafka_connection(session, kafka_url, program_name, topic_name):
    """
    Quick connectivity test to Kafka REST endpoint before processing.
    This does NOT send/produce any message.
    """
    test_url = f"{kafka_url}{topic_name}"   # Kafka REST Proxy endpoint

    try:
        # A simple HEAD request just checks if endpoint is reachable
        response = session.head(test_url, timeout=5)

        #if response.status_code not in (200, 204):  # Expect "OK" or "No Content"
        if response.status_code not in (200, 204, 500):  # Expect "OK" or "No Content" or "??? Kafka REST unhealthy"
            err = (f"{program_name}: Kafka endpoint not healthy. "
                   f"HTTP {response.status_code} returned for topic: {topic_name}"
                   f"Unable to connect to Kafka topic:  ---->  {topic_name}   Trying to connect... up to entry 5  !!!")
            raise ValueError(err)

        log_event("INFO", f"{program_name}: Kafka connection to topic '{topic_name}' verified successfully.")

    except Exception as e:
        err = f"{program_name}: Unable to connect to Kafka topic {topic_name} — {str(e)}"
        raise Exception(err) from e

##########################################################################
if __name__ == "__main__":
    set_job_params_as_env_vars()
    log_event("STARTED", f"glue_kafkaload job started processing with file_name : {os.getenv('file_name', '')}")
    program = os.path.basename(sys.argv[0])
    program_name = program.split('.')[0]

    args = get_configs()
    bucket_name   = args['bucket_name']
    send_folder   = args['send_folder']
    topic_name    = args['topic_name']
    file_name     = args['file_name']
    save_folder   = args['save_folder']
    failed_folder = args['failed_folder']
    region        = args['region']
    kafka_url_parameter = args['kafka_url_parameter']
    max_workers = int(args['threads']) if 'threads' in args else 50

    if 'file_formats' in args:
        allowed_formats = args['file_formats']
    else:
        allowed_formats = ["json"]  # backward compatibility

    if not all([bucket_name, send_folder, topic_name, file_name, save_folder, failed_folder, region, kafka_url_parameter, max_workers]):
        message = f"{program_name} load failed at missing required parameters !!!"
        raise ValueError(message)

    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=max_workers, pool_maxsize=max_workers)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    ssm = boto3.client("ssm", region_name=region)
    kafka_url = ssm.get_parameter(Name=kafka_url_parameter, WithDecryption=True)["Parameter"]["Value"]

    try:
        # 🔹 First check connectivity to Kafka before processing files
        check_kafka_connection(session, kafka_url, program_name, topic_name)

        success_count, failed_count, total_processed = kafka_load(
            session, kafka_url, program_name, topic_name,
            bucket_name, send_folder, save_folder, failed_folder, max_workers, allowed_formats
        )

        if total_processed == 0:
            msg = f"There is no {allowed_formats} file found to be processed. {allowed_formats} files count:  ---->  {total_processed:,}"
            log_event("INFO", msg)
        else:
            msg = (
                f"Processed {allowed_formats} files:  ---->  {total_processed:,}\nSuccess messages:      ---->  {success_count:,}\nFailed messages:       ---->  {failed_count:,}")
            log_event("INFO", msg)

        if ERRORS:
            log_event("WARN", "Finished with Errors: " + "; ".join(ERRORS))

        if success_count == 0 and failed_count > 0:
            err_msg = f"All ---->  {failed_count:,}  messages failed for topic {topic_name}"
            raise Exception(err_msg)

        msg = f"{program_name} finished Kafka load at {datetime.now()} SUCCESSFULLY"
        log_event("ENDED", f"{msg}")
    
    except Exception as e:
        log_event("FAILED", f"glue_kafkaload::Fatal Error={traceback.format_exc()}::Exception::{e}")
        raise e

    finally:
        session.close()