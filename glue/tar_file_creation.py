import boto3
import os
import re
import tarfile
import json
import sys
import datetime as dt
import splunk
import logging
from awsglue.utils import getResolvedOptions
import time
import traceback
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed

# Initialize the boto3 S3 client
s3 = boto3.client(
    "s3",
    config=Config(
        retries={'max_attempts': 15, 'mode': 'adaptive'},
        connect_timeout=60,
        read_timeout=600,
        max_pool_connections=50,
    ),
)

# Initialize logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

AWS_ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

def get_job_name():
    job_name = "glue_tar_file_creation"
    statemachine_name = os.getenv("statemachine_name")
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

# --- Splunk logger ---
def log_event(status: str, message: str, tar_file_name=" "):

    event_data = {
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "transactionId": os.getenv("transactionId", " "),
        "clientName": os.getenv("clientName", " "),
        "clientID": os.getenv("clientID", " "),
        "applicationName": os.getenv("applicationName", " "),
        "order_ids": [],
        "step_function": "compression",
        "component": "glue",
        "job_name": get_job_name(),
        "job_run_id": get_run_id(),
        "compressed_file": tar_file_name,
        "execution_name": os.getenv("execution_name", " "),
        "execution_id": os.getenv("execution_id", ""),
        "execution_starttime": os.getenv("execution_starttime", " "),
        "statemachine_name": os.getenv("statemachine_name", " "),
        "statemachine_id": os.getenv("statemachine_id", " "),
        "state_name": os.getenv("state_name", " "),
        "state_enteredtime": os.getenv("state_enteredtime", " "),
        "status": status,
        "message": message,
        "aws_account_id": AWS_ACCOUNT_ID,
    }

    print(json.dumps(event_data))

    try:
        splunk.log_message(event_data, get_run_id())
    except Exception as e:
        print(f"[ERROR] glue_tar_file_creation : log_event : Failed to write log to Splunk: {str(e)}")

    if status == "FAILED" and is_valid_email(os.getenv("teamsId")):

        glue_link = (
            f"https://us-east-1.console.aws.amazon.com/gluestudio/home?region=us-east-1"
            f"#/editor/job/{event_data['job_name']}/runs"
        )

        stepfn_link = (
            f"https://us-east-1.console.aws.amazon.com/states/home?region=us-east-1"
            f"#/v2/executions/details/{event_data['execution_id']}"
        )

        subject = f"[ALERT] Glue job Failure - {event_data['job_name']} ({status})"

        body_text = (
            f"Glue job failed.\n\n"
            f"Event Time: {event_data['event_time']}\n"
            f"Client: {event_data['clientName']} ({event_data['clientID']})\n"
            f"Job: {event_data['job_name']}\n"
            f"Order Ids: {event_data['order_ids']}\n"
            f"TransactionId: {event_data['transactionId']}\n"
            f"Status: {event_data['status']}\n"
            f"Message: {event_data['message']}\n"
            f"Glue Job: {glue_link}\n"
            f"Step Function Execution: {stepfn_link}\n"
        )

        body_html = f"""<html>
        <body>
          <h3>Glue Job Failure Notification</h3>
          <ul>
            <li><b>Event Time:</b> {event_data['event_time']}</li>
            <li><b>Client:</b> {event_data['clientName']} ({event_data['clientID']})</li>
            <li><b>Job Name:</b> {event_data['job_name']}</li>
            <li><b>Order Ids:</b> {event_data['order_ids']}</li>
            <li><b>TransactionId:</b> {event_data['transactionId']}</li>
            <li><b>Status:</b> {event_data['status']}</li>
            <li><b>Message:</b> {event_data['message']}</li>
          </ul>
          <p><a href="{glue_link}">View Glue Job</a></p>
          <p><a href="{stepfn_link}">View Step Function Execution</a></p>
        </body>
        </html>"""

        sender = os.getenv("ALERT_SENDER_EMAIL", "noreply@broadridge.com")
        ses_client = boto3.client("ses", region_name="us-east-1")

        try:
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
            print(f"Email sent! sender: {sender}")
            print("SES send_email full response:\n", json.dumps(response, indent=2))
        except ClientError as e:
            print(f"[ERROR] Failed to send email: {str(e)}")


def find_files(bucket, file_path_list, file_pattern_list):
    
    matched_files = []
    matched_meta_file = None

    for prefix, pattern in zip(file_path_list, file_pattern_list):
        paginator = s3.get_paginator('list_objects_v2')
        files_found = False
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            files = page.get('Contents', [])
            if files:
                files_found = True
            for file in files:
                key = file['Key']
                if re.match(pattern, os.path.basename(key)):
                    if key.endswith('.xml') or key.endswith('.json'):
                        matched_meta_file = key
                    else:
                        matched_files.append(key)
        if not files_found:
            log_event("FAILED", f"glue_tar_file_creation : No matching file pattern {pattern} in {prefix}", f"{os.getenv('tarDestinationPath', '')}{os.getenv('tarFileName', '')}")
            raise FileNotFoundError(f"No file matching pattern {pattern} in {prefix}")
    
    return matched_files, matched_meta_file

def extract_client_id_and_document_type(file_name):
    # Assuming the file name format is UR_DP_PDF.cbu.MLGTO.WSMPDF.xml
    parts = file_name.split('.')
    client_id = parts[2] if len(parts) > 2 else None
    document_type = parts[3] if len(parts) > 3 else None
    return client_id, document_type

class TarCreator:
    def __init__(self, bucket, file_keys, meta_file_key, tar_destination_path, tar_destination_prefix, tar_file_name, tar_type, corp, cycle):
        self.bucket = bucket
        self.file_keys = file_keys
        self.meta_file_key = meta_file_key
        self.tar_destination_path = tar_destination_path
        self.tar_destination_prefix = tar_destination_prefix
        self.tar_file_name = tar_file_name
        self.tar_type = tar_type
        self.corp = corp if corp else '99999'
        self.cycle = cycle if cycle else '9'


    def create_tar_file(self):
        try:
            if not self.file_keys or not self.meta_file_key:
                raise Exception("No input files found.")

            # tar name and mode
            if self.tar_type == "UR_TAR":
                client_id, document_type = extract_client_id_and_document_type(os.path.basename(self.meta_file_key))
                timestamp = dt.datetime.now().strftime("%Y%m%d%H%M")
                tar_file_name = f"{self.tar_destination_prefix}.cbu.{client_id}.{document_type}.{self.corp}.{self.cycle}.{timestamp}.tar"
                tar_mode = "w"
            else:
                tar_file_name = self.tar_file_name + ".tgz"
                tar_mode = "w:gz"

            all_keys = self.file_keys + [self.meta_file_key]
            start_time = time.time()
            processed = 0
            local_tar_path = f"/tmp/{tar_file_name}"

            with tarfile.open(local_tar_path, mode=tar_mode) as tar:
                with ThreadPoolExecutor(max_workers=15) as pool:
                    futures = {}
                    for k in all_keys:
                        local_path = os.path.join("/tmp", os.path.basename(k))
                        futures[pool.submit(s3.download_file, self.bucket, k, local_path)] = (k, local_path)

                    for f in as_completed(futures):
                        key, local_path = futures[f]
                        f.result()
                        tar.add(local_path, arcname=os.path.basename(key))
                        try:
                            os.remove(local_path)
                        except OSError as e:
                            log_event("WARNING", f"Could not delete temporary file {local_path}: {e}", os.path.join(self.tar_destination_path, tar_file_name))

                        processed += 1
                        if processed % 500 == 0 or processed == len(all_keys):
                            elapsed = round(time.time() - start_time, 2)
                            log_event("INFO",
                                f"Processed {processed}/{len(all_keys)} files in {elapsed} seconds",
                                os.path.join(self.tar_destination_path, tar_file_name),
                            )

            s3.upload_file(local_tar_path,
                        self.bucket,
                        os.path.join(self.tar_destination_path, tar_file_name))
            try:
                os.remove(local_tar_path)
            except OSError as e:
                log_event("WARNING", f"Could not delete temporary file {local_tar_path}: {e}", os.path.join(self.tar_destination_path, tar_file_name))

            duration = round(time.time() - start_time, 2)
            log_event("ENDED",
                    f"Created {tar_file_name} with {processed} files in {duration} s",
                    os.path.join(self.tar_destination_path, tar_file_name))

        except Exception as e:
            raise Exception(f"Tar file creation failed: {e}")
        
def set_job_params_as_env_vars():
    messages = []
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i+1]
            os.environ[key] = value
            messages.append(f"{key}={value}")

    log_event("INFO", f"Env vars set from job params: {', '.join(messages)}")


def main():
    try:
        set_job_params_as_env_vars()
        log_event("STARTED", f"glue_tar_file_creation job started processing transactionId : {os.getenv('transactionId', '')}", f"{os.getenv('tarDestinationPath', '')}{os.getenv('tarFileName', '')}")
        # Get job arguments using getResolvedOptions
        # args = getResolvedOptions(sys.argv, [
        #     'inputFileName', 
        #     's3_bucket_output',
        #     's3_bucket_input',
        #     'numberOfFilesType', 
        #     'filePathList', 
        #     'xmlPath', 
        #     'filePatternList', 
        #     'xmlPattern', 
        #     'tarDestinationPath', 
        #     'tarDestinationPrefix',
        #     'corp',
        #     'cycle'
        # ])
    
        global input_file_name, tar_file_name
        input_file_name = os.getenv('inputFileName')
    
        bucket = os.getenv('s3BucketInput')
        s3_bucket_output = os.getenv('s3BucketOutput')
        file_path_list = json.loads(os.getenv('filePathList'))
        file_pattern_list = json.loads(os.getenv('filePatternList'))
        tar_destination_path = os.getenv('tarDestinationPath')
        tar_type = os.getenv('tarType')
        tar_destination_prefix = os.getenv('tarDestinationPrefix')
        tar_file_name = os.getenv('tarFileName')
        corp=os.getenv('corp')
        cycle=os.getenv('cycle')
        # corp = args.get('corp', '99999')  # Default value if corp is not provided
        # cycle = args.get('cycle', '9')    # Default value if cycle is not provided

        # Log the start of the tar file creation process
        log_event("INFO", f"glue_tar_file_creation : Starting tar file creation process for transactionId : {os.getenv('transactionId', '')}", f"{tar_destination_path}{tar_file_name}")

        # Find files matching the patterns
        file_keys, meta_file_key = find_files(bucket, file_path_list, file_pattern_list)
        
        # if len(file_keys) != number_of_files_type or not xml_key:
        #     log_message('failed', f"Could not find files matching patterns: {file_pattern_list}, {xml_pattern}")
        #     raise Exception(f"Could not find files matching patterns: {file_pattern_list}, {xml_pattern}")

        # Create TarCreator object and create the tar file
        tar_creator = TarCreator(s3_bucket_output, file_keys, meta_file_key, tar_destination_path, tar_destination_prefix, tar_file_name, tar_type, corp, cycle)
        tar_creator.create_tar_file()
    
    except Exception as e:
        log_event("FAILED", f"glue_tar_file_creation : Tar file creation failed: Error: {traceback.format_exc()}, Exception : {str(e)}", f"{os.getenv('tarDestinationPath', '')}{os.getenv('tarFileName', '')}")
        raise e

input_file_name = ''
if __name__ == '__main__':
    main()
