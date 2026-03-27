import os
import sys
import splunk
import traceback
import boto3 # type: ignore
import re
import json
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from dataPostgres import DataPostgres
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
s3 = boto3.client('s3')

import itertools

AWS_ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]


def get_job_name():
    job_name = "glue_copy_files"
    statemachine_name = os.getenv("statemachine_name")
    if statemachine_name:
        parts = statemachine_name.split("-")
        if len(parts) >= 3:
            prefix = "_".join(parts[0:3])
            job_name = f"{prefix}_{job_name}"
    return job_name


def get_run_id():
    return f"arn:dpm:glue:{get_job_name()}:{AWS_ACCOUNT_ID}"


def set_job_params_as_env_vars():
    messages = []
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i+1]
            os.environ[key] = value
            messages.append(f"{key}={value}")

    log_event("INFO", f"Env vars set from job params: {', '.join(messages)}")


def is_valid_email(email: str) -> bool:
    """Return True if email looks like a valid address."""
    if not email:
        return False
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(email_regex, email) is not None

def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

# --- Splunk logger ---
def log_event(status: str, message: str, all_order_ids_for_logging=None):

    if all_order_ids_for_logging is None:
        all_order_ids_for_logging = []

    chunk_size = 1000

    if len(all_order_ids_for_logging) > chunk_size:
        for idx, order_chunk in enumerate(chunk_list(all_order_ids_for_logging, chunk_size), start=1):
            batched_message = f"{message} :: (Batch {idx} of { (len(all_order_ids_for_logging) // chunk_size) + 1 })"
            event_data = {
                "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "transactionId": os.getenv("transactionId", " "),
                "clientName": os.getenv("clientName", " "),
                "clientID": os.getenv("clientID", " "),
                "applicationName": os.getenv("applicationName", " "),
                "order_ids": order_chunk,
                "step_function": "orders_data_aggregation",
                "component": "glue",
                "job_name": get_job_name(),
                "job_run_id": get_run_id(),
                "compressed_file": " ",
                "execution_name": os.getenv("execution_name", " "),
                "execution_id": os.getenv("execution_id", " "),
                "execution_starttime": os.getenv("execution_starttime", " "),
                "statemachine_name": os.getenv("statemachine_name", " "),
                "statemachine_id": os.getenv("statemachine_id", " "),
                "state_name": os.getenv("state_name", " "),
                "state_enteredtime": os.getenv("state_enteredtime", " "),
                "status": status,
                "message": batched_message,
                "aws_account_id": AWS_ACCOUNT_ID,
            }
            print(json.dumps(event_data))
            try:
                splunk.log_message(event_data, get_run_id())
            except Exception as e:
                print(f"[ERROR] glue_copy_files : log_event : Failed to write log batch {idx} to Splunk: {str(e)}")

    else:
        event_data = {
            "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "transactionId": os.getenv("transactionId", " "),
            "clientName": os.getenv("clientName", " "),
            "clientID": os.getenv("clientID", " "),
            "applicationName": os.getenv("applicationName", " "),
            "order_ids": all_order_ids_for_logging,
            "step_function": "orders_data_aggregation",
            "component": "glue",
            "job_name": get_job_name(),
            "job_run_id": get_run_id(),
            "compressed_file": " ",
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
            print(f"[ERROR] glue_copy_files : log_event : Failed to write log to Splunk: {str(e)}")

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

def main():
    try:
        set_job_params_as_env_vars()
        log_event("STARTED", f"glue_copy_files job started processing transactionId : {os.getenv('transactionId')}")
        maxRetries, maxFailuresPerBatch, sleepTimeout = int(os.getenv('maxRetries', '5')), int(os.getenv('maxFailuresPerBatch', '10')), int(os.getenv('sleepTimeout', '30'))
        db = DataPostgres(os.getenv('region'), os.getenv('ssmdbKey'), os.getenv('dbName'), os.getenv('schema'), get_run_id())

        documents_extracted = db.getDocuments(os.getenv('transactionId'))

        groupedDocuments = {}

        for order in documents_extracted :
            if order.orderid in groupedDocuments :
                groupedDocuments[order.orderid].append(order)
            else :
                document_data = []
                document_data.append(order)
                groupedDocuments[order.orderid] = document_data
                          
        #groupedDocuments = {key: list(group) for key, group in itertools.groupby(db.getDocuments(os.getenv('transactionId')), lambda doc: doc.orderid)}
        retryCount = 0
        orderIdsToProcess = list(groupedDocuments.keys())

        all_order_ids = list(groupedDocuments.keys())

        # Metrics
        total_order_ids = len(groupedDocuments)
        total_documents = sum(len(docs) for docs in groupedDocuments.values())
        total_extracted = len(documents_extracted or [])

        log_event("INFO", f"glue_copy_files for transactionId : {os.getenv('transactionId')}, total_order_ids={total_order_ids}, total_documents={total_documents}, total_extracted={total_extracted}", all_order_ids)
        
        while retryCount < maxRetries and orderIdsToProcess:
            orderIdsToProcess = processOrders(orderIdsToProcess, groupedDocuments, maxFailuresPerBatch)
            if len(orderIdsToProcess) <= 0:
                break
            retryCount += 1
            log_event("WARN", f"Retry #{retryCount} for failed orderIds {orderIdsToProcess}")
            time.sleep(sleepTimeout)  # Wait before retrying
            
        if orderIdsToProcess:
            db.removeOrder(orderIdsToProcess)
    
        log_event("ENDED", f"glue_copy_files job successfully processed transactionId : {os.getenv('transactionId')}", all_order_ids)
    except Exception as e:
        log_event("FAILED", f"glue_copy_files : Error: {traceback.format_exc()} : Exception : {str(e)}")
        raise e


def processOrders(orderIds, groupedDocuments, maxFailuresPerBatch):
    orderIdsToProcess = []
    max_threads = 25
    failedDocs = 0

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = {}

        for orderid in orderIds:
            documents = groupedDocuments[orderid]
            for document in documents:
                dest = f"{os.getenv('outputBucketName')}/{os.getenv('outputDir').rstrip('/')}/{document.orderid}_{document.documentid}.pdf"
                futures[executor.submit(customCopyLogic, document.s3location, dest)] = (orderid, document)

        for future in as_completed(futures):
            orderid, document = futures[future]
            try:
                future.result()
            except Exception as e:
                log_event("WARN",
                          f"Document copy failed: order {orderid}, src={document.s3location}: {e}")
                if orderid not in orderIdsToProcess:
                    orderIdsToProcess.append(orderid)
                    failedDocs += 1
                if failedDocs > maxFailuresPerBatch:
                    raise ValueError("Exceeded maxFailuresPerBatch")

    return orderIdsToProcess


idx = lambda filePath: filePath.index('/')


def customCopyLogic(path, copiedpath):
    bucket, key = processS3Uri(path)
    if not key.endswith('.pdf'):
        try:
            s3.head_object(Bucket=bucket, Key=f"{key}.pdf")
            key = f"{key}.pdf"
            path = f"{bucket}/{key}"
        except ClientError as e:
            if e.response["Error"]["Code"] not in ("404", "NoSuchKey"):
                raise
            log_event('INFO', f'{path}.pdf file not found, using {path}')
    copyToDestination(path, copiedpath)
    

def copyToDestination(srcUri, destUri):
    
    try:    
        srcBucket, srcKey = processS3Uri(srcUri)
        destBucket, destKey = processS3Uri(destUri)
        s3.copy_object(Bucket=destBucket, CopySource={'Bucket': srcBucket, 'Key': srcKey}, Key=destKey)
    except Exception:
        raise

def processS3Uri(uri):
    pvtIndex = uri.index('/')
    srcBucket, srcKey = uri[:pvtIndex], uri[pvtIndex + 1:]

    return srcBucket, srcKey

if __name__ == "__main__":
    main()