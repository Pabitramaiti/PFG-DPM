import json
import boto3
import csv
import io
from dpm_splunk_logger_py import splunk
import re
import os
import traceback
from datetime import datetime, timezone
from botocore.exceptions import ClientError

# Initialize S3 client
s3 = boto3.client('s3')

all_order_ids = []
client_name = None
clientID = None
application_name = None
execution_name = None
execution_id = None
execution_starttime = None
statemachine_name = None
statemachine_id = None
state_name = None
state_enteredtime = None
transactionId = None
teamsId = ""


def get_run_id():
    try:
        account_id = boto3.client("sts").get_caller_identity()["Account"]
        return f"arn:dpm:lambda:brx_invocation:{account_id}"
    except Exception as e:
        print(f"[ERROR] Failed to get run id/account: {e}")
        raise e

def is_valid_email(email: str) -> bool:
    """Return True if email looks like a valid address."""
    if not email:
        return False
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(email_regex, email) is not None

def log_event(status, message, context):

    job_name = "lmbd_brx_invocation"
    if statemachine_name:
        parts = statemachine_name.split("-")
        if len(parts) >= 3:
            prefix = "_".join(parts[0:3])
            job_name = f"{prefix}_{job_name}"

    event_data = {
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "transactionId": transactionId,
        "clientName": client_name,
        "clientID": clientID,
        "applicationName": application_name,
        "order_ids": all_order_ids if "all_order_ids" in globals() else [],
        "step_function": "orders_data_aggregation",
        "component": "lambda",
        "job_name": job_name,
        "job_run_id": get_run_id(),
        "compressed_file": "",
        "execution_name": execution_name,
        "execution_id": execution_id,
        "execution_starttime": execution_starttime,
        "statemachine_name": statemachine_name,
        "statemachine_id": statemachine_id,
        "state_name": state_name,
        "state_enteredtime": state_enteredtime,
        "status": status,
        "message": message,
        "aws_account_id": boto3.client("sts").get_caller_identity()["Account"],
    }

    print(json.dumps(event_data))

    try:
        splunk.log_message(event_data, context)
    except Exception as e:
        print(f"[ERROR] lmbd_brx_invocation : log_event : Failed to write log to Splunk: {str(e)}")

    if status == "FAILED" and is_valid_email(teamsId):

        lambda_link = (
            f"https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1"
            f"#/functions/{event_data['job_name']}"
        )

        stepfn_link = (
            f"https://us-east-1.console.aws.amazon.com/states/home?region=us-east-1"
            f"#/v2/executions/details/{event_data['execution_id']}"
        )

        subject = f"[ALERT] Lambda Function Failure - {event_data['job_name']} ({status})"

        body_text = (
            f"Lambda job failed.\n\n"
            f"Event Time: {event_data['event_time']}\n"
            f"Client: {event_data['clientName']} ({event_data['clientID']})\n"
            f"Job: {event_data['job_name']}\n"
            f"Order Ids: {event_data['order_ids']}\n"
            f"TransactionId: {event_data['transactionId']}\n"
            f"Status: {event_data['status']}\n"
            f"Message: {event_data['message']}\n"
            f"Lambda Function: {lambda_link}\n"
            f"Step Function Execution: {stepfn_link}\n"
        )

        body_html = f"""<html>
        <body>
          <h3>Lambda Job Failure Notification</h3>
          <ul>
            <li><b>Event Time:</b> {event_data['event_time']}</li>
            <li><b>Client:</b> {event_data['clientName']} ({event_data['clientID']})</li>
            <li><b>Job Name:</b> {event_data['job_name']}</li>
            <li><b>Order Ids:</b> {event_data['order_ids']}</li>
            <li><b>TransactionId:</b> {event_data['transactionId']}</li>
            <li><b>Status:</b> {event_data['status']}</li>
            <li><b>Message:</b> {event_data['message']}</li>
          </ul>
          <p><a href="{lambda_link}">View Lambda Function</a></p>
          <p><a href="{stepfn_link}">View Step Function Execution</a></p>
        </body>
        </html>"""

        sender = os.getenv("ALERT_SENDER_EMAIL", "noreply@broadridge.com")
        ses_client = boto3.client("ses", region_name="us-east-1")

        try:
            response = ses_client.send_email(
                Destination={"ToAddresses": [teamsId]},
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
            print(f"[ERROR] Failed to send email: {str(e)}")

def lambda_handler(event, context):
    try:
        # Extract the input params from the event
        destination_bucket = event["bucket"]
        destination_file_name = event['destinationFileName']
        destination_key = event['destinationKey']
        transaction_id = event['transactionId']
        onprem_output_file_name = event['onpremOutputFileName']
        onprem_output_path = event['onpremOutputPath']
        marcomm_output_file_name = event['marcommOutputFileName']
        marcomm_output_path = event['marcommOutputPath']
        destination_type = event['destinationType']
        job_name = event['jobName']
        brx_output_trigger_file = event['brxOutputTriggerFile']
        marcomm_sla = event['marcommSLA']

        global client_name, clientID, application_name, execution_name, execution_id, execution_starttime
        global statemachine_name, statemachine_id, state_name, state_enteredtime, transactionId, teamsId
        client_name = event.get("clientName", "")
        clientID = event.get("clientID", "")
        application_name = event.get("applicationName", "")
        execution_name = event.get("execution_name", "")
        execution_id = event.get("execution_id", "")
        execution_starttime = event.get("execution_starttime", "")
        statemachine_name = event.get("statemachine_name", "")
        statemachine_id = event.get("statemachine_id", "")
        state_name = event.get("state_name")
        state_enteredtime = event.get("state_enteredtime", "")
        transactionId = event.get("transactionId", "")
        teamsId = event.get("teamsId", "")

        log_event("STARTED", f"lmbd_brx_invocation job started with with event: {json.dumps(event)}", context)

        # Check if destination file ends with required formats
        if destination_file_name.endswith('.json'):
            response = json_trigger_file_creation(destination_bucket, destination_key, 
                    destination_file_name, transaction_id, onprem_output_file_name, onprem_output_path, marcomm_output_file_name, marcomm_output_path, destination_type, job_name, brx_output_trigger_file, marcomm_sla)
        elif destination_file_name.endswith('.csv'):
            response = csv_trigger_file_creation(destination_bucket, destination_key, 
                    destination_file_name, transaction_id, onprem_output_file_name, onprem_output_path, marcomm_output_file_name, marcomm_output_path, destination_type, job_name, brx_output_trigger_file, marcomm_sla)
        
        log_event("ENDED", f"lmbd_brx_invocation job completed successfully with response: {json.dumps(response)}", context)
        return response

    except Exception as e:
        log_event("FAILED", f"Error: {traceback.format_exc()}, Exception : {str(e)}, Context: {context}", context)
        raise e


def csv_trigger_file_creation(destination_bucket, destination_key, 
                destination_file_name, transaction_id, onprem_output_file_name, onprem_output_path, marcomm_output_file_name, marcomm_output_path, destination_type, job_name, brx_output_trigger_file, marcomm_sla):
    '''
    Function to create csv trigger file
    '''

    try:
        # Create in-memory CSV file
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)

        # Write CSV data
        csv_writer.writerow(["transaction_id", "onprem_output_file_name", "onprem_output_path", "marcomm_output_file_name", "marcomm_output_path", "destination_type", "job_name", "brx_output_trigger_file", "marcomm_sla"])
        csv_writer.writerow([transaction_id, onprem_output_file_name, onprem_output_path, marcomm_output_file_name, marcomm_output_path, destination_type, job_name, brx_output_trigger_file, marcomm_sla])

        # Move buffer position to start before upload
        csv_buffer.seek(0)

        # Upload the zip file back to S3
        s3.put_object(Bucket=destination_bucket, Key=f"{destination_key}/{destination_file_name}", Body=csv_buffer.getvalue())

        response = {
            'statusCode': 200,
            'body': json.dumps(f'{destination_key}/{destination_file_name} created successfully')
        }

        return response
    
    except Exception as e:
        raise e


def json_trigger_file_creation(destination_bucket, destination_key, 
                destination_file_name, transaction_id, onprem_output_file_name, onprem_output_path, marcomm_output_file_name, marcomm_output_path, destination_type, job_name, brx_output_trigger_file, marcomm_sla):
    '''
    Function to create json trigger file
    '''
    try:
        # output json creation
        output_json = {
            "transaction_id": transaction_id,
            "onprem_output_file_name": onprem_output_file_name,
            "onprem_output_path": onprem_output_path,
            "marcomm_output_file_name": marcomm_output_file_name,
            "marcomm_output_path": marcomm_output_path,
            "destination_type": destination_type,
            "job_name": job_name,
            "brx_output_trigger_file": brx_output_trigger_file,
            "marcomm_sla": marcomm_sla
        }

        # Convert data to a JSON string
        json_data = json.dumps(output_json, indent=4)
        
        # Upload the JSON file to S3
        s3.upload_fileobj(
            io.BytesIO(json_data.encode('utf-8')),
            destination_bucket,
            f'{destination_key}/{destination_file_name}' 
        )

        response = {
            'statusCode': 200,
            'body': json.dumps(f'{destination_key}/{destination_file_name} created successfully')
        }

        return response

    except Exception as e:
        raise e

