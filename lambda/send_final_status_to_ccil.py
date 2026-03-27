import json
import psycopg2
import requests
from datetime import datetime, timezone
from psycopg2 import pool
import boto3
import os
import re
import traceback
from dpm_splunk_logger_py import splunk
from datetime import datetime, timezone
from botocore.exceptions import ClientError

# boto3 cient
client = boto3.client("secretsmanager")
ssm = boto3.client('ssm', region_name='us-east-1')

# Database Configuration

# Global Variables
db_pool = None
destination = None
package = None

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
destination = None
job_id = ""
processed_date = ""

def get_run_id():
    try:
        account_id = boto3.client("sts").get_caller_identity()["Account"]
        return f"arn:dpm:lambda:send_final_status_to_ccil:{account_id}"
    except Exception as e:
        print(f"[ERROR] Failed to get run id/account: {e}")
        raise e

def is_valid_email(email: str) -> bool:
    """Return True if email looks like a valid address."""
    if not email:
        return False
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    return re.match(email_regex, email) is not None

def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def log_event(status, message, context, all_order_ids_for_logging=None):

    if all_order_ids_for_logging is None:
        all_order_ids_for_logging = []

    job_name = "lmbd_send_final_status_to_ccil"
    if statemachine_name:
        parts = statemachine_name.split("-")
        if len(parts) >= 3:
            prefix = "_".join(parts[0:3])
            job_name = f"{prefix}_{job_name}"

    chunk_size = 1000
    total_orders = len(all_order_ids_for_logging)

    if total_orders > chunk_size:
        num_batches = (total_orders // chunk_size) + (1 if total_orders % chunk_size else 0)
        for idx, order_chunk in enumerate(chunk_list(all_order_ids_for_logging, chunk_size), start=1):
            batched_message = f"{message} (Batch {idx} of {num_batches})"

            event_data = {
                "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "transactionId": transactionId,
                "clientName": client_name,
                "clientID": clientID,
                "applicationName": application_name,
                "processed_date" : processed_date,
                "total_orders": len(all_order_ids),
                "job_id": job_id,
                "order_ids": json.dumps(order_chunk),
                "step_function": "send_order_status_to_ccil",
                "component": "lambda",
                "job_name": job_name,
                "job_run_id": get_run_id(),
                "compressed_file": package,
                "destinationType": destination,
                "execution_name": execution_name,
                "execution_id": execution_id,
                "execution_starttime": execution_starttime,
                "statemachine_name": statemachine_name,
                "statemachine_id": statemachine_id,
                "state_name": state_name,
                "state_enteredtime": state_enteredtime,
                "status": status,
                "message": batched_message,
                "aws_account_id": boto3.client("sts").get_caller_identity()["Account"],
            }

            print(json.dumps(event_data))
            try:
                splunk.log_message(event_data, context)
            except Exception as e:
                print(f"[ERROR] lmbd_send_final_status_to_ccil : log_event : Failed to write log batch {idx} to Splunk: {str(e)}")

    else:
        event_data = {
            "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "transactionId": transactionId,
            "clientName": client_name,
            "clientID": clientID,
            "applicationName": application_name,
            "processed_date" : processed_date,
            "total_orders": len(all_order_ids),
            "job_id": job_id,
            "order_ids": json.dumps(all_order_ids_for_logging),
            "step_function": "send_order_status_to_ccil",
            "component": "lambda",
            "job_name": job_name,
            "job_run_id": get_run_id(),
            "compressed_file": package,
            "destinationType": destination,
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
            print(f"[ERROR] lmbd_send_final_status_to_ccil : log_event : Failed to write log to Splunk: {str(e)}")

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
            print(f"Email sent! sender: {sender}")
            print("SES send_email full response:\n", json.dumps(response, indent=2))
        except ClientError as e:
            print(f"[ERROR] Failed to send email: {str(e)}")

def get_db_credentials(ssmdb_key, context):
    """
    Fetch PostgreSQL credentials from AWS Secrets Manager.
    """
    try:
        ssmdbkey = ssmdb_key
        param_name = ssm.get_parameter(Name=ssmdbkey, WithDecryption=True)
        param_value = param_name['Parameter']['Value']
        response = client.get_secret_value(SecretId=param_value)
        os.environ['json_db_values'] = response['SecretString']
    except Exception as e:
        raise Exception(f"Error getting db credentials: {str(e)}") from e

def initialize_db_pool(db_credentials):
    """
    Initialize the global database connection pool.
    """
    global db_pool
    if not db_credentials:
        raise ValueError("Database credentials could not be loaded.")

    db_pool = psycopg2.pool.SimpleConnectionPool(
        1, 10,  # Min & max connections
        host=db_credentials["host"],
        port=db_credentials["port"],
        dbname=os.environ["DB_NAME"],
        user=db_credentials["username"],
        password=db_credentials["password"]
    )

KAFKA_API_URL = os.environ["KAFKA_API_URL"]

def get_db_connection():
    return db_pool.getconn()

def release_db_connection(conn):
    if conn is not None:
        db_pool.putconn(conn)

def get_client_orderid_from_transaction(context, client_id, transaction_id):
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        query = """
            SELECT orderid, client 
            FROM order_transactions 
            WHERE client = %s AND transactionid = %s
        """
        cursor.execute(query, (client_id, transaction_id))
        result = cursor.fetchall()
        return result
    except psycopg2.Error as e:
        error_message = f"Error fetching data from order_transactions table: {str(e)}"
        if connection:
            connection.rollback()
        raise Exception(error_message) from e
    finally:
        if cursor:
            cursor.close()
        if connection:
            release_db_connection(connection)

def get_processing_orders_bulk(context, order_ids, clients):
    connection = None
    cursor = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        order_ids_list = list(order_ids)
        clients_list = list(clients)

        global all_order_ids
        all_order_ids = order_ids_list

        query = """
            SELECT orderid, client,orderdata,correlation_id,traceid,parent_order
            FROM order_table
            WHERE orderid = ANY(%s) AND client = ANY(%s) AND status='PROCESSING'
        """
        cursor.execute(query, (order_ids_list, clients_list))
        orders = cursor.fetchall()
        return orders
    except psycopg2.Error as e:
        error_message = f"Error fetching data from orders table: {str(e)}"
        if connection:
            connection.rollback()
        raise Exception(error_message) from e
    finally:
        if cursor:
            cursor.close()
        if connection:
            release_db_connection(connection)

def send_to_kafka_api(context, message, transaction_id):
    try:
        correlation_id = ""
        trace_id = ""
        if "detail" in message:
            if "correlationId" in message["detail"]:
                correlation_id = message["detail"]["correlationId"]
            if "traceId" in message["detail"]:
                trace_id = message["detail"]["traceId"]

        headers = {"Content-Type": "application/json", 'message-source': 'DPM-AGGREGATOR', 'Accept': 'application/json','transaction-id':correlation_id,'client-trace-id':trace_id,'wip-id':transaction_id}
        response = requests.post(KAFKA_API_URL, json=message, headers=headers, timeout=5)
        return response.status_code == 200
    except requests.RequestException as e:
        return False
    
def get_kafka_message(context, record_json,correlation_id,traceid,client_id,parent_order):
    try:
        order_id = record_json["communicationCommon"]["orderId"]
        communication_narrative_type = None
        communication_narrative_value = None
        if "communicationNarrativeType" in record_json["communicationCommon"]:
            communication_narrative_type = record_json["communicationCommon"]["communicationNarrativeType"]
        if "communicationNarrativeValue" in record_json["communicationCommon"]:
            communication_narrative_value = record_json["communicationCommon"]["communicationNarrativeValue"]
        
        nonStandardItems = None
        businessUnitType = None
        departmentId = None
        requestid = None
        notificationProperties = None
        is_bulk_flag = None
        is_foreign_address = None
        submit_to_print_flag = None
        infact_account_number = parent_order

        if "communicationFlags" in record_json["communicationCommon"]:
            for flag in record_json["communicationCommon"]["communicationFlags"]:
                if "communicationFlagType" in flag:
                    flag_name = flag["communicationFlagType"]
                    if (flag_name == "isBulkShipping") :
                        is_bulk_flag = flag["communicationFlagValue"]                            
                    elif (flag_name == "isForeignAddress") :
                        is_foreign_address = flag["communicationFlagValue"]
                    elif (flag_name == "submitForPrint") :
                        submit_to_print_flag = flag["communicationFlagValue"]

        if "nonStandardItems" in record_json["communicationCommon"]:
            nonStandardItems = record_json["communicationCommon"]["nonStandardItems"]
        
        if None != nonStandardItems:
            if "businessUnitType" in nonStandardItems:
                businessUnitType = nonStandardItems["businessUnitType"]
            if "departmentId" in nonStandardItems:
                departmentId = nonStandardItems["departmentId"]
            if "notificationProperties" in nonStandardItems:
                notificationProperties = nonStandardItems["notificationProperties"]
        
        if "communicationIdentifierType" in record_json["communicationCommon"]:
            if str(record_json["communicationCommon"]["communicationIdentifierType"]) == "requestId":
                if "communicationIdentifierValue" in record_json["communicationCommon"]:
                    requestid = str(record_json["communicationCommon"]["communicationIdentifierValue"])

        unique_documents = {}
        
        for doc in record_json["communicationDocuments"]:
            if "documentId" in doc:
                doc_id = doc["documentId"]
            else:
                doc_id = doc["communicationDocumentSequenceNumber"]
            seq_number = doc["communicationDocumentSequenceNumber"]
            if "communicationActionRegistry" in doc:
                communicationActionRegistry = doc["communicationActionRegistry"]
            else:
                communicationActionRegistry = None
            if "communicationIndices" in doc:
                communicationIndices = doc["communicationIndices"]
            else:
                communicationIndices = None
            if "communicationDetail" in doc:
                communicationDetail = doc["communicationDetail"]
            else:
                communicationDetail = None
            if doc_id not in unique_documents:
                unique_documents[doc_id] = {
                    "documentId": doc_id,
                    "communicationDocumentSequenceNumber":str(seq_number),
                    "communicationStatus": "AGGREGATION COMPLETE",
                    "communicationActionRegistry" : communicationActionRegistry,
                    "communicationIndices" : communicationIndices,
                    "communicationDetail" : communicationDetail
                }

        kafka_message = [
            {
                "detail": {
                    "communicationOrigin": "AGGREGATION",
                    "communicationRequestSource" : "DPM-AGGREGATOR",
                    "facility" : destination,
                    "package" : package,
                    "departmentId" : departmentId,
                    "correlationId": correlation_id,
                    "clientId" : str(client_id),
                    "traceId": traceid,
                    "orderId": order_id,
                    "is_bulk": is_bulk_flag,
                    "infact_account_number" : infact_account_number,
                    "is_foreign_address" : is_foreign_address,
                    "submit_to_print" : submit_to_print_flag,
                    "communicationOriginDateTime": datetime.now(timezone.utc).isoformat(),
                    "communicationEventType": "ORDER",
                    "communicationStatus": "AGGREGATION COMPLETE",
                },
                "payload": {
                    "orderId": order_id,
                    "communicationNarrativeType": communication_narrative_type,
                    "communicationNarrativeValue": communication_narrative_value,
                    "communicationStatus": "AGGREGATION COMPLETE",
                    "echoBack": None,
                    "orderEchoback" : {
                        "businessunit" : businessUnitType,
                        "requestid": requestid,
                        "notificationProperties" : notificationProperties
                    }
                }
            }
        ]
        return kafka_message
    except Exception as e:
        error_message = f"Error in get_kafka_message: {str(e)}"
        raise Exception(error_message) from e

def get_job_id(file_name):
    try:
        if not file_name or not isinstance(file_name, str):
            return ""

        # Take part before the first dot
        first_part = file_name.split(".", 1)[0]

        # Extract after the last underscore
        parts = first_part.split("_")
        if parts and len(parts) > 0:
            job_id = parts[-1]
            return job_id.strip()
        return ""
    except Exception:
        return ""

def lambda_handler(event, context):
    try:
        client_id = event.get("client")
        transaction_id = event.get("transaction_id")
        ssmdb_key = event.get("ssmdbkey")
        
        global destination, package, job_id, processed_date
        destination = event.get("destinationType",None)
        package = event.get("compressFileName",None)
        job_id = get_job_id(package)
        processed_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        global client_name, clientID, application_name, execution_name, execution_id, execution_starttime, statemachine_name
        global statemachine_id, state_name, state_enteredtime, transactionId, teamsId
        client_name = event.get("clientName", "")
        clientID = event.get("clientID", "")
        application_name = event.get("applicationName", "")
        execution_name = event.get("execution_name", "")
        execution_id = event.get("execution_id", "")
        execution_starttime = event.get("execution_starttime", "")
        statemachine_name = event.get("statemachine_name", "")
        statemachine_id = event.get("statemachine_id", "")
        state_name = event.get("state_name", "")
        state_enteredtime = event.get("state_enteredtime", "")
        transactionId = event.get("transactionId", "")
        teamsId = event.get("teamsId", "")

        log_event("STARTED", f"lmbd_send_final_status_to_ccil job started with event: {json.dumps(event)}", context)    

        if destination and destination.lower() == "onprem":
            destination = "BRCC"
        elif destination and destination.lower() == "marcomm":
            destination = "MARCOM"
        
        log_event("INFO", f"Facility: {str(destination)}, Client: {str(client_id)}, Transaction: {str(transaction_id)}", context)

        if not client_id or not transaction_id or not ssmdb_key:
            error_message = "lmbd_send_final_status_to_ccil job failed: Missing required parameter"
            raise Exception(error_message)

        # Fetch DB credentials only once per Lambda execution
        global db_pool
        if db_pool is None:
            get_db_credentials(ssmdb_key, context)
            db_credentials = json.loads(os.getenv('json_db_values'))
            if not db_credentials:
                error_message = "Failed to retrieve DB credentials"
                raise Exception(error_message)
            initialize_db_pool(db_credentials)

        try:
            orders = get_client_orderid_from_transaction(context, client_id, transaction_id)
        except Exception as e:
            raise e
        
        if not orders:
            error_message = f"No orders found for client_id: {client_id}, transaction_id: {transaction_id}"
            log_event("WARN", f"lmbd_send_final_status_to_ccil : Orders not found for client_id: {client_id}, transaction_id: {transaction_id}", context)

            
        order_ids, clients = zip(*orders)
        try:
            processing_orders = get_processing_orders_bulk(context, order_ids, clients)
        except Exception as e:
            raise e
        
        if processing_orders:
            kafka_messages = [get_kafka_message(context,json.loads(order[2]),order[3],order[4],client_id,order[5]) for order in processing_orders]
            kafka_messages = [msg for sublist in kafka_messages for msg in sublist]
            for message in kafka_messages:
                if send_to_kafka_api(context, message,transaction_id):
                    pass
                else:
                    splunk.log_message({"Status": "Warning", "Message": "Kafka API call failed: Unable to deliver message"}, context)
                    counter = 1
                    while counter < 4 :
                        splunk.log_message({"Status": "Retrying", "Message": "Retrying to send message to KAFKA , counter : "+str(counter)}, context)
                        if send_to_kafka_api(context, message,transaction_id):
                            counter = 5
                        else:
                            counter = counter +1

                    if counter == 4:
                        raise Exception("Kafka API call failed: Unable to deliver message")
        else:
            error_message = f"No processing orders found for clients: {clients}, order_ids: {order_ids}"
            raise Exception(error_message)

        log_event("ENDED", f"lmbd_send_final_status_to_ccil job successfully completed for {len(processing_orders)} orders", context, all_order_ids)
        log_event("SUMMARY", f" Total {len(processing_orders)} orders processed for job_id={job_id} on {processed_date}", context)

        return {"statusCode": 200, "body": f"Processed {len(processing_orders)} orders"}

    except Exception as e:
        log_event("FAILED", f"Error: {traceback.format_exc()}, Exception : {str(e)}, Context: {context}", context)
        raise e
