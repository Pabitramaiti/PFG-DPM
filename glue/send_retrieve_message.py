import json
import psycopg2
import requests
from datetime import datetime, timezone
from botocore.exceptions import ClientError
import os
from psycopg2 import pool
import splunk
import boto3
import traceback
import uuid
import time
import csv
import sys
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.pool import ThreadedConnectionPool

SPLUNK_EXECUTOR = ThreadPoolExecutor(max_workers=20)
S3_CHECK_POOL = ThreadPoolExecutor(max_workers=20)

# Initialize boto3 client for s3
s3 = boto3.client('s3')
ssm = boto3.client('ssm', region_name='us-east-1')
client_db = boto3.client("secretsmanager")
AWS_ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]


def get_job_name():
    job_name = "glue_send_retrieve_message"
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

def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def send_splunk_event_async(event_data):
    """Submit a Splunk event asynchronously."""
    def _send():
        try:
            splunk.log_message(event_data, get_run_id())
        except Exception as e:
            # Keep errors visible; don’t fail main thread
            print(f"[ERROR] Splunk async log failed for {event_data}:: Exception : {e}")

    # fire and forget
    SPLUNK_EXECUTOR.submit(_send)

# --- Splunk logger ---
def log_event(status: str, message: str, all_order_ids_for_logging=None):

    if all_order_ids_for_logging is None:
        all_order_ids_for_logging = []

    chunk_size = 1000
    total_orders = len(all_order_ids_for_logging)

    if total_orders > chunk_size:
        num_batches = (total_orders // chunk_size) + (1 if total_orders % chunk_size else 0)
        for idx, order_chunk in enumerate(chunk_list(all_order_ids_for_logging, chunk_size), start=1):
            batched_message = f"{message} (Batch {idx} of {num_batches})"

            event_data = {
                "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "transactionId": os.getenv("transactionId", " "),
                "clientName": os.getenv("clientName", " "),
                "clientID": os.getenv("clientID", " "),
                "applicationName": os.getenv("applicationName", " "),
                "order_ids": json.dumps(order_chunk),
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
                "message": batched_message,
                "aws_account_id": AWS_ACCOUNT_ID,
            }

            print(json.dumps(event_data))
            try:
                send_splunk_event_async(event_data)
            except Exception as e:
                print(f"[ERROR] glue_send_retrieve_message : log_event : Failed to write log batch {idx} to Splunk: {str(e)}")

    else:
        event_data = {
            "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "transactionId": os.getenv("transactionId", " "),
            "clientName": os.getenv("clientName", " "),
            "clientID": os.getenv("clientID", " "),
            "applicationName": os.getenv("applicationName", " "),
            "order_ids": json.dumps(all_order_ids_for_logging),
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
            "state_name": os.getenv("state_name", ""),
            "state_enteredtime": os.getenv("state_enteredtime", " "),
            "status": status,
            "message": message,
            "aws_account_id": AWS_ACCOUNT_ID,
        }

        print(json.dumps(event_data))
        try:
            send_splunk_event_async(event_data)
        except Exception as e:
            print(f"[ERROR] glue_send_retrieve_message : log_event : Failed to write log to Splunk: {str(e)}")

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
            f"Event Time: {event_data.get('event_time')}\n"
            f"Client: {event_data.get('clientName')} ({event_data.get('clientID')})\n"
            f"Job: {event_data.get('job_name')}\n"
            f"Order Ids: {event_data.get('order_ids')}\n"
            f"TransactionId: {event_data.get('transactionId')}\n"
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
            <li><b>Order Ids:</b> {event_data.get('order_ids')}</li>
            <li><b>TransactionId:</b> {event_data.get('transactionId')}</li>
            <li><b>Status:</b> {event_data.get('status')}</li>
            <li><b>Message:</b> {event_data.get('message')}</li>
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

def set_job_params_as_env_vars():
    messages = []
    messages.append(f"Set environment variable:")
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i+1]
            os.environ[key] = value
            messages.append(f"{key}={value}")

    log_event("INFO", f"glue_send_retrieve_message::Environment variables set from job parameters: {', '.join(messages)}")

# PostgreSQL connection details (set via AWS Lambda environment variables)
def get_db_credentials(ssmDBKey):
    """
    Fetch PostgreSQL credentials from AWS Secrets Manager.
    """
    try:
        ssmdbkey = ssmDBKey
        param_name = ssm.get_parameter(Name=ssmdbkey, WithDecryption=True)
        param_value = param_name['Parameter']['Value']
        response = client_db.get_secret_value(SecretId=param_value)
        json_secret_value = json.loads(response['SecretString'])
        return json_secret_value
    except Exception as e:
        raise Exception(f"Error getting db credentials: {str(e)}") from e


def initialize_db_pool(db_credentials):
    """
    Initialize the global database connection pool.
    """
    if not db_credentials:
        raise ValueError("Database credentials could not be loaded.")

    db_pool = ThreadedConnectionPool(
        5, 20,
        host=db_credentials["host"],
        port=db_credentials["port"],
        dbname=os.environ["dbName"],
        user=db_credentials["username"],
        password=db_credentials["password"]
    )
    return db_pool


def get_db_connection(db_pool):
    return db_pool.getconn()


def release_db_connection(connection, db_pool):
    if connection is not None:
        db_pool.putconn(connection)

def process_orders(db_pool):
    try:
        transaction_id = os.getenv('transactionId')
        clientName = os.getenv('clientName')
        bucket_name = os.getenv("bucket")
        objectKey = os.getenv('objectKey')
        response = s3.get_object(Bucket=bucket_name, Key=objectKey)
        content = response['Body'].read().decode('utf-8').strip()
        order_ids = []

        lines = content.splitlines()
        buffer = []
        orders = 0
        valid_orders = []
        bad_orders = []
        transactions = []
        job_name = ""

        for line in lines:

            kafka_send_status = True
            is_file_present = True
            s3_locations = []
            order_status_value = 'PROCESSING'
            order_status_reason = 'Valid data'
            
            buffer.append(line)
            joined = "\n".join(buffer)

            # Match end of record like }|CGS12345|PFG|COMPLETED
            match = re.search(r'}\|([^\|]+)\|([^\|]+)\|([^\|]+)$', line.strip())
            if match:
                job_name = match.group(1)
                client = match.group(2)
                status = match.group(3)

                # Extract the first 3 fields (before JSON)
                first_line = buffer[0]
                first_parts = first_line.split('|', 3)
                if len(first_parts) < 4:
                    buffer = []
                    continue

                order_id = first_parts[0].strip()
                correlation_id = first_parts[1].strip()
                order_ids.append(order_id)

                # Extract JSON block from the full buffer
                full_text = "\n".join(buffer)
                json_start_index = full_text.find('{')
                json_end_index = full_text.rfind('}') + 1
                if json_start_index == -1 or json_end_index == -1:
                    buffer = []
                    continue

                orderdata = full_text[json_start_index:json_end_index]

                order = {
                    "order_id": order_id,
                    "correlation_id": correlation_id,
                    "orderdata": orderdata,
                    "job_name": job_name,
                    "client": client,
                    "status": status
                }

                orders += 1

                buffer = []  # Reset buffer for next record
                s3_locations = get_s3locations(order_id, db_pool)

                
                if s3_locations:
                    # Run S3 file checks concurrently
                    futures = [S3_CHECK_POOL.submit(is_s3_location_present, s3_location[1])
                            for s3_location in s3_locations]

                    # Collect results as they complete
                    results = []
                    for f in as_completed(futures):
                        try:
                            results.append(f.result())
                        except Exception as e:
                            log_event("WARN", f"S3 existence check failed: {str(e)}")
                            results.append(False)

                    # Evaluate which S3 locations are missing
                    for idx, s3_location in enumerate(s3_locations):
                        is_file_present = results[idx]
                        s3_file_location = s3_location[1]
                        if not is_file_present:
                            splunk.log_message(
                                {"Status": "Warning", "Message": "S3 file not found, sending retrieve message for: " + str(s3_location)},
                                get_run_id()
                            )
                            try:
                                path = s3_file_location[5:]
                                bucket, key = path.split('/', 1)
                                kafka_message = get_kafka_message(order, s3_location[0], key)
                                if kafka_message is not None:
                                    kafka_send_status = send_to_kafka_api(kafka_message)
                                else:
                                    order_status_value = 'BAD DATA'
                                    kafka_send_status = True
                                    order_status_reason = 'Not able to create retrieve message; missing required fields'
                                    break
                            except Exception as e:
                                if kafka_send_status != True :
                                    counter = 1
                                    while counter < 4:
                                        time.sleep(10)
                                        kafka_send_status = send_to_kafka_api(kafka_message)
                                        if kafka_send_status:
                                            counter = 5
                                        else:
                                            counter=counter+1
                                if kafka_send_status != True :
                                    raise Exception(f"Failed to send Kafka message after retries for order_id={order['order_id']}") from e
                else:
                    order_status_value = 'BAD DATA'
                    kafka_send_status = True
                    order_status_reason = 'No S3 locations were found for this order in retrieve_table'

                if kafka_send_status:
                    try:
                        if 'BAD DATA' != order_status_value:
                            transactions.append((order['order_id'], transaction_id, clientName))
                            valid_orders.append(order['order_id'])
                        else:
                            bad_orders.append(order['order_id'])
                            log_event("WARN", f"Order was not considered for Aggregation, Reason: {order_status_reason}, order_id: {order['order_id']}")
                    except Exception as e:
                        error_message = f"Failed to insert kafka_send_status={kafka_send_status}, transaction_id={transaction_id}, clientName={clientName} into order_transactions table: Exception={e}"
                        raise Exception(error_message) from e
                else:
                    error_message = f"Failed to send Kafka message for kafka_send_status={kafka_send_status}, order_id={order['order_id']}, order_status_value={order_status_value}, order_status={order['status']}, job_name={order['job_name']}, client={order['client']}"
                    raise Exception(error_message) from e
                    
        log_event("INFO", f"Total orders processed: {orders}, Valid orders: {len(valid_orders)}, Bad orders: {len(bad_orders)}")
        if len(valid_orders) > 0 :
            insert_transaction(transactions, db_pool)
            update_order_status(clientName, job_name, 'PROCESSING', valid_orders, db_pool)
        if len(bad_orders) > 0 :
            update_order_status(clientName, job_name, 'BAD_DATA', bad_orders, db_pool)
            
        return orders, order_ids
        
    except Exception as e:
        error_message = f"Error reading .trig file: {e}"
        raise Exception(error_message) from e

def get_s3locations(order_id, db_pool):
    """
    Fetch s3 locations from the PostgreSQL retrieve_table based on order_id
    """
    connection = None
    cursor = None

    try:
        connection = get_db_connection(db_pool)
        cursor = connection.cursor()
       
        query = f"""
            SELECT DISTINCT ON (document_id) document_id,s3location
            FROM retrieve_table 
            WHERE orderid = %s
        """
        params = [order_id]
        cursor.execute(query, params)
        s3_locations = cursor.fetchall()
        return s3_locations
    
    except Exception as e:
        error_message = f"Error fetching data from retrieve table: {e}"
        if connection:
            connection.rollback()
        raise Exception(error_message) from e
    finally:
        # Close cursor and connection safely
        if cursor:
            cursor.close()
        if connection:
            release_db_connection(connection, db_pool)



def get_kafka_message(record, document_id, s3_location):
    try:
        kafka_message =[]
        data_dict={}
        corelation_id = ""
        clientName = os.getenv('clientName')
        myuuid = uuid.uuid4()

        corelation_id = record['correlation_id']
        data_dict["orderdata"] = json.loads(record['orderdata'])
        if corelation_id == "" :
            corelation_id = myuuid
        order_id = data_dict["orderdata"]["communicationCommon"]["orderId"]
        for data in data_dict["orderdata"]["communicationDocuments"]:

            doc_id = ""
            
            if "documentId" in data:
                if str(data["documentId"]) != str(document_id):
                    continue
                else:
                    doc_id = data["documentId"]
            else:
                doc_id = str(document_id)


            s3_location = s3_location.replace('composedFile.pdf/composedFile','composedFile.pdf')

            message_dict = {}
            message_dict["documentRegistryIdentifier"] = clientName
            message_dict["communicationOrigin"] = "DPM-AGGREGATOR"
            message_dict["communicationActionType"] = "retrieve"
            message_dict["communicationDocumentSequenceNumber"] = data["communicationDocumentSequenceNumber"]
            message_dict["orderId"] = order_id 
            message_dict["documentId"] = doc_id
            message_dict["s3locator"] = str(s3_location)
            message_dict["clientId"] = clientName
            message_dict["communicationDocumentRegistryIdentifierVersion"] = None
            message_dict["communicationIndices"] = None

            if "communicationActions" in data:
                for items in data["communicationActions"]:
                    for item in items["communicationActionRegistry"]:
                        if item.get("communicationCustomKey") == "RegistryItemDetailItemKey":          
                            message_dict["communicationDocumentRegistryIdentifier"] = item.get("communicationCustomValue")
                        if item.get("communicationCustomKey") == "RegistryItemDetailItemKeyVersion":
                            message_dict["communicationDocumentRegistryIdentifierVersion"] = item.get("communicationCustomValue")

            if "nonStandardItems" in data:
                if "documentDetails" in  data["nonStandardItems"]:
                    if "staticDocument" in data["nonStandardItems"]["documentDetails"]:
                        if "businessIdentifier" in data["nonStandardItems"]["documentDetails"]["staticDocument"]:
                            message_dict["communicationDocumentRegistryIdentifier"] = data["nonStandardItems"]["documentDetails"]["staticDocument"]["businessIdentifier"]

            if "nonStandardItems" in data:
                if "documentDetails" in data["nonStandardItems"]:
                    if "documentDeliveryDetail" in data["nonStandardItems"]["documentDetails"]:
                        if "documentIndexes" in data["nonStandardItems"]["documentDetails"]["documentDeliveryDetail"]:
                            if "documentIndex" in data["nonStandardItems"]["documentDetails"]["documentDeliveryDetail"]["documentIndexes"]:
                                message_dict["communicationIndices"] = []
                                counter = 0
                                for item in data["nonStandardItems"]["documentDetails"]["documentDeliveryDetail"]["documentIndexes"]["documentIndex"]:
                                    row={}
                                    message_dict["communicationIndices"].append(row)
                                    message_dict["communicationIndices"][counter]["communicationCustomKey"] = item.get("uniqueDocumentInstanceKeyName")
                                    message_dict["communicationIndices"][counter]["communicationCustomValue"] = item.get("uniqueDocumentInstanceKeyValue")
                                    counter = counter + 1
            
            
            kafka_message.append(message_dict)

        return kafka_message
    except Exception as e:
        error_message = f"Error in get_kafka_message: {traceback.format_exc()}"
        log_event("WARN", error_message)
        return None

def send_to_kafka_api(messages):
    """
    Publish message to Kafka via API call
    """
    success = True
    for message in messages:
        try:
            headers = {"Content-Type": "application/json", 'message-source': 'DPM-AGGREGATOR','Accept': 'application/json'}
            response = requests.post(os.environ["KAFKA_API_URL"], json=message, headers=headers)
            if response.status_code != 200:
                success = False
                log_event("WARN", f"Failed to send message to Kafka API. Status Code={response.status_code}, message={message}")
        except requests.RequestException as e:
            error_message = f"Error publishing message={message} to Kafka via API call: {str(e)}"
            raise Exception(error_message) from e

    return success


def update_order_status(clientName, job_name, new_status, order_ids, db_pool):
    """Update order_status (e.g., to 'PROCESSING') for a given clientName, job_name, and order IDs."""
    connection = None
    cursor = None
    update_query = None
    try:
        # Establish connection
        connection = get_db_connection(db_pool)
        cursor = connection.cursor()
        update_query = """
        UPDATE order_table 
        SET status = %s
        WHERE client = %s
            AND job_name = %s
            AND status IN ('COMPLETED', 'ACCEPTED')
            AND orderid IN %s;
        """
        cursor.execute(update_query, (new_status, clientName, job_name, tuple(order_ids)))
        connection.commit()

        """
        for order_id in order_ids:
            event_data = {
                "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "event_type": "ORDER_STATUS_CHANGE",
                "order_id": order_id,
                "previous_status": "ACCEPTED",
                "new_status": new_status,
                "client": clientName,
                "correlation_id": " ",
                "trace_id": " ",
                "job_id": job_name,
            }
            send_splunk_event_async(event_data)
        """


        log_event("INFO", f"Updated order status to {new_status} for {len(order_ids)} orders, clientName={clientName}, job_name={job_name}")

    except psycopg2.Error as e:
        error_message = f"Error updating orders for order_ids={order_ids}, curent_status={new_status}, clientName={clientName}, job_name={job_name}, Exception={e}"
        if connection:
            connection.rollback()
        raise Exception(error_message) from e
    finally:
        # Close cursor and connection safely
        if cursor:
            cursor.close()
        if connection:
            release_db_connection(connection, db_pool)

def insert_transaction(transactions, db_pool):
    """
    Insert multiple transaction records into the PostgreSQL order_transactions table.
    """
    connection = None
    cursor = None

    if not transactions:
        # nothing to insert
        return

    try:
        connection = get_db_connection(db_pool)
        cursor = connection.cursor()

        query = """
            INSERT INTO order_transactions (orderid, transactionid, client)
            VALUES (%s, %s, %s)
        """
        cursor.executemany(query, transactions)
        connection.commit()

    except psycopg2.Error as e:
        error_message = f"Error inserting into order_transactions table: {e}"
        if connection:
            connection.rollback()
        raise Exception(error_message) from e

    finally:
        if cursor:
            cursor.close()
        if connection:
            release_db_connection(connection, db_pool)


def is_s3_location_present(s3_location: str) -> bool:
    """
    Check whether an S3 object exists for the given s3:// path.
    Tries a few common name variants (with and without '.pdf').
    Returns True if any variant exists, else False.
    """
    try:
        # Strip the "s3://" prefix and split into bucket/key
        path = s3_location[5:]
        bucket, key = path.split('/', 1)

        # Possible variants you used in your original logic
        variants = [key, key.replace('.pdf', ''), key + '.pdf']

        for variant in variants:
            try:
                s3.head_object(Bucket=bucket, Key=variant)
                # Found – return immediately
                return True
            except ClientError as e:
                # 404/NoSuchKey means "not found": try next variant
                if e.response["Error"]["Code"] not in ("404", "NoSuchKey"):
                    # Something else went wrong (e.g., permission)
                    raise
            except Exception as e:
                # Catch any other issue – log and continue
                log_event("WARN", f"{s3_location} check failed for {variant}: {str(e)}")
                continue

        # All variants failed
        log_event("WARN", f"{s3_location} file not found (checked {variants})")
        return False

    except Exception as outer_e:
        log_event("WARN", f"Error parsing S3 path or checking object {s3_location}: {str(outer_e)}")
        return False

def main():
    """
    AWS Lambda function entry point.
    Expected event format:
    {
        "clientName": "client_123",
        "job_id": ["job_1", "job_2"],  # job_id can be a list of one or more job IDs
        "transactionId": "trans_456"
    }
    """
    try:

        set_job_params_as_env_vars()
        log_event("STARTED", "glue_send_retrieve_message::Started execution")

        transactionId = os.getenv("transactionId")
        ssmDBKey = os.getenv("ssmDBKey")
        bucket = os.getenv("bucket")
        objectKey = os.getenv("objectKey")
        clientName = os.getenv('clientName')

        if not clientName or not transactionId or not ssmDBKey or not bucket or not objectKey or not objectKey:
            error_message = "Missing required parameter"
            raise Exception(error_message)

        # Fetch DB credentials only once per glue execution
        db_credentials = get_db_credentials(ssmDBKey)
        db_pool = initialize_db_pool(db_credentials)

        # Fetch orders based on clientName, job_ids, and status 'COMPLETED'
        orders, all_order_ids = process_orders(db_pool)

        if orders <= 0:
            error_message = f"No orders found for clientName: {clientName}, status: 'ACCEPTED'"
            raise Exception(error_message)

        log_event("ENDED", f"glue_send_retrieve_message::Successfully processed {orders} orders", all_order_ids)

    except Exception as e:
        log_event("FAILED", f"glue_send_retrieve_message::Fatal Error::{traceback.format_exc()}, Exception : {str(e)}")
        raise e

    finally:
        SPLUNK_EXECUTOR.shutdown(wait=True)
        S3_CHECK_POOL.shutdown(wait=True)
    
if __name__ == '__main__':
    main()