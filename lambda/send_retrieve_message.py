import json
import psycopg2
import requests
from datetime import datetime, timezone
import os
from psycopg2 import pool
from dpm_splunk_logger_py import splunk
import boto3
import traceback
import uuid
import time
import csv
import re
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed

# Global Variables
db_pool = None

# Initialize boto3 client for s3
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
        return f"arn:dpm:lambda:lmbd_send_retrieve_message:{account_id}"
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

def log_event(status, message, context):

    job_name = "lmbd_send_retrieve_message"
    if statemachine_name:
        parts = statemachine_name.split("-")
        if len(parts) >= 3:
            prefix = "_".join(parts[0:3])
            job_name = f"{prefix}_{job_name}"


    all_order_ids_for_logging = all_order_ids if "all_order_ids" in globals() else []

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
                "order_ids": json.dumps(order_chunk),
                "step_function": "orders_data_aggregation",
                "component": "lambda",
                "job_name": job_name,
                "job_run_id": get_run_id(),
                "compressed_file": " ",
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
                print(f"[ERROR] lmbd_send_retrieve_message : log_event : Failed to write log batch {idx} to Splunk: {str(e)}")

    else:
        event_data = {
            "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "transactionId": transactionId,
            "clientName": client_name,
            "clientID": clientID,
            "applicationName": application_name,
            "order_ids": json.dumps(all_order_ids_for_logging),
            "step_function": "orders_data_aggregation",
            "component": "lambda",
            "job_name": job_name,
            "job_run_id": get_run_id(),
            "compressed_file": " ",
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
            print(f"[ERROR] lmbd_send_retrieve_message : log_event : Failed to write log to Splunk: {str(e)}")

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

# PostgreSQL connection details (set via AWS Lambda environment variables)
def get_db_credentials(context, ssmdb_key):
    """
    Fetch PostgreSQL credentials from AWS Secrets Manager.
    """
    try:
        ssm = boto3.client('ssm', region_name='us-east-1')
        ssmdbkey = ssmdb_key
        secret_name = ssm.get_parameter(Name=ssmdbkey, WithDecryption=True)
        secret_name = secret_name['Parameter']['Value']
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)
        secret_value = response['SecretString']
        json_secret_value = json.loads(secret_value)
        return json_secret_value
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

# 🔹 Function to Read Job IDs from S3
# def get_job_ids_from_s3(context, bucket_name, file_key):
#     """
#     Fetch job_ids from an S3 bucket JSON file.
#     """
#     try:
#         response = s3.get_object(Bucket=bucket_name, Key=file_key)
#         file_content = response['Body'].read().decode('utf-8').strip()
#         job_list = [job.strip() for job in file_content.split(',')] if ',' in file_content else [file_content]
#         return job_list
#     except Exception as e:
#         splunk.log_message({"Status": "Failed", "Message": f"Error fetching job IDs from S3: {traceback.format_exc()}"}, context)
#         return []


def get_db_connection():
    return db_pool.getconn()

def release_db_connection(conn):
    db_pool.putconn(conn)    


def get_orders(context, s3_bucket, input_file_path):
    try:
        response = s3.get_object(Bucket=s3_bucket, Key=input_file_path)
        content = response['Body'].read().decode('utf-8').strip()
        order_ids = []

        lines = content.splitlines()
        buffer = []
        orders = []

        for line in lines:
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

                orders.append({
                    "order_id": order_id,
                    "correlation_id": correlation_id,
                    "orderdata": orderdata,
                    "job_name": job_name,
                    "client": client,
                    "status": status
                })

                buffer = []  # Reset buffer for next record

        if not orders:
            raise Exception("No valid orders found in the file.")
        return orders, order_ids

    except Exception as e:
        error_message = f"Error reading .trig file: {str(e)}"
        raise Exception(error_message) from e

def get_s3locations(context, order_id):
    """
    Fetch s3 locations from the PostgreSQL retrieve_table based on order_id
    """
    connection = None
    cursor = None

    try:
        connection = get_db_connection()
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
        error_message = f"Error fetching data from retrieve table: {str(e)}"
        if connection:
            connection.rollback()
        raise Exception(error_message) from e
    finally:
        # Close cursor and connection safely
        if cursor:
            cursor.close()
        if connection:
            release_db_connection(connection)


def get_kafka_message(context, record, client_id,document_id,s3_location):
    try:
        kafka_message =[]
        data_dict={}
        corelation_id = ""
        myuuid = uuid.uuid4()
        #log_event("INFO", f"lmbd_send_retrieve_message job: Data received from SQL query: Data: {','.join(map(str, record))}", context)

        corelation_id = record['correlation_id']
        data_dict["orderdata"] = json.loads(record['orderdata'])
        if corelation_id == "" :
            corelation_id = myuuid
        order_id = data_dict["orderdata"]["communicationCommon"]["orderId"]
        for data in data_dict["orderdata"]["communicationDocuments"]:
            #log_event("INFO", f"lmbd_send_retrieve_message job: Checking for document id={document_id}, S3_location={s3_location}", context)

            doc_id = ""
            
            if "documentId" in data:
                if str(data["documentId"]) != str(document_id):
                    continue
                else:
                    doc_id = data["documentId"]
                    #log_event("INFO", f"lmbd_send_retrieve_message job: Document is matching doc_id={doc_id} with document_id={document_id}", context)
            else:
                doc_id = str(document_id)


            s3_location = s3_location.replace('composedFile.pdf/composedFile','composedFile.pdf')

            message_dict = {}
            message_dict["documentRegistryIdentifier"] = client_id
            message_dict["communicationOrigin"] = "DPM-AGGREGATOR"
            message_dict["communicationActionType"] = "retrieve"
            message_dict["communicationDocumentSequenceNumber"] = data["communicationDocumentSequenceNumber"]
            message_dict["orderId"] = order_id 
            message_dict["documentId"] = doc_id
            message_dict["s3locator"] = str(s3_location)
            message_dict["clientId"] = client_id
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

            #log_event("INFO", f"lmbd_send_retrieve_message job : Retrive message created: Data : {json.dumps(kafka_message)}", context)

        return kafka_message
    except Exception as e:
        error_message = f"Error in get_kafka_message: {str(e)}"
        log_event("WARN", error_message, context)
        return None

def send_to_kafka_api(context, messages):
    """
    Publish message to Kafka via API call
    """
    success = True
    for message in messages:
        try:
            headers = {"Content-Type": "application/json", 'message-source': 'DPM-AGGREGATOR','Accept': 'application/json'}
            response = requests.post(KAFKA_API_URL, json=message, headers=headers)
            if response.status_code != 200:
                success = False
                log_event("WARN", f"Failed to send message to Kafka API. Status Code={response.status_code}, message={message}", context)
        except requests.RequestException as e:
            error_message = f"Error publishing message={message} to Kafka via API call: {str(e)}"
            raise Exception(error_message) from e

    return success


def update_order_status(context, client_id, job_name, new_status, order_ids):
    """Update order_status (e.g., to 'PROCESSING') for a given client_id, job_name, and order IDs."""
    connection = None
    cursor = None
    update_query = None
    try:
        # Establish connection
        connection = get_db_connection()
        cursor = connection.cursor()
        update_query = """
        UPDATE order_table 
        SET status = %s
        WHERE client = %s
            AND job_name = %s
            AND status IN ('COMPLETED', 'ACCEPTED')
            AND orderid IN %s;
        """
        cursor.execute(update_query, (new_status ,client_id, job_name, tuple(order_ids)))
        connection.commit()

        # --- CONCURRENT SPLUNK LOGGING ---
        def send_splunk_event(order_id):
            event_data = {
                "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "event_type": "ORDER_STATUS_CHANGE",
                "order_id": order_id,
                "previous_status": "COMPLETED",
                "new_status": new_status,
                "correlation_id": " ",
                "trace_id": " ",
                "client": client_id,
                "job_id": job_name,
            }
            try:
                splunk.log_message(event_data, context)
            except Exception as e:
                print(f"[ERROR] lmbd_send_retrieve_message : log_event : Failed to write log to Splunk: {str(e)}")

        max_threads = min(20, len(order_ids))
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = [executor.submit(send_splunk_event, oid) for oid in order_ids]
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    print(f"[ERROR] Thread exception while logging to Splunk: {e}")

        log_event("INFO", f"Updated order status to {new_status} for {len(order_ids)} orders, client_id={client_id}, job_name={job_name}", context)

    except psycopg2.Error as e:
        error_message = (
            f"Error updating order_table for order_id={order_ids}, "
            f"job_id={job_name}, client_id={client_id}, "
            f"update_query={update_query}, "
            f"params={(new_status, client_id, job_name, tuple(order_ids))}, error message : {str(e)}"
        )
        if connection:
            connection.rollback()
        raise Exception(error_message) from e
    finally:
        # Close cursor and connection safely
        if cursor:
            cursor.close()
        if connection:
            release_db_connection(connection)

def insert_transaction(context, transactions):
    """
    Insert transaction details into the PostgreSQL database
    """
    connection = None
    cursor = None

    try:
        transactions_list = ','.join(transactions)
        connection = get_db_connection()
        cursor = connection.cursor()
        query = f"""
            INSERT INTO order_transactions (orderid, transactionid, client)
            VALUES {transactions_list}
        """
        cursor.execute(query, (transactions_list))
        connection.commit()

    except psycopg2.Error as e:
        error_message = (
            f"Error inserting into order_transactions table: {e}"
            f"INSERT INTO order_transactions (orderid, transactionid, client) VALUES transactions_list={transactions_list}"
        )
        if connection:
            connection.rollback()
        raise Exception(error_message) from e
    finally:
        # Close cursor and connection safely
        if cursor:
            cursor.close()
        if connection:
            release_db_connection(connection)

def is_s3_location_present(context, s3_location):
    is_file_present = True
    try:
        path = s3_location[5:]
        bucket, key = path.split('/', 1)
        s3.get_object(Bucket=bucket, Key=key)
        #log_event("INFO", f"File is already present in s3_location={s3_location}", context)
    except Exception as e:
        #log_event("WARN", f"{s3_location} file not found due to {str(e)}", context)
        try:
            path = s3_location[5:]
            bucket, key = path.split('/', 1)
            if ".pdf" in key:
                key = key.replace('.pdf','')
            else:
                key = key + '.pdf'
            #log_event("INFO", f"Checking for file with key={key}", context)
            s3.get_object(Bucket=bucket, Key=key)
        except Exception as e:
            log_event("WARN", f"{s3_location} file not found second time too due to {str(e)}", context)
            is_file_present = False
    return is_file_present

def lambda_handler(event, context):
    try :
        """
        AWS Lambda function entry point.
        Expected event format:
        {
            "client_id": "client_123",
            "job_id": ["job_1", "job_2"],  # job_id can be a list of one or more job IDs
            "transaction_id": "trans_456"
        }
        """
        client_id = event.get("clientName")
        transaction_id = event.get("transactionId")
        ssmdb_key = event.get("ssmDBKey")
        s3_bucket = event.get("bucket")
        s3_file_key = event.get("objectKey")
        input_file_path = event.get("objectKey")

        global all_order_ids
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
        state_name = event.get("state_name", "")
        state_enteredtime = event.get("state_enteredtime", "")
        transactionId = event.get("transactionId", "")
        teamsId = event.get("teamsId", "")

        log_event("STARTED", f"lmbd_send_retrieve_message job started with with event: {json.dumps(event)}", context)

        if not client_id or not transaction_id or not ssmdb_key or not s3_bucket or not s3_file_key or not input_file_path:
            error_message = "Missing required parameter"
            raise Exception(error_message)
        
        # job_ids = get_job_ids_from_s3(context, s3_bucket, s3_file_key)
        # if not job_ids:
        #     error_message = "No job IDs found in S3."
        #     splunk.log_message({"Status": "success", "Message": error_message}, context)
        #     raise Exception(error_message)

        # Fetch DB credentials only once per Lambda execution
        global db_pool
        if db_pool is None:
            db_credentials = get_db_credentials(context, ssmdb_key)
            if not db_credentials:
                error_message = "Failed to retrieve DB credentials"
                raise Exception(error_message)        
            initialize_db_pool(db_credentials)

        # Fetch orders based on client_id, job_ids, and status 'COMPLETED'
        try:
            orders, all_order_ids = get_orders(context, s3_bucket, input_file_path)
        except Exception as e:
            error_message = f"Error fetching data from trigger file: {e}"
            raise Exception(error_message) from e
        if not orders:
            error_message = f"No orders found for client_id: {client_id}, status: 'COMPLETED'"
            raise Exception(error_message)
        
        valid_orders = []
        bad_orders = []
        transactions = []
        job_name = ""
            
        for record in orders:
            
            kafka_send_status = True
            is_file_present = True
            s3_locations = []
            order_status_value = 'PROCESSING'
            order_status_reason = 'Valid data'
            job_name = f"{record['job_name']}"

            #s3_locations = get_s3locations(context, record['order_id'])

            #if s3_locations:
            s3_location = []
                #for s3_location in s3_locations:
            s3_file_location = ""
            #s3_file_location = s3_location[1]
            is_file_present = True
            #is_file_present = is_s3_location_present(context,s3_file_location)
            if is_file_present == False:
                log_event("WARN", f"S3 file not found, so sending retrive message for s3_location={s3_location} ", context)
                try:
                        path = s3_file_location[5:]
                        bucket, key = path.split('/', 1)
                        kafka_message = get_kafka_message(context, record, client_id,s3_location[0],key)
                        if kafka_message is not None:
                            kafka_send_status = send_to_kafka_api(context, kafka_message)
                        else:
                            order_status_value = 'BAD DATA'
                            kafka_send_status = True
                            order_status_reason = 'Not able to create retrieve message, could be missing required fields'
                            break
                except Exception as e:
                    if kafka_send_status != True :
                        counter = 1
                        while counter < 4:
                            time.sleep(10)
                            kafka_send_status = send_to_kafka_api(context, kafka_message)
                            if kafka_send_status:
                                counter = 5
                            else:
                                counter=counter+1
                    if kafka_send_status != True :
                        raise Exception(f"Failed to send Kafka message for order {record['order_id']} after retries") from e
            #else:
            #    order_status_value = 'BAD DATA'
            #    kafka_send_status = True
            #    order_status_reason = 'No S3 locations were found for this order in retrieve_table'
            
            if kafka_send_status:
                try:
                    valid_orders.append(record['order_id'])
                except Exception as e:
                    error_message = f"Failed to update order status: {e}"
                    raise Exception(error_message) from e
                try:
                    if 'BAD DATA' != order_status_value:
                        transaction = f"('{record['order_id']}','{transaction_id}','{client_id}')"
                        transactions.append(transaction)
                    else:
                        bad_orders.append(record['order_id'])
                        log_event("WARN", f"Order was not considered for Aggregation, because order_status_reason={order_status_reason}, order_id={record['order_id']} ", context)
                except Exception as e:
                    error_message = f"Failed to inserting transaction_id, client_id into order_transactions table: {e}"
                    raise Exception(error_message) from e
            else:
                error_message = f"Skipping order {record['order_id']} update & transaction due to Kafka failure"
                raise Exception(f"Failed to process orders: {error_message}")

        if len(valid_orders) > 0 :
            insert_transaction(context, transactions)
            update_order_status(context, client_id, job_name, 'PROCESSING',valid_orders)
        if len(bad_orders) > 0 :
            update_order_status(context, client_id, job_name, 'BAD_DATA' ,bad_orders)

        log_event("ENDED", f"lmbd_send_retrieve_message job completed successfully.", context)
        return {"statusCode": 200, "body": f"Processed {len(orders)} orders"}

    except Exception as e:
        log_event("FAILED", f"Error: {traceback.format_exc()}, Exception : {str(e)}, Context: {context}", context)
        raise e