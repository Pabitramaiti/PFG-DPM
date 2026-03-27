import os
import json
import boto3
import logging
import psycopg2
import traceback
import re
from collections import defaultdict
from psycopg2 import pool
from dpm_splunk_logger_py import splunk
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
ssm = boto3.client('ssm', region_name='us-east-1')
secrets_client = boto3.client("secretsmanager")
db_pool = None
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
job_id = ""
processed_date = ""


def get_run_id():
    try:
        account_id = boto3.client("sts").get_caller_identity()["Account"]
        return f"arn:dpm:lambda:group_orders:{account_id}"
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

    job_name = "lmbd_group_orders"
    if statemachine_name:
        parts = statemachine_name.split("-")
        if len(parts) >= 3:
            prefix = "_".join(parts[0:3])
            job_name = f"{prefix}_{job_name}"

    chunk_size = 1000

    # Decide if multiple logs needed
    if len(all_order_ids_for_logging) > chunk_size:
        for idx, order_chunk in enumerate(chunk_list(all_order_ids_for_logging, chunk_size), start=1):
            batched_message = f"{message} :: (Batch {idx} of { (len(all_order_ids_for_logging) // chunk_size) + 1 })"
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
                "step_function": "order_grouping_function",
                "component": "lambda",
                "job_name": job_name,
                "job_run_id": execution_id,
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
                print(f"[ERROR] lmbd_group_orders : log_event : Failed to write log batch {idx} to Splunk: {str(e)}")

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
            "step_function": "order_grouping_function",
            "component": "lambda",
            "job_name": job_name,
            "job_run_id": execution_id,
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
            print(f"[ERROR] lmbd_group_orders : log_event : Failed to write log to Splunk: {str(e)}")

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


# ------------------ DB Connection Setup ------------------
def get_db_credentials(context):
    try:
        param = ssm.get_parameter(Name=os.getenv('ssmdb_key'), WithDecryption=True)
        param_value = param['Parameter']['Value']
        response = secrets_client.get_secret_value(SecretId=param_value)
        return json.loads(response['SecretString'])
    except Exception as e:
        raise Exception("Error fetching DB secrets") from e


def initialize_db_pool(db_credentials):
    global db_pool
    if not db_credentials:
        raise ValueError("Missing DB credentials")

    db_pool = psycopg2.pool.SimpleConnectionPool(
        1, 10,
        host=db_credentials["host"],
        port=db_credentials["port"],
        dbname=os.environ["DB_NAME"],
        user=db_credentials["username"],
        password=db_credentials["password"]
    )


def get_db_connection():
    return db_pool.getconn()


def release_db_connection(conn):
    if conn is not None:
        db_pool.putconn(conn)


def none_to_empty_string(value):
    if value is None:
        return ""
    else:
        return str(value)


# ------------------ Helper Functions ------------------
def get_job_ids_from_s3(context, bucket_name, file_key):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read().decode('utf-8').strip()
        global job_id
        job_id = file_content
        job_list = [job.strip() for job in file_content.split(',')] if ',' in file_content else [file_content]
        log_event("INFO", f"lmbd_group_orders : Fetched job names from S3 file : {job_list}: Input_file : {str(file_key)}, context : {context}", context)
        return job_list
    except Exception as e:
        raise e


def format_address(row):
    first_name, last_name, address_line1, address_line2, city, country, zip_code = row
    first_name = none_to_empty_string(first_name)
    last_name = none_to_empty_string(last_name)
    address_line1 = none_to_empty_string(address_line1)
    address_line2 = none_to_empty_string(address_line2)
    city = none_to_empty_string(city)
    country = none_to_empty_string(country)
    zip_code = none_to_empty_string(zip_code)
    return f"{first_name}, {last_name}, {address_line1}, {address_line2}, {city}, {country}, {zip_code}".strip().replace("  ", " ")


def process_orders(grouped_orders, output_bucket, output_key_prefix, bulk_flag, context):

    uploaded_files = []
    for idx, (address, orders) in enumerate(grouped_orders.items(), start=1):
        # pick job name from first order
        job_name = orders[0][4] + f".{bulk_flag}"
        timestamp = datetime.now().strftime("%m%d%Y%H%M%S%f")[:-3]
        filename = f"PFG_{job_name}_RETRIEVE_AND_ZIP_{timestamp}.{idx}.trig"
        local_path = f"/tmp/{filename}"
        s3_key = f"{output_key_prefix}/{filename}".replace("//", "/")

        # write all rows
        with open(local_path, "w") as f:
            lines = []
            for order_id, correlation_id, traceid, orderdata, job, client, status in orders:
                try:
                    orderdata_json = (
                        json.dumps(orderdata)
                        if isinstance(orderdata, dict)
                        else str(orderdata)
                    )
                except Exception:
                    orderdata_json = str(orderdata)
                lines.append(
                    f"{order_id}|{correlation_id}|{traceid}|{orderdata_json}|{job}|{client}|{status}\n"
                )
            f.writelines(lines)

        s3.upload_file(local_path, output_bucket, s3_key)
        os.remove(local_path)
        uploaded_files.append(f"s3://{output_bucket}/{s3_key}")

    return uploaded_files


def get_shippingid(orderdata_dict, context):

    try:
        cc = orderdata_dict.get("communicationCommon", {})
        details = cc.get("communicationPrintDetail", [])
        code = None
        service = None

        for detail in details:
            ids = detail.get("communicationIds") or []
            for cid in ids:
                cid_type = cid.get("communicationIdentifierType")
                cid_val = cid.get("communicationIdentifierValue")
                if cid_type == "shippingCode":
                    code = cid_val
                elif cid_type == "shippingService":
                    service = cid_val
            # Early exit if both found in this detail block
            if code and service:
                return f"{code}_{service}"

        if code and service:
            return f"{code}_{service}"
    except Exception as e:
        log_event("WARN", f"get_shippingid: failed to parse orderdata_dict: {e}", context)

    return None


def get_insertSet(orderdata_dict, context):

    try:
        cc = orderdata_dict.get("communicationCommon", {})
        details = cc.get("communicationPrintDetail", [])
        insert_codes = set()

        for detail in details:
            narratives = detail.get("communicationNarrative") or []
            for n in narratives:
                if n.get("communicationNarrativeType") == "physicalInsertPocketItemId":
                    value = n.get("communicationNarrativeValue")
                    if value:
                        insert_codes.add(str(value).strip())

        return insert_codes
    except Exception as e:
        log_event("WARN", f"get_insertcount: failed to parse orderdata_dict: {e}", context)
        return set()


def group_onprem_orders(records, context):
    """
    Group on-prem orders by (address + shippingid) and limit each group
    to ≤ 11 unique insert codes.
    """

    addr_ship_groups = defaultdict(list)
    grouped_orders = {}
    inserts_at_shipping = {}
    shipping_id_sequence = {}

    sorted_records = sorted(records, key=lambda x: (none_to_empty_string(x[5]), none_to_empty_string(x[6]), none_to_empty_string(x[7]), none_to_empty_string(x[8]), none_to_empty_string(x[9]), none_to_empty_string(x[10]), none_to_empty_string(x[11])))

    for row in sorted_records:
        order_id = str(row[0])
        orderdata = row[3]
        contact_fields = row[4:11]
        job_name = row[11]
        address = format_address(contact_fields)
        is_blank = " , , , ," in str(address)
        bulk = "n" if is_blank else "b"

        try:
            orderdata_dict = (
                json.loads(orderdata)
                if isinstance(orderdata, str)
                else orderdata or {}
            )
        except Exception as e:
            log_event("WARN", f"Bad JSON in order {order_id}: {e}", context)
            orderdata_dict = {}

        shippingid = get_shippingid(orderdata_dict, context)
        insert_codes = get_insertSet(orderdata_dict, context)

        unique_id = shippingid+"_"+bulk+"_"+job_name

        matched_key = None
        
        if bulk == "b":
            for pid, grp in grouped_orders.items():
                if grp["address"] == address and grp["shippingid"] == shippingid:
                     combined = {}
                     if unique_id in inserts_at_shipping:
                         combined = inserts_at_shipping[unique_id]["insert_codes"].union(insert_codes)
                     if len(combined) <= 11:
                         matched_key = pid
                         break
                     else:
                         if unique_id in shipping_id_sequence:
                             shipping_id_sequence[unique_id] = shipping_id_sequence[unique_id] +1
                         else:
                             shipping_id_sequence[unique_id] = 1

        else:
            combined = {}
            if unique_id in inserts_at_shipping:
                combined = inserts_at_shipping[unique_id]["insert_codes"].union(insert_codes)
            if len(combined) <= 11:
                pass
            else:
                if unique_id in shipping_id_sequence:
                    shipping_id_sequence[unique_id] = shipping_id_sequence[unique_id] +1
                else:
                    shipping_id_sequence[unique_id] = 1           

        if matched_key:
            grp = grouped_orders[matched_key]
            grp["orders"].append(order_id)
            grp["insert_codes"].update(insert_codes)
            inserts_at_shipping[unique_id]["insert_codes"].update(insert_codes)
        else:
            if unique_id in shipping_id_sequence:
                pass
            else:
                shipping_id_sequence[unique_id] = 1

            if unique_id in inserts_at_shipping:
                inserts_at_shipping[unique_id]["insert_codes"].update(insert_codes)
            else:
                inserts_at_shipping[unique_id]={
                "insert_codes": set(insert_codes)
                }

            key_tuple = (unique_id,shipping_id_sequence[unique_id])
            seq = shipping_id_sequence[unique_id]
            parent_id = order_id
            grouped_orders[parent_id] = {
                "job_name": job_name,
                "address": address,
                "shippingid": shippingid,
                "insert_codes": set(insert_codes),
                "sequence": seq,
                "bulk": bulk,
                "orders": [order_id],
            }
            addr_ship_groups[key_tuple].append(parent_id)

    for grp in grouped_orders.values():
        if isinstance(grp.get("insert_codes"), set):
            grp["insert_codes"] = list(grp["insert_codes"])

    '''
    try:
        for parent_order_id, g in grouped_orders.items():
            log_event(
                "INFO",
                (
                    f"Final on-prem group: parent_order_id={parent_order_id}, "
                    f"job_name={g.get('job_name')}, address={g.get('address')!r}, "
                    f"shippingid={g.get('shippingid')}, seq={g.get('sequence')}, "
                    f"bulk={g.get('bulk')}, insert_codes={g.get('insert_codes')}, "
                    f"orders_count={len(g.get('orders', []))}, orders={g.get('orders')}"
                ),
                context,
            )
    except Exception as e:
        log_event("WARN", f"Error logging final groups: {e}", context)
    '''

    log_event(
        "INFO",
        f"Built grouped_orders for on-prem using address+shippingid: {len(grouped_orders)} total groups created",
        context,
    )

    return grouped_orders


def safe_commit(connection, context):
    try:
        if connection and not connection.closed:
            connection.commit()
    except Exception as e:
        log_event("WARN", f"Commit skipped/not possible: {e}", context)

def write_output_file(data, bucket_name, key_prefix, output_file, context):

    # Skip empty data
    if not data:
        log_event("WARN", f"No data data to write for s3://{bucket_name}/{key_prefix}/{output_file}. Skipping upload.", context)
        return

    # Try convert to JSON
    try:
        if isinstance(data, list):
            body = json.dumps({"order_ids": data, "count": len(data)}, indent=2)
        else:
            body = json.dumps(data, indent=2)
    except (TypeError, ValueError) as e:
        log_event("WARN", f"Failed to convert data to JSON for {bucket_name}/{key_prefix}/{output_file}: {e}. Skipping upload.", context)
        return

    # Try upload to S3
    key_prefix = key_prefix.strip("/")  # remove leading/trailing slashes
    key = f"{key_prefix}/{output_file}" if key_prefix else output_file
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        #log_event("INFO", f"Successfully uploaded data file to s3://{bucket_name}/{key}", context)
    except Exception as e:
        log_event("WARN", f"Failed to write data data file to s3://{bucket_name}/{key}::Exception: {e}", context)

# ------------------ Main Lambda Handler ------------------
def lambda_handler(event, context):

    try:
        global all_order_ids
        splunk.log_message({"Message": "Started Lambda: group_orders", "Status": "SUCCESS"}, context)

        # === INPUT PARAMS ===
        os.environ['ssmdb_key'] = event.get("ssmDBKey")
        input_bucket = event.get("bucket")
        input_key = event.get("objectKey")
        output_bucket = event.get("outputBucket")
        output_key_prefix = event.get("outputObjectKey")
        destination_type = event.get("destinationType") or "unknown"

        # === META INFO ===
        global client_name, clientID, application_name, execution_name, execution_id, execution_starttime, processed_date
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
        processed_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        log_event("STARTED", f"lmbd_group_orders job started with Destination : {destination_type} : event: {json.dumps(event)}", context)

        if not client_name:
            raise Exception("clientName is required")

        # === LOAD JOB IDS FROM S3 ===
        job_ids = get_job_ids_from_s3(context, input_bucket, input_key)
        if not job_ids:
            raise Exception("No job_ids found")

        db_credentials = get_db_credentials(context)
        initialize_db_pool(db_credentials)
        conn = get_db_connection()
        cursor = conn.cursor()

        placeholders = ','.join(['%s'] * len(job_ids))

        # === FETCH ALL ORDER DATA
        query = f"""
            SELECT o.orderid, o.correlation_id, o.traceid, o.orderdata,
                   c.first_name, c.last_name, c.address_line1, c.address_line2,
                   c.city, c.country, c.zip_code,
                   o.job_name, o.client, o.status
            FROM order_table o
            LEFT JOIN contact_details c ON o.orderid = c.orderid
            WHERE o.client = %s
              AND o.job_name IN ({placeholders})
              AND o.status = 'COMPLETED'
        """
        cursor.execute(query, [client_name] + job_ids)
        records = cursor.fetchall()
        log_event("INFO", f"Fetched {len(records)} records from DB", context)

        if not records:
            safe_commit(conn, context)
            return {"statusCode": 200, "body": json.dumps({"status": "Success", "files": []})}

        # === STEP: BULK GET VALID ORDERS FROM retrieve_table ===
        all_order_ids = [row[0] for row in records]
        placeholders_ids = ','.join(['%s'] * len(all_order_ids))
        cursor.execute(
            f"SELECT orderid FROM retrieve_table "
            f"WHERE orderid IN ({placeholders_ids}) AND s3location IS NOT NULL", 
            all_order_ids
        )
        valid_order_ids = {row[0] for row in cursor.fetchall()}

        #log_event("INFO", f"{len(valid_order_ids)} orders are valid for retrieve_table::input_key={str(input_key)}::Context={context}", context)

        invalid_orders = [
            {
                "order_id": row[0],
                "correlation_id": row[1],
                "trace_id": row[2],
                "job_name": row[11],
                "client": row[12],
            }
            for row in records
            if row[0] not in valid_order_ids
        ]

        # === BATCH UPDATE INVALID ORDERS ===
        if invalid_orders:
            invalid_order_ids = [o["order_id"] for o in invalid_orders]
            placeholders_invalid = ','.join(['%s'] * len(invalid_order_ids))
            cursor.execute(
                f"UPDATE order_table SET status='NO-PRINT-DOCS' "
                f"WHERE orderid IN ({placeholders_invalid})",
                invalid_order_ids,
            )
            log_event("INFO", f"Marked {len(invalid_orders)} invalid orders as NO-PRINT-DOCS::invalid_order_ids={invalid_order_ids}", context)

            # send one event per invalid order to Splunk
            def send_splunk_event(order):
                event_data = {
                    "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "event_type": "ORDER_STATUS_CHANGE",
                    "order_id": order.get("order_id", " "),
                    "previous_status": "COMPLETED",
                    "new_status": "NO-PRINT-DOCS",
                    "client": order.get("client", " "),
                    "correlation_id": order.get("correlation_id", " "),
                    "trace_id": order.get("trace_id", " "),
                    "job_id": order.get("job_name", " "),
                }
                try:
                    splunk.log_message(event_data, context)
                except Exception as e:
                    print(f"[ERROR] Failed Splunk log for order {order.get('order_id')}: {e}")

            max_threads = min(20, len(invalid_orders)) 
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                futures = [executor.submit(send_splunk_event, o) for o in invalid_orders]
                for f in as_completed(futures):
                    try:
                        f.result()
                    except Exception as e:
                        print(f"[ERROR] Thread exception while logging to Splunk: {e}")

            #log_event("INFO", f"Processed all {len(invalid_orders)} invalid orders", context)

        valid_records = [row for row in records if row[0] in valid_order_ids]
        if not valid_records:
            log_event("WARN", "No valid orders remain after filtering.", context)
            safe_commit(conn, context)
            return {"statusCode": 200, "body": json.dumps({"status": "Success", "files": []})}

        # === FILE CREATION & UPLOAD TO S3 ===
        uploaded_files = []
        if destination_type.lower() in ("on-prem", "onprem"):
            log_event("INFO", f"Processing onprem orders, destination_type={destination_type}", context)
            record_lookup = {str(r[0]): r for r in valid_records}
            grouped_orders = group_onprem_orders(valid_records, context)

            ts = datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")
            prefix = f"{output_key_prefix}".replace("input", "save")
            json_file_name = f"PFG_grouped_onprem_orders_{ts}.json"
            write_output_file(grouped_orders, output_bucket, prefix, json_file_name, context)

            uploaded_files = []
            final_grouped_orders = {}
            order_to_parent = []

            for parent_order, g in grouped_orders.items():
                orders = g["orders"]
                job_name = g["job_name"]
                bulk_flag = g["bulk"]
                seq = g["sequence"]
                shipping_id = g["shippingid"]

                # store mapping for later bulk update
                for oid in orders:
                    order_to_parent.append((oid, parent_order))

                key_string = job_name+"_"+bulk_flag+"_"+shipping_id+"_"+str(seq)

                if key_string in final_grouped_orders:
                    final_grouped_orders[key_string]["orders"].extend(g["orders"])
                else:
                    final_grouped_orders[key_string]={
                        "orders" : g["orders"],
                        "job_name" : g["job_name"],
                        "bulk" : g["bulk"],
                        "sequence" : g["sequence"]
                    }

            BATCH_SIZE = 1000
            for i in range(0, len(order_to_parent), BATCH_SIZE):
                batch = order_to_parent[i : i + BATCH_SIZE]
                order_ids = [row[0] for row in batch]
                placeholders = ",".join(["%s"] * len(order_ids))

                args = []
                updates_by_parent = {}
                for oid, parent_order in batch:
                    updates_by_parent.setdefault(parent_order, []).append(oid)

                for parent_order, oids in updates_by_parent.items():
                    placeholders = ",".join(["%s"] * len(oids))
                    cursor.execute(
                        f"UPDATE order_table SET parent_order=%s WHERE orderid IN ({placeholders})",
                        [parent_order] + oids,
                    )
                    cursor.execute(
                        f"UPDATE retrieve_table SET parent_order=%s WHERE orderid IN ({placeholders})",
                        [parent_order] + oids,
                    )

            log_event("INFO", f"Updated parent_order for {len(order_to_parent)} orders in batches of {BATCH_SIZE}", context)

            json_file_name = f"PFG_final_grouped_onprem_orders_{ts}.json"
            write_output_file(final_grouped_orders, output_bucket, prefix, json_file_name, context)

            for parent_order, g in final_grouped_orders.items():
                orders = g["orders"]
                job_name = g["job_name"]
                bulk_flag = g["bulk"]
                seq = g["sequence"]

                timestamp = datetime.now().strftime("%m%d%Y%H%M%S%f")[:-3]
                counter = len(uploaded_files) + 1
                #filename = f"PFG_{job_name}.{bulk_flag}.{int(seq)}_RETRIEVE_AND_ZIP_{timestamp}.{counter}.trig"
                filename = f"PFG_{job_name}.{bulk_flag}_RETRIEVE_AND_ZIP_{timestamp}.{counter}.trig"
                log_event("INFO", "Filename : "+filename+" Orders : "+str(orders), context)
                local_path = f"/tmp/{filename}"
                s3_key = f"{output_key_prefix}/{filename}".replace("//", "/")


                with open(local_path, "w") as f:
                    lines = []
                    for oid in orders:
                        row = record_lookup.get(str(oid))
                        if not row:
                            continue
                        order_id, correlation_id, traceid, orderdata = row[0:4]
                        job, client, status = row[11:14]
                        try:
                            orderdata_json = (
                                json.dumps(orderdata)
                                if isinstance(orderdata, dict)
                                else str(orderdata)
                            )
                        except Exception:
                            orderdata_json = str(orderdata)
                        lines.append(
                            f"{order_id}|{correlation_id}|{traceid}|{orderdata_json}|{job}|{client}|{status}\n"
                        )
                    f.writelines(lines)

                s3.upload_file(local_path, output_bucket, s3_key)
                os.remove(local_path)
                uploaded_files.append(f"s3://{output_bucket}/{s3_key}")


            #log_event("ENDED", f"Uploaded {len(uploaded_files)} on-prem trigger files: {uploaded_files}", context)

        else:
            log_event("INFO", f"Processing others orders, destination_type={destination_type}", context)
            grouped_orders = defaultdict(list)
            for row in valid_records:
                order_id, correlation_id, traceid, orderdata = row[0:4]
                contact_fields = row[4:11]
                job_name, client, status = row[11:14]
                address = format_address(contact_fields)
                grouped_orders[address].append((order_id, correlation_id, traceid, orderdata, job_name, client, status))

            bulk_orders = {k: v for k, v in grouped_orders.items() if str(k) == "BULK"}
            bulk_order_ids = [order[0] for orders in bulk_orders.values() for order in orders]

            non_bulk_orders = {k: v for k, v in grouped_orders.items() if str(k) != "BULK"}
            non_bulk_order_ids = [order[0] for orders in non_bulk_orders.values() for order in orders]

            log_event("INFO", f"Total bulk_orders={len(bulk_orders)}, non_bulk_orders={len(non_bulk_orders)}, destination_type={destination_type}", context)

            if bulk_orders:
                uploaded_files.extend(process_orders(bulk_orders, output_bucket, output_key_prefix, "b", context))
            if non_bulk_orders:
                uploaded_files.extend(process_orders(non_bulk_orders, output_bucket, output_key_prefix, "n", context))

        if uploaded_files:
            log_event("ENDED", f"Uploaded {len(uploaded_files)} trigger files: {uploaded_files}", context, all_order_ids)
        else:
            log_event("ENDED", "Uploaded 0 trigger files.", context, all_order_ids)

        safe_commit(conn, context)
        return {"statusCode": 200, "body": json.dumps({"status": "Success", "files": uploaded_files})}

    except Exception as e:
        log_event("FAILED", f"Error: {str(e)}\n{traceback.format_exc()}", context)
        raise e

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            release_db_connection(conn)