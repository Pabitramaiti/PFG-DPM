# Standard library
import os
import sys
import re
import json
import traceback
from datetime import datetime, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# Third‑party libraries
import boto3
from psycopg2.pool import ThreadedConnectionPool
from botocore.exceptions import ClientError

# Application / local modules
import splunk


# Initialize boto3 client for s3
s3 = boto3.client('s3')
ssm = boto3.client('ssm', region_name='us-east-1')
client_db = boto3.client("secretsmanager")
AWS_ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
SPLUNK_EXECUTOR = ThreadPoolExecutor(max_workers=20)
job_id = ""
processed_date = ""
cronConfig = {}

def ns(x):
    return "" if x is None else str(x)

def get_job_name():
    job_name = "glue_group_orders"
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

    if len(all_order_ids_for_logging) > chunk_size:
        for idx, order_chunk in enumerate(chunk_list(all_order_ids_for_logging, chunk_size), start=1):
            batched_message = f"{message} :: (Batch {idx} of { (len(all_order_ids_for_logging) // chunk_size) + 1 })"
            event_data = {
                "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "transactionId": os.getenv("transactionId", " "),
                "clientName": os.getenv("clientName", " "),
                "clientID": os.getenv("clientID", " "),
                "applicationName": os.getenv("applicationName", " "),
                "processed_date" : processed_date,
                "total_orders": len(all_order_ids_for_logging),
                "job_id": job_id,
                "order_ids": order_chunk,
                "step_function": "order_grouping_function",
                "component": "glue",
                "job_name": get_job_name(),
                "job_run_id": os.getenv("execution_id", " "),
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
                send_splunk_event_async(event_data)
            except Exception as e:
                print(f"[ERROR] glue_copy_files : log_event : Failed to write log batch {idx} to Splunk: {str(e)}")

    else:
        event_data = {
            "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "transactionId": os.getenv("transactionId", " "),
            "clientName": os.getenv("clientName", " "),
            "clientID": os.getenv("clientID", " "),
            "applicationName": os.getenv("applicationName", " "),
            "processed_date" : processed_date,
            "total_orders": len(all_order_ids_for_logging),
            "job_id": job_id,
            "order_ids": all_order_ids_for_logging,
            "step_function": "order_grouping_function",
            "component": "glue",
            "job_name": get_job_name(),
            "job_run_id": os.getenv("execution_id", ""),
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
            send_splunk_event_async(event_data)
        except Exception as e:
            print(f"[ERROR] glue_group_orders : log_event : Failed to write log to Splunk: {str(e)}")

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
            print(f"Email sent! ToAddresses: {os.getenv('teamsId')}")
            print("SES send_email full response:\n", json.dumps(response, indent=2))
        except ClientError as e:
            print(f"[ERROR] Failed to send email: {str(e)}")


# ------------------ DB Connection Setup ------------------
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
        1, 10,
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


# ------------------ Helper Functions ------------------
def get_job_ids_from_s3(bucket_name, file_key):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read().decode('utf-8').strip()
        global job_id, processed_date
        job_id = file_content
        processed_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        job_list = [job.strip() for job in file_content.split(',')] if ',' in file_content else [file_content]
        log_event("INFO", f"glue_group_orders : Fetched job names from S3 file : {job_list}: Input_file : {str(file_key)}")
        return job_list
    except Exception as e:
        raise e


def format_address(row):
    (
        first_name,
        last_name,
        name,
        address_line1,
        address_line2,
        city,
        province_country,
        country,
        zip_code,
    ) = row

    first_name = ns(first_name)
    last_name = ns(last_name)
    name = ns(name)
    address_line1 = ns(address_line1)
    address_line2 = ns(address_line2)
    city = ns(city)
    province_country = ns(province_country)
    country = ns(country)
    zip_code = ns(zip_code)

    return (
        f"{first_name}, {last_name}, {name}, "
        f"{address_line1}, {address_line2}, "
        f"{city}, {province_country}, {country}, {zip_code}"
        .strip()
        .replace("  ", " ")
    )


def process_orders(grouped_orders, output_bucket, output_key_prefix, bulk_flag):

    uploaded_files = []
    for idx, (address, orders) in enumerate(grouped_orders.items(), start=1):
        # pick job name from first order
        job_name = orders[0][4] + f".{bulk_flag}"
        timestamp = datetime.now().strftime("%m%d%Y%H%M%S%f")[:-3]
        filename = f"PFG_{job_name}_RETRIEVE_AND_ZIP_{timestamp}.{idx}.trig"
        local_path = f"/tmp/{filename}"
        s3_key = f"{output_key_prefix}/{filename}".replace("//", "/")

        # write all rows
        with open(local_path, "w") as file:
            for order_id, correlation_id, traceid, orderdata, job, client, status in orders:
                try:
                    orderdata_json = (
                        json.dumps(orderdata)
                        if isinstance(orderdata, dict)
                        else str(orderdata)
                    )
                except Exception:
                    orderdata_json = str(orderdata)

                file.write(f"{order_id}|{correlation_id}|{traceid}|{orderdata_json}|{job}|{client}|{status}\n")

        s3.upload_file(local_path, output_bucket, s3_key)
        os.remove(local_path)
        uploaded_files.append(f"s3://{output_bucket}/{s3_key}")

    return uploaded_files


def get_shippingid(orderdata_dict):

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
        log_event("WARN", f"get_shippingid: failed to parse orderdata_dict: {e}")

    return ""


def get_insertSet(orderdata_dict):

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
        log_event("WARN", f"get_insertcount: failed to parse orderdata_dict: {e}")
        return set()
    
def get_bulkflag(orderdata_dict):

    try:
        cc = orderdata_dict.get("communicationCommon", {})
        details = cc.get("communicationFlags", [])
        bulk_flag = False

        for n in details:
            if n.get("communicationFlagType") == "isBulkShipping":
                value = n.get("communicationFlagValue")
                if value == "false":
                    bulk_flag = False
                else :
                    bulk_flag = True
                    

        return bulk_flag
    except Exception as e:
        log_event("WARN", f"get_bulkflag: failed to parse orderdata_dict: {e}")
        return False

MISSING_BULK_LOGGED = set()

def get_bulk_type(orderdata_dict, job_name, order_id):

    # Non-bulk case
    if not get_bulkflag(orderdata_dict):
        return "n"

    # Highest‑priority rule: shipping package override
    shipping_pkg = get_shippingPackageId(orderdata_dict)
    if shipping_pkg == "4":
        return "c"

    # Job‑based bulk mapping (JSON config)
    job_cfg = BULK_JOB_CONFIG.get("bulk_job_config", {}).get(job_name)

    if job_cfg and "bulk_code" in job_cfg:
        return job_cfg["bulk_code"]


    if job_name not in MISSING_BULK_LOGGED:
        log_event("INFO",f"No bulk_code configured for product {job_name}, order_id: {order_id}; using default bulk code 'b'")
        MISSING_BULK_LOGGED.add(job_name)

    # Fallback default
    return BULK_JOB_CONFIG.get("defaults", {}).get("bulk_code", "b")

def get_shippingPackageId(orderdata_dict):

    try:
        cc = orderdata_dict.get("communicationCommon", {})
        details = cc.get("communicationPrintDetail", [])
        value = None

        for detail in details:
            ids = detail.get("communicationIds") or []
            for cid in ids:
                cid_type = cid.get("communicationIdentifierType")
                cid_val = cid.get("communicationIdentifierValue")
                if cid_type == "shippingPackageId":
                    return f"{cid_val}"

    except Exception as e:
        log_event("WARN", f"get_shippingPackageId: failed to parse orderdata_dict: {e}")

    return None

def get_physicalInsertItemId(orderdata_dict):

    try:
        cc = orderdata_dict.get("communicationCommon", {})
        details = cc.get("communicationPrintDetail", [])
        value = None

        for detail in details:
            ids = detail.get("communicationIds") or []
            for cid in ids:
                cid_type = cid.get("communicationIdentifierType")
                cid_val = cid.get("communicationIdentifierValue")
                if cid_type == "PhysicalInsertItemId":
                    return f"{cid_val}"

    except Exception as e:
        log_event("WARN", f"get_physicalInsertItemId: failed to parse orderdata_dict: {e}")

    return None

# ------------------ Corp Mapping Builder ------------------

def build_corp_mapping_from_cron(cron_config):

    corp_map = {}

    for item in cron_config.get("triggerDetails", []):
        product_str = item.get("product", "")
        corp_mapping = item.get("corp_mapping")

        # Skip if no corp_mapping defined
        if not corp_mapping:
            continue

        products = [p.strip() for p in product_str.split(",") if p.strip()]

        for prod in products:
            corp_map[prod] = corp_mapping

    return corp_map

# ------------------ Job Name Resolver ------------------

def resolve_job_name(original_job_name, bulk_code):

    # No corp mapping configured → use original
    if original_job_name not in CORP_MAPPING:
        return original_job_name

    mapping = CORP_MAPPING.get(original_job_name, {})

    # Bulk specific mapping
    if bulk_code in mapping:
        return mapping[bulk_code]

    # Default mapping support (optional)
    if "default" in mapping:
        return mapping["default"]

    # Safe fallback
    return original_job_name

def build_bulk_job_config_from_cron(cron_config):

    bulk_cfg = {}

    for item in cron_config.get("triggerDetails", []):
        product_str = item.get("product", "")
        bulk_code = item.get("bulk_code")
        category = item.get("category")

        # Skip if no bulk_code defined
        if not bulk_code:
            continue

        products = [p.strip() for p in product_str.split(",") if p.strip()]

        for prod in products:
            bulk_cfg[prod] = {
                "bulk_code": bulk_code,
                "category": category
            }

    return {
        "bulk_job_config": bulk_cfg,
        "defaults": {
            "bulk_code": "b"
        }
    }

def group_onprem_orders(records):
    """
    Group on-prem orders by (address + shippingid) and limit each group
    to ≤ 11 unique insert codes.
    """

    addr_ship_groups = defaultdict(list)
    grouped_orders = {}
    inserts_at_shipping = {}
    shipping_id_sequence = {}

    bulk_unique_ids = set()
    parsed_rows = []

    # ---------------- SORT RECORDS (unchanged) ----------------
    sorted_records = sorted(
        records,
        key=lambda x: (
            ns(x[4]),   # first_name
            ns(x[5]),   # last_name
            ns(x[6]),   # name
            ns(x[7]),   # address_line1
            ns(x[8]),   # address_line2
            ns(x[9]),   # city
            ns(x[10]),  # province_country
            ns(x[11]),  # country
            ns(x[12])   # zip_code
        )
    )

    # ==========================================================
    # FIRST PASS – build STABLE bulk_unique_id
    # ==========================================================
    for row in sorted_records:
        order_id = str(row[0])
        orderdata = row[3]
        contact_fields = row[4:13]
        job_name = row[13]
        address = format_address(contact_fields)

        try:
            orderdata_dict = orderdata if isinstance(orderdata, dict) else json.loads(orderdata)
        except Exception as e:
            log_event("WARN", f"Bad JSON in order {order_id}: {e}")
            orderdata_dict = {}

        shippingid = get_shippingid(orderdata_dict)
        insert_codes = get_insertSet(orderdata_dict)
        insert_key = ",".join(sorted(insert_codes))
        physical_insert_id = (get_physicalInsertItemId(orderdata_dict) or "NA").strip()
        bulk = get_bulk_type(orderdata_dict, job_name, order_id)

        if bulk == "c":
            # For .c type, force uniqueness per order
            bulk_unique_id = (
                f"{address}_"
                f"{shippingid}_"
                f"{bulk}_"
                f"{job_name}_"
                f"{insert_key}_"
                f"{physical_insert_id}_"
                f"{order_id}"
            )
        else:
            bulk_unique_id = (
                f"{address}_"
                f"{shippingid}_"
                f"{bulk}_"
                f"{job_name}_"
                f"{insert_key}_"
                f"{physical_insert_id}"
            )

        parsed_rows.append((row, bulk_unique_id))
        bulk_unique_ids.add(bulk_unique_id)

    # ==========================================================
    # deterministic bulk_id assignment
    # ==========================================================
    bulk_id_list = {
        buid: idx
        for idx, buid in enumerate(sorted(bulk_unique_ids), start=1)
    }

    # ==========================================================
    # SECOND PASS
    # ==========================================================
    for row, bulk_unique_id in parsed_rows:
        order_id = str(row[0])
        orderdata = row[3]
        contact_fields = row[4:13]
        job_name = row[13]
        address = format_address(contact_fields)

        try:
            orderdata_dict = orderdata if isinstance(orderdata, dict) else json.loads(orderdata)
        except Exception:
            orderdata_dict = {}

        shippingid = get_shippingid(orderdata_dict)
        insert_codes = get_insertSet(orderdata_dict)
        bulk = get_bulk_type(orderdata_dict, job_name, order_id)

        bulk_id = f"{bulk_id_list[bulk_unique_id]:010}"

        unique_id = f"{shippingid}_{bulk}_{job_name}"

        # ---------- sequence logic ----------
        if bulk == "e":
            combined = inserts_at_shipping.get(unique_id, {}).get("insert_codes", set()).union(insert_codes)
            if len(combined) > 11:
                shipping_id_sequence[unique_id] = shipping_id_sequence.get(unique_id, 1) + 1
        else:
            combined = inserts_at_shipping.get(unique_id, {}).get("insert_codes", set()).union(insert_codes)
            if len(combined) > 11:
                shipping_id_sequence[unique_id] = shipping_id_sequence.get(unique_id, 1) + 1

        if unique_id not in shipping_id_sequence:
            shipping_id_sequence[unique_id] = 1

        # ---------- update insert tracking ----------
        inserts_at_shipping.setdefault(unique_id, {"insert_codes": set()})
        inserts_at_shipping[unique_id]["insert_codes"].update(insert_codes)

        seq = shipping_id_sequence[unique_id]

        # ---------- reuse logic   ----------
        matched_parent_id = None

        if bulk == "e":
            for pid, grp in grouped_orders.items():
                if (
                    grp["address"] == address and
                    grp["shippingid"] == shippingid and
                    grp["sequence"] == seq
                ):
                    matched_parent_id = pid
                    break

        # ---------- create or reuse group ----------
        if matched_parent_id:
            grouped_orders[matched_parent_id]["orders"].append(order_id)
            parent_id = matched_parent_id
        else:
            parent_id = order_id
            grouped_orders[parent_id] = {
                "job_name": job_name,
                "address": address,
                "shippingid": shippingid,
                "insert_codes": list(insert_codes),
                "sequence": seq,
                "bulk": bulk,
                "bulk_id": bulk_id,
                "orders": [order_id],
            }

        addr_ship_groups[(unique_id, seq)].append(parent_id)

    for grp in grouped_orders.values():
        if isinstance(grp.get("insert_codes"), set):
            grp["insert_codes"] = list(grp["insert_codes"])

    return grouped_orders


def safe_commit(connection):
    if not connection:
        return
    try:
        connection.commit()
    except Exception as e:
        log_event("WARN", f"Commit failed or skipped: {e}")


def write_output_file(data, bucket_name, key_prefix, output_file):

    # Skip empty data
    if not data:
        log_event("WARN", f"No data data to write for s3://{bucket_name}/{key_prefix}/{output_file}. Skipping upload.")
        return

    # Try convert to JSON
    try:
        if isinstance(data, list):
            body = json.dumps({"order_ids": data, "count": len(data)}, indent=2)
        else:
            body = json.dumps(data, indent=2)
    except (TypeError, ValueError) as e:
        log_event("WARN", f"Failed to convert data to JSON for {bucket_name}/{key_prefix}/{output_file}: {e}. Skipping upload.")
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
        #log_event("INFO", f"Successfully uploaded data file to s3://{bucket_name}/{key}")
    except Exception as e:
        log_event("WARN", f"Failed to write data data file to s3://{bucket_name}/{key}::Exception: {e}")


def set_job_params_as_env_vars():
    messages = []
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i+1]
            os.environ[key] = value
            messages.append(f"{key}={value}")

    log_event("INFO", f"Env vars set from job params: {', '.join(messages)}")


# ------------------ Main ------------------
def main():

    connection = None
    cursor = None
    db_pool = None

    try:
        set_job_params_as_env_vars()

        job_start_time_utc = datetime.now(timezone.utc)

        # === INPUT PARAMS ===
        ssmDBKey = os.getenv("ssmDBKey")
        input_bucket = os.getenv("bucket")
        input_key = os.getenv("objectKey")
        output_bucket = os.getenv("outputBucket")
        output_key_prefix = os.getenv("outputObjectKey")
        destination_type = os.getenv("destinationType", "unknown")
        client_name = os.getenv("clientName")
        global cronConfig
        cronConfig = json.loads(os.getenv("sf_cronConfig"))
        print(f"cronConfig: {json.dumps(cronConfig)}")

        global BULK_JOB_CONFIG
        BULK_JOB_CONFIG = build_bulk_job_config_from_cron(cronConfig)
        print(f"BULK_JOB_CONFIG: {json.dumps(BULK_JOB_CONFIG)}")

        global CORP_MAPPING
        CORP_MAPPING = build_corp_mapping_from_cron(cronConfig)
        print(f"CORP_MAPPING: {json.dumps(CORP_MAPPING)}")

        try:
            max_orderids = int(os.getenv("max_orderids", 10000))
        except Exception:
            max_orderids = 10000

        if max_orderids < 1:
            max_orderids = 10000

        log_event("STARTED", f"glue_group_orders job started with Destination : {destination_type}, max_orderids: {max_orderids}, input_s3: s3://{input_bucket}/{input_key}, output_s3: s3://{output_bucket}/{output_key_prefix}, BULK_JOB_CONFIG: {json.dumps(BULK_JOB_CONFIG)}")

        if not client_name:
            raise ValueError("Environment variable clientName is required.")

        # === LOAD JOB IDS FROM S3 ===
        job_ids = get_job_ids_from_s3(input_bucket, input_key)
        if not job_ids:
            raise ValueError("job_ids is empty; nothing to process")

        db_credentials = get_db_credentials(ssmDBKey)  
        db_pool = initialize_db_pool(db_credentials)

        # Establish connection
        connection = get_db_connection(db_pool)
        connection.autocommit = False
        cursor = connection.cursor()

        job_name_str = job_id.replace(",", "_").replace(" ", "")
        job_name_params = ','.join(['%s'] * len(job_ids))

        # === FETCH ALL ORDER DATA FOR JOB IDS ===
        query = f"""
            SELECT o.orderid, o.correlation_id, o.traceid, o.orderdata,
                c.first_name, c.last_name, c.name,
                c.address_line1, c.address_line2,
                c.city, c.province_country, c.country, c.zip_code,
                o.job_name, o.client, o.status, o.recieved_time
            FROM order_table o
            LEFT JOIN contact_details c ON o.orderid = c.orderid
            WHERE o.client = %s
            AND o.job_name IN ({job_name_params})
            AND o.status = 'COMPLETED'
            ORDER BY o.recieved_time ASC, o.orderid ASC
            LIMIT %s
        """
        cursor.execute(query, [client_name] + job_ids + [max_orderids])
        records = cursor.fetchall()
        log_event("INFO", f"Fetched {len(records)} records from DB (max limit={max_orderids}), for client={client_name}, job_ids={job_ids}")

        if not records:
            log_event("ENDED", "glue_group_orders job ended - no orders to process.")
            return

        # === STEP: BULK GET VALID ORDERS FROM retrieve_table ===
        all_order_ids = [row[0] for row in records]
        orders_ids_param = ','.join(['%s'] * len(all_order_ids))
        cursor.execute(
            f"SELECT orderid FROM retrieve_table "
            f"WHERE orderid IN ({orders_ids_param}) AND s3location IS NOT NULL", 
            all_order_ids
        )

        valid_order_ids = {row[0] for row in cursor.fetchall()}
        # === Mark loaded orders as ACCEPTED ===
        accepted_orders = [
            {
                "order_id": row[0],
                "correlation_id": row[1],
                "trace_id": row[2],
                "job_name": row[13],
                "client": row[14],
            }
            for row in records
        ]

        if accepted_orders:
            accepted_order_ids = [o["order_id"] for o in accepted_orders]
            accepted_orders_ids_param = ','.join(['%s'] * len(accepted_order_ids))
            cursor.execute(
                f"UPDATE order_table SET status='ACCEPTED' "
                f"WHERE orderid IN ({accepted_orders_ids_param})",
                accepted_order_ids,
            )

        """
            for accepted_order in accepted_orders:
                event_data = {
                    "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "event_type": "ORDER_STATUS_CHANGE",
                    "order_id": accepted_order.get("order_id", " "),
                    "previous_status": "COMPLETED",
                    "new_status": "ACCEPTED",
                    "client": accepted_order.get("client", " "),
                    "correlation_id": accepted_order.get("correlation_id", " "),
                    "trace_id": accepted_order.get("trace_id", " "),
                    "job_id": accepted_order.get("job_name", " "),
                }
                send_splunk_event_async(event_data)
        """

        # === process invalid orders ===
        invalid_orders = [
            {
                "order_id": row[0],
                "correlation_id": row[1],
                "trace_id": row[2],
                "job_name": row[13],
                "client": row[14],
            }
            for row in records
            if row[0] not in valid_order_ids
        ]

        # === BATCH UPDATE INVALID ORDERS ===
        if invalid_orders:
            invalid_order_ids = [o["order_id"] for o in invalid_orders]
            invalid_orders_ids_param = ','.join(['%s'] * len(invalid_order_ids))
            cursor.execute(
                f"UPDATE order_table SET status='S3 NOT FOUND' "
                f"WHERE orderid IN ({invalid_orders_ids_param})",
                invalid_order_ids,
            )

            """
            for invalid_order in invalid_orders:
                event_data = {
                    "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "event_type": "ORDER_STATUS_CHANGE",
                    "order_id": invalid_order.get("order_id", " "),
                    "previous_status": "COMPLETED",
                    "new_status": "NO-PRINT-DOCS",
                    "client": invalid_order.get("client", " "),
                    "correlation_id": invalid_order.get("correlation_id", " "),
                    "trace_id": invalid_order.get("trace_id", " "),
                    "job_id": invalid_order.get("job_name", " "),
                }
                send_splunk_event_async(event_data)
            """

        log_event("INFO", f"Total orders received: {len(all_order_ids)}, accepted: {len(accepted_orders)}, valid: {len(valid_order_ids)}, invalid: {len(invalid_orders)}", all_order_ids)

        valid_records = [row for row in records if row[0] in valid_order_ids]
        if not valid_records:
            log_event("INFO", "No valid records to process after filtering.")

        else:
            # === FILE CREATION & UPLOAD TO S3 ===
            uploaded_files = []
            if destination_type.lower() in ("on-prem", "onprem"):
                log_event("INFO", f"Processing onprem orders, destination_type={destination_type}")
                record_lookup = {str(r[0]): r for r in valid_records}
                grouped_orders = group_onprem_orders(valid_records)

                ts = datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")
                prefix = f"{output_key_prefix}".replace("input", "save")
                json_file_name = f"PFG_grouped_onprem_orders_{ts}_{job_name_str}.json"
                write_output_file(grouped_orders, output_bucket, prefix, json_file_name)

                uploaded_files = []
                final_grouped_orders = {}
                order_to_parent = []

                for parent_order, g in grouped_orders.items():
                    orders = g["orders"]
                    job_name = g["job_name"]
                    bulk_flag = g["bulk"]
                    seq = g["sequence"]
                    shipping_id = g["shippingid"]
                    bulk_id = g["bulk_id"]

                    # store mapping for later bulk update
                    for oid in orders:
                        order_to_parent.append((oid, parent_order,bulk_id))

                    key_string = job_name + "_" + bulk_flag + "_" + shipping_id + "_" + str(seq)

                    if key_string in final_grouped_orders:
                        final_grouped_orders[key_string]["orders"].extend(g["orders"])
                    else:
                        final_grouped_orders[key_string]={
                            "orders" : g["orders"],
                            "job_name" : g["job_name"],
                            "bulk" : g["bulk"],
                            "sequence" : g["sequence"]
                        }

                log_event("INFO", f"Updating parent_order for {len(order_to_parent)} orders using executemany()")
                updates = [(parent, bulk_id, oid) for (oid, parent, bulk_id) in order_to_parent]

                CHUNK = 10000
                for i in range(0, len(updates), CHUNK):
                    chunk = updates[i:i + CHUNK]
                    cursor.executemany(
                        "UPDATE order_table SET parent_order = %s,bulk_id = %s WHERE orderid = %s",
                        chunk
                    )

                updates = [(parent, oid) for (oid, parent, bulk_id) in order_to_parent]
               
                CHUNK = 10000
                for i in range(0, len(updates), CHUNK):
                    chunk = updates[i:i + CHUNK]
                    cursor.executemany(
                        "UPDATE retrieve_table SET parent_order = %s WHERE orderid = %s",
                        chunk
                    )

                log_event("INFO", f"Updated parent_order for {len(updates)} orders via executemany()")

                json_file_name = f"PFG_final_grouped_onprem_orders_{ts}_{job_name_str}.json"
                write_output_file(final_grouped_orders, output_bucket, prefix, json_file_name)

                for parent_order, g in final_grouped_orders.items():
                    orders = g["orders"]
                    bulk_flag = g["bulk"]
                    seq = g["sequence"]

                    original_job_name = g["job_name"]
                    # Resolve corp-based job name (if configured)
                    job_name = resolve_job_name(original_job_name, bulk_flag)

                    timestamp = datetime.now().strftime("%m%d%Y%H%M%S%f")[:-3]
                    counter = len(uploaded_files) + 1
                    #filename = f"PFG_{job_name}.{bulk_flag}.{int(seq)}_RETRIEVE_AND_ZIP_{timestamp}.{counter}.trig"
                    filename = f"PFG_{job_name}.{bulk_flag}_RETRIEVE_AND_ZIP_{timestamp}.{counter}.trig"

                    local_path = f"/tmp/{filename}"
                    s3_key = f"{output_key_prefix}/{filename}".replace("//", "/")

                    with open(local_path, "w") as file:
                        for oid in orders:
                            row = record_lookup.get(str(oid))
                            if not row:
                                continue
                            order_id, correlation_id, traceid, orderdata = row[0:4]
                            job, client, status = row[13:16]
                            try:
                                orderdata_json = (
                                    json.dumps(orderdata)
                                    if isinstance(orderdata, dict)
                                    else str(orderdata)
                                )
                            except Exception:
                                orderdata_json = str(orderdata)
                            file.write(f"{order_id}|{correlation_id}|{traceid}|{orderdata_json}|{job}|{client}|{status}\n")

                    s3.upload_file(local_path, output_bucket, s3_key)
                    os.remove(local_path)
                    uploaded_files.append(f"s3://{output_bucket}/{s3_key}")

            else:
                log_event("INFO", f"Processing others orders, destination_type={destination_type}")
                grouped_orders = defaultdict(list)
                for row in valid_records:
                    order_id, correlation_id, traceid, orderdata = row[0:4]
                    contact_fields = row[4:13]
                    job_name, client, status = row[13:16]
                    address = format_address(contact_fields)
                    grouped_orders[address].append((order_id, correlation_id, traceid, orderdata, job_name, client, status))

                bulk_orders = {k: v for k, v in grouped_orders.items() if str(k) == "BULK"}

                non_bulk_orders = {k: v for k, v in grouped_orders.items() if str(k) != "BULK"}

                log_event("INFO", f"Total bulk_orders={len(bulk_orders)}, non_bulk_orders={len(non_bulk_orders)}, destination_type={destination_type}")

                if bulk_orders:
                    uploaded_files.extend(process_orders(bulk_orders, output_bucket, output_key_prefix, "b"))
                if non_bulk_orders:
                    uploaded_files.extend(process_orders(non_bulk_orders, output_bucket, output_key_prefix, "n"))

            if uploaded_files:
                log_event("INFO", f"Uploaded {len(uploaded_files)} trigger files: {uploaded_files}")
            else:
                log_event("INFO", "Uploaded 0 trigger files.")


        safe_commit(connection)

        cursor.execute(
            f"SELECT COUNT(*) FROM order_table "
            f"WHERE client=%s AND status='COMPLETED' "
            f"AND job_name IN ({job_name_params}) "
            f"AND recieved_time < %s",
            [client_name] + job_ids + [job_start_time_utc]
        )

        remaining_count = cursor.fetchone()[0]
        log_event("INFO", f"Remaining order cutoff time (UTC): {job_start_time_utc.isoformat()}, count of orders still in COMPLETED status for client={client_name}, job_ids={job_ids}: {remaining_count}")
        if remaining_count > 0:
            timestamp = datetime.now().strftime("%m%d%Y%H%M%S%f")[:17]
            new_key = input_key.rsplit('/', 1)[0] + "/" + f"PFG_ORDER_PROCESSING_{timestamp}.trig"

            # Duplicate the original input file with new name
            copy_source = {'Bucket': input_bucket, 'Key': input_key}
            s3.copy_object(
                Bucket=input_bucket,
                CopySource=copy_source,
                Key=new_key
            )
            log_event("INFO", f"{remaining_count} COMPLETED orders for job_ids={job_ids}, still exist; created new trigger file s3://{input_bucket}/{new_key} to process them in next run.")



        log_event("ENDED", "glue_group_orders job completed successfully.", all_order_ids)

    except Exception as e:
        log_event("FAILED", f"Error: {str(e)}\n{traceback.format_exc()}")
        raise e

    finally:
        if cursor is not None:
            cursor.close()

        if connection is not None:
            release_db_connection(connection, db_pool)
    
        SPLUNK_EXECUTOR.shutdown(wait=True)

if __name__ == '__main__':
    main()