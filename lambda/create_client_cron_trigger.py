import json
import os
import re
import time
import traceback
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import boto3
from psycopg2 import pool
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from dpm_splunk_logger_py import splunk

# ----------------------------------------------------------------------
# Environment
# ----------------------------------------------------------------------
REGION = os.environ["REGION"]
CONFIG_TABLE_DYNAMO = os.environ["CONFIG_TABLE_DYNAMO"]
S3_DATAINGRESS = os.environ["S3_DATAINGRESS"]
DBKEY = os.environ["DBKEY"]

config = Config(connect_timeout=2, read_timeout=10)
ssm = boto3.client("ssm", region_name=REGION)
secrets = boto3.client("secretsmanager")
s3 = boto3.client("s3", config=config)
ses = boto3.client("ses", region_name="us-east-1")
dynamo = boto3.resource("dynamodb")
sts = boto3.client("sts")
AWS_ACCOUNT_ID = sts.get_caller_identity()["Account"]

DAYS = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
lambda_start = time.time()
processed_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ----------------------------------------------------------------------
# Logging and alerts
# ----------------------------------------------------------------------
def is_valid_email(addr: str) -> bool:
    return bool(addr and re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", addr))


def log_event(status, message, context, **extra):
    elapsed = round(time.time() - lambda_start, 2)
    event_data = {
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "event_type": "MASTER_CLIENT_CRON_TRIGGER",
        "component": "lambda",
        "function_name": context.function_name,
        "aws_account_id": AWS_ACCOUNT_ID,
        "region": REGION,
        "status": status,
        "message": message,
        "duration_sec": elapsed,
        "processed_date": processed_date,
        **extra
    }

    print(json.dumps(event_data))
    try:
        splunk.log_message(event_data, context)
    except Exception as e:
        print(f"[WARN] Splunk logging failed: {e}")

    if status == "FAILED":
        alert_to = os.getenv("ALERT_EMAIL", "")
        if is_valid_email(alert_to):
            try:
                link = (
                    f"https://{REGION}.console.aws.amazon.com/lambda/home?"
                    f"region={REGION}#/functions/{context.function_name}"
                )
                ses.send_email(
                    Source=os.getenv("ALERT_SENDER_EMAIL", "noreply@example.com"),
                    Destination={"ToAddresses": [alert_to]},
                    Message={
                        "Subject": {"Data": f"[ALERT] {context.function_name} failure"},
                        "Body": {"Text": {"Data": f"{message}\n{link}"}},
                    },
                )
            except ClientError as e:
                print(f"[WARN] SES send failed: {e}")


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def normalize_time(tstr):
    m = re.match(r"^\s*(\d{1,2})(:?(\d{0,2}))?\s*(AM|PM|am|pm)?\s*$", tstr)
    if not m:
        raise ValueError(f"Invalid time: {tstr}")
    hour = int(m.group(1))
    minute = int(m.group(3) or 0)
    ap = (m.group(4) or "").lower()
    if ap == "pm" and hour < 12:
        hour += 12
    if ap == "am" and hour == 12:
        hour = 0
    bucket_min = 0 if minute < 30 else 30
    return f"{hour:02d}:{bucket_min:02d}"


def get_db_credentials(context):
    param = ssm.get_parameter(Name=DBKEY, WithDecryption=True)
    secret_id = param["Parameter"]["Value"]
    resp = secrets.get_secret_value(SecretId=secret_id)
    return json.loads(resp["SecretString"])


def init_pool(creds, db):
    return pool.SimpleConnectionPool(
        1, 10,
        host=creds["host"],
        port=creds["port"],
        dbname=db,
        user=creds["username"],
        password=creds["password"]
    )


def query_jobs(conn_pool, table, product_list, status, context):
    conn = cur = None
    try:
        conn = conn_pool.getconn()
        cur = conn.cursor()
        cur.execute("SET TIME ZONE 'EST'")
        sql = f"SELECT DISTINCT job_name FROM {table} WHERE job_name IN ({product_list}) AND status=%s"
        cur.execute(sql, [status])
        rows = cur.fetchall()
        jobs = [r[0] for r in rows if r and r[0]]
        #log_event("INFO", f"Fetched {(jobs)} job names from {table} for products ({product_list}) with status {status}", context)
        return jobs
    except Exception as e:
        if conn:
            conn.rollback()
        raise Exception(f"DB fetch error: {e}") from e
    finally:
        if cur:
            cur.close()
        if conn:
            conn_pool.putconn(conn)


def write_trigger(bucket, key, job_ids, context, **meta):
    if not job_ids:
        log_event("INFO", f"No jobIds for {key}", context, **meta)
        return False
    body = ",".join(job_ids).encode()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="text/plain",
        ContentDisposition=f'attachment; filename="{os.path.basename(key)}"'
    )
    log_event("INFO", f"Trigger file created {bucket}/{key}", context, **meta)
    return True


# ----------------------------------------------------------------------
# Core logic
# ----------------------------------------------------------------------
def process_group(client, cfg, grp, conn_pool, context, sched_meta, jobs_already_triggered_in_schedule):
    tz = ZoneInfo(sched_meta["schedule_timezone"])
    today = DAYS[datetime.now(tz).weekday()]
    valid_days = [d.upper() for d in cfg.get("days_of_week", ["ALL"])]
    triggered_in_group = False

    log_event("INFO", f"Group start {grp['name']} for {client}, job_list: {grp.get('job_list',[])}, job_patterns: {grp.get('job_patterns',[])}", context, **sched_meta)

    if "ALL" not in valid_days and today not in valid_days:
        log_event("INFO", f"Skip group {grp['name']}, job_list: {grp.get('job_list',[])}, job_patterns: {grp.get('job_patterns',[])} today is {today}, valid_days: {valid_days}", context, **sched_meta)
        return False

    # Process exact job_list items
    for job_str in grp.get("job_list", []):
        product_sql = ", ".join(f"'{p.strip()}'" for p in job_str.split(",") if p.strip())
        job_ids = query_jobs(conn_pool, cfg["table_name"], product_sql, cfg["status"], context)
        log_event("INFO", f"Group {grp['name']} for {client}, product_sql: {product_sql}, fetched job_ids: {job_ids}", context, **sched_meta)

        if not job_ids:
            continue

        # Filter out already triggered jobs
        new_jobs = [j for j in job_ids if j not in jobs_already_triggered_in_schedule]
        if not new_jobs:
            log_event("INFO", f"Group {grp['name']}, job_str: {job_str}, all jobs already triggered in this schedule", context, **sched_meta)
            continue
        
        # Create trigger file for this job_list item
        ts = datetime.now().strftime(cfg["timestamp_format"])
        if cfg.get("timestamp_length"):
            ts = ts[: cfg["timestamp_length"]]
        trig_filename = cfg["trig_file_template"].replace("<TIMESTAMP>", ts)
        s3_key = f"{cfg['s3_dest']}/{trig_filename}"

        if write_trigger(S3_DATAINGRESS, s3_key, new_jobs, context, client=client, group=grp["name"]):
            triggered_in_group = True
            jobs_already_triggered_in_schedule.update(new_jobs)

    # Process regex job_patterns items
    for pattern in grp.get("job_patterns", []):
        try:
            job_ids = query_jobs_with_patterns(conn_pool, cfg["table_name"], pattern, cfg["status"], context)
            log_event("INFO", f"Group {grp['name']} for {client}, pattern: {pattern}, fetched job_ids: {job_ids}", context, **sched_meta)
            
            if not job_ids:
                continue
            
            # Filter out already triggered jobs in this schedule
            new_jobs = [j for j in job_ids if j not in jobs_already_triggered_in_schedule]
            if not new_jobs:
                log_event("INFO", f"Group {grp['name']}, pattern: {pattern}, all jobs already triggered in this schedule", context, **sched_meta)
                continue
            
            # Create trigger file for this pattern item
            ts = datetime.now().strftime(cfg["timestamp_format"])
            if cfg.get("timestamp_length"):
                ts = ts[: cfg["timestamp_length"]]
            trig_filename = cfg["trig_file_template"].replace("<TIMESTAMP>", ts)
            s3_key = f"{cfg['s3_dest']}/{trig_filename}"
            
            if write_trigger(S3_DATAINGRESS, s3_key, new_jobs, context, client=client, group=grp["name"], pattern=pattern):
                triggered_in_group = True
                jobs_already_triggered_in_schedule.update(new_jobs)
        
        except re.error as e:
            log_event("WARN", f"Invalid regex pattern '{pattern}': {e}", context, **sched_meta)

    log_event("INFO", f"Group {grp['name']} complete", context, **sched_meta)
    return triggered_in_group


def query_jobs_with_patterns(conn_pool, table, pattern, status, context):
    """
    Fetch jobs matching a single regex pattern
    Returns exact job names that match the pattern
    """
    conn = cur = None
    try:
        conn = conn_pool.getconn()
        cur = conn.cursor()
        cur.execute("SET TIME ZONE 'EST'")
        
        # Fetch all jobs with the given status
        sql = f"SELECT DISTINCT job_name FROM {table} WHERE status=%s"
        cur.execute(sql, [status])
        all_rows = cur.fetchall()
        all_jobs = [r[0] for r in all_rows if r and r[0]]
        
        # Match regex pattern
        try:
            regex = re.compile(pattern)
            matched_jobs = [j for j in all_jobs if regex.match(j)]
            return matched_jobs
        except re.error as e:
            raise ValueError(f"Invalid regex pattern '{pattern}': {e}") from e
    
    except Exception as e:
        if conn:
            conn.rollback()
        raise Exception(f"DB fetch error: {e}") from e
    finally:
        if cur:
            cur.close()
        if conn:
            conn_pool.putconn(conn)

def run_schedule(client, cfg, sched, conn_pool, context):
    raw_time = sched["time"]

    # Support both string and list
    if isinstance(raw_time, list):
        sched_times = [normalize_time(t) for t in raw_time]
    else:
        sched_times = [normalize_time(raw_time)]

    timezone = sched.get("timezone", "America/New_York")
    tz = ZoneInfo(timezone)
    now_local = datetime.now(tz)
    bucket = f"{now_local.hour:02d}:{0 if now_local.minute < 30 else 30:02d}"

    sched_meta = {
        "client": client,
        "schedule_time": sched_times,
        "schedule_timezone": timezone,
    }

    # Only run if current time matches any configured time
    if bucket not in sched_times:
        log_event("INFO", f"{client}=skip::sched_times={sched_times}::current={bucket}", context, **sched_meta)
        return
    else:
        log_event("INFO", f"{client}=run::sched_times={sched_times}::current={bucket}", context, **sched_meta)


    # Track jobs across ALL groups in this schedule to prevent duplicates
    jobs_already_triggered_in_schedule = set()

    triggered_this_schedule = False
    groups = sched.get("groups", [])

    for idx, grp in enumerate(groups):
        log_event("INFO", f"Processing group {grp['name']}, for client {client}, job_list: {grp.get('job_list',[])}, job_patterns: {grp.get('job_patterns',[])}", context, **sched_meta)

        if process_group(client, cfg, grp, conn_pool, context, sched_meta, jobs_already_triggered_in_schedule):
            triggered_this_schedule = True

            # Wait only if we actually created a trigger and it isn’t the last group
            if idx < len(groups) - 1:
                log_event("INFO", "Waiting 2 minutes before next group", context, **sched_meta)
                time.sleep(120)

    if triggered_this_schedule:
        log_event("INFO", f"{client}: schedule {sched_times} completed with trigger(s)", context, **sched_meta)
    else:
        log_event("INFO", f"{client}: schedule {sched_times} completed (no triggers)", context, **sched_meta)


def process_client(client, cfg, conn_pool, context):
    log_event("INFO", f"Start client {client}", context)
    for sched in cfg.get("schedules", []):
        try:
            log_event("INFO", f"client:{client}::sched:{sched}", context)
            run_schedule(client, cfg, sched, conn_pool, context)
        except Exception as e:
            raise Exception(f"Client {client}, sched:{sched}, error: {e}") from e
    log_event("INFO", f"Client {client} done", context)


# ----------------------------------------------------------------------
# Lambda handler
# ----------------------------------------------------------------------
def lambda_handler(event, context):
    try:
        log_event("INFO", "Lambda start", context)

        tbl = dynamo.Table(CONFIG_TABLE_DYNAMO)
        resp = tbl.query(KeyConditionExpression=Key("clientName").eq("master_cron_config"))
        if not resp["Items"]:
            raise RuntimeError("master_cron_config not found")

        cfg_raw = resp["Items"][0]["clientConfig"]
        master = json.loads(cfg_raw) if isinstance(cfg_raw, str) else cfg_raw
        clients = master.get("clients", {})
        if not clients:
            raise RuntimeError("No clients configured")

        creds = get_db_credentials(context)

        for cname, ccfg in clients.items():
            log_event("INFO", f"Processing client:{cname}, config:{ccfg}", context)
            conn_pool = init_pool(creds, ccfg["db_name"])
            process_client(cname, ccfg, conn_pool, context)

        log_event("ENDED", "Cron master completed successfully", context)
        return {"statusCode": 200, "body": "Cron master completed"}

    except Exception as e:
        log_event("FAILED", f"Unhandled error: {e}\n{traceback.format_exc()}", context)
        raise