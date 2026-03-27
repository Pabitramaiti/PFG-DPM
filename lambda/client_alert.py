import json
import os
import re
import time
import traceback
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import boto3
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from dpm_splunk_logger_py import splunk

# ----------------------------------------------------------------------
# Environment / AWS clients
# ----------------------------------------------------------------------
CONFIG_TABLE_DYNAMO = os.environ["CONFIG_TABLE_DYNAMO"]

session = boto3.session.Session()
REGION = session.region_name or os.environ.get("AWS_REGION", "us-east-1")

config = Config(connect_timeout=2, read_timeout=10)
dynamo = boto3.resource("dynamodb", region_name=REGION)
secrets = boto3.client("secretsmanager", region_name=REGION)
ses = boto3.client("ses", region_name=REGION)
#ses = boto3.client("ses", region_name="us-east-1")
sts = boto3.client("sts", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)

AWS_ACCOUNT_ID = sts.get_caller_identity()["Account"]
lambda_start = time.time()
processed_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ----------------------------------------------------------------------
# Logging / alerts
# ----------------------------------------------------------------------
def is_valid_email(addr: str) -> bool:
    return bool(addr and re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", addr))


def log_event(status, message, context, **extra):
    elapsed = round(time.time() - lambda_start, 2)
    event_data = {
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "event_type": "MONITORING_CRON",
        "component": "lambda",
        "function_name": context.function_name,
        "aws_account_id": AWS_ACCOUNT_ID,
        "region": REGION,
        "status": status,
        "message": message,
        "duration_sec": elapsed,
        "processed_date": processed_date,
        **extra,
    }

    print(json.dumps(event_data, default=str))
    try:
        splunk.log_message(event_data, context)
    except Exception as e:
        print(f"[WARN] Splunk logging failed: {e}")


def build_lambda_link(function_name: str) -> str:
    return (
        f"https://{REGION}.console.aws.amazon.com/lambda/home?"
        f"region={REGION}#/functions/{function_name}"
    )


def send_email_alert(client_name, alert_cfg, subject, body, context):
    if not alert_cfg or not alert_cfg.get("email_enabled", False):
        return

    sender = alert_cfg.get("from")
    recipients = alert_cfg.get("email_to", [])

    if not sender or not recipients:
        log_event(
            "INFO",
            f"Email alert skipped due to missing sender or recipients for client {client_name}",
            context,
            client=client_name,
        )
        return

    valid_recipients = [addr for addr in recipients if is_valid_email(addr)]
    if not valid_recipients:
        print(f"[WARN] No valid email recipients for client {client_name}")
        log_event(
            "INFO", f"Email alert skipped due to no valid recipients for client {client_name}", context, client=client_name
        )
        return

    log_event(
        "INFO", f"Sending email alert for client {client_name} to {valid_recipients}", context, client=client_name, subject=subject
    )
    try:
        response = ses.send_email(
            Source=sender,
            Destination={"ToAddresses": valid_recipients},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Text": {"Data": body}},
            },
        )
        print("SES send_email full response:\n", json.dumps(response, indent=2))
    except ClientError as e:
        raise Exception(f"Failed to send email alert for client {client_name}: {e}") from e

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def get_db_credentials(dbkey):
    param = ssm.get_parameter(Name=dbkey, WithDecryption=True)
    secret_id = param["Parameter"]["Value"]
    resp = secrets.get_secret_value(SecretId=secret_id)
    return json.loads(resp["SecretString"])


def init_pool(creds, db):
    return pool.SimpleConnectionPool(
        1,
        5,
        host=creds["host"],
        port=creds["port"],
        dbname=db,
        user=creds["username"],
        password=creds["password"],
    )


def get_s3_client(region_name):
    return boto3.client("s3", region_name=region_name, config=config)


def get_time_window_bounds(tz_name, time_window):
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz)

    window_type = (time_window or {}).get("type", "today")

    if window_type == "today":
        start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        end_local = now_local
    elif window_type == "last_n_hours":
        value = int((time_window or {}).get("value", 24))
        start_local = now_local - timedelta(hours=value)
        end_local = now_local
    else:
        raise ValueError(f"Unsupported time_window.type: {window_type}")

    return {
        "timezone": tz_name,
        "window_type": window_type,
        "window_start": start_local.strftime("%Y-%m-%d %H:%M:%S"),
        "window_end": end_local.strftime("%Y-%m-%d %H:%M:%S"),
        "today_date": now_local.strftime("%Y-%m-%d"),
        "current_timestamp": now_local.strftime("%Y-%m-%d %H:%M:%S"),
    }


def render_query(query, time_tokens):
    rendered = query
    for token_name, token_value in time_tokens.items():
        rendered = rendered.replace(f"{{{{{token_name}}}}}", str(token_value))
    return rendered


def validate_regex_patterns(patterns):
    compiled = []
    for pat in patterns or []:
        compiled.append(re.compile(pat))
    return compiled


def format_s3_file_details(files):
    lines = []
    for f in files:
        lines.append(
            f"key={f['key']}, last_modified={f['last_modified']}, age_minutes={f['age_minutes']}"
        )
    return "\n".join(lines)


def format_db_row_details(rows, display_fields=None):
    lines = []
    for row in rows:
        if display_fields:
            parts = [f"{fld}={row.get(fld)}" for fld in display_fields]
        else:
            parts = [f"{k}={v}" for k, v in row.items()]
        lines.append(", ".join(parts))
    return "\n".join(lines)


# ----------------------------------------------------------------------
# S3 checks
# ----------------------------------------------------------------------
def run_s3_check(client_name, client_cfg, s3_check, context):
    check_name = s3_check.get("name", "unnamed_s3_check")
    if not s3_check.get("enabled", False):
        log_event(
            "INFO",
            f"S3 check skipped (disabled): {check_name}",
            context,
            client=client_name,
            check_name=check_name,
            check_type="S3",
        )
        return

    region_name = s3_check.get("region", REGION)
    bucket = s3_check["bucket"]
    prefixes = s3_check.get("prefixes", [])
    file_patterns = s3_check.get("file_patterns", [])
    older_than_minutes = int(s3_check["older_than_minutes"])
    list_threshold = int(s3_check.get("list_threshold", 20))
    alert_if_no_match = bool(s3_check.get("alert_if_no_match", False))

    s3_client = get_s3_client(region_name)
    compiled_patterns = validate_regex_patterns(file_patterns)

    now_utc = datetime.now(timezone.utc)
    matched_files = []
    stale_files = []


    for prefix in prefixes:
        continuation_token = None

        while True:
            kwargs = {
                "Bucket": bucket,
                "Prefix": prefix,
                "MaxKeys": 1000,
            }
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token

            resp = s3_client.list_objects_v2(**kwargs)

            for obj in resp.get("Contents", []):
                key = obj["Key"]
                filename = key.split("/")[-1]

                if compiled_patterns and not any(p.search(filename) for p in compiled_patterns):
                    continue

                last_modified = obj["LastModified"]
                age_minutes = round((now_utc - last_modified).total_seconds() / 60, 2)

                file_info = {
                    "key": key,
                    "last_modified": last_modified.isoformat(),
                    "age_minutes": age_minutes,
                }

                matched_files.append(file_info)

                if age_minutes > older_than_minutes:
                    stale_files.append(file_info)

            if resp.get("IsTruncated"):
                continuation_token = resp.get("NextContinuationToken")
            else:
                break

    extra = {
        "client": client_name,
        "check_name": check_name,
        "check_type": "S3",
        "bucket": bucket,
        "prefixes": prefixes,
        "file_patterns": file_patterns,
        "older_than_minutes": older_than_minutes,
        "matched_file_count": len(matched_files),
        "stale_file_count": len(stale_files),
        "s3_region": region_name,
    }

    if not matched_files:
        message = f"S3 check found no matching files for client={client_name}, check={check_name}"
        log_event("INFO", message, context, **extra)

        if alert_if_no_match:
            alert_cfg = client_cfg.get("alert", {})
            subject = f"[ALERT] {context.function_name} S3 no matching files - {client_name} - {check_name}"
            body = (
                f"Client: {client_name}\n"
                f"Check: {check_name}\n"
                f"Type: S3\n"
                f"Bucket: {bucket}\n"
                f"Prefixes: {prefixes}\n"
                f"Patterns: {file_patterns}\n"
                f"Result: No matching files found\n\n"
                f"{build_lambda_link(context.function_name)}"
            )
            send_email_alert(client_name, alert_cfg, subject, body, context)
        return

    if not stale_files:
        log_event("INFO", f"No stale files found for client={client_name}, check={check_name}", context, **extra)
        return

    listed_files = stale_files[:list_threshold]
    results_truncated = len(stale_files) > list_threshold

    if results_truncated:
        detail_text = f"Stale file count: {len(stale_files)} (count only, exceeds list_threshold={list_threshold})"
    else:
        detail_text = format_s3_file_details(listed_files)

    log_event(
        "ALERT",
        f"Stale files found for client={client_name}, check={check_name}",
        context,
        listed_file_count=len(listed_files),
        results_truncated=results_truncated,
        stale_files=listed_files if not results_truncated else [],
        **extra,
    )

    alert_cfg = client_cfg.get("alert", {})
    subject = f"[ALERT] {context.function_name} S3 stale files - {client_name} - {check_name}"
    body = (
        f"Client: {client_name}\n"
        f"Check: {check_name}\n"
        f"Type: S3\n"
        f"Bucket: {bucket}\n"
        f"Region: {region_name}\n"
        f"Prefixes: {prefixes}\n"
        f"Patterns: {file_patterns}\n"
        f"Older Than Minutes: {older_than_minutes}\n"
        f"Matched File Count: {len(matched_files)}\n"
        f"Stale File Count: {len(stale_files)}\n\n"
        f"Details:\n{detail_text}\n\n"
        f"{build_lambda_link(context.function_name)}"
    )
    send_email_alert(client_name, alert_cfg, subject, body, context)


# ----------------------------------------------------------------------
# DB checks
# ----------------------------------------------------------------------
def run_db_query(conn_pool, rendered_query):
    conn = cur = None
    try:
        conn = conn_pool.getconn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(rendered_query)
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        if conn:
            conn.rollback()
        raise Exception(f"DB query error: {e}") from e
    finally:
        if cur:
            cur.close()
        if conn:
            conn_pool.putconn(conn)


def run_db_check(client_name, client_cfg, db_check, context):
    check_name = db_check.get("name", "unnamed_db_check")
    if not db_check.get("enabled", False):
        log_event(
            "INFO",
            f"DB check skipped (disabled): {check_name}",
            context,
            client=client_name,
            check_name=check_name,
            check_type="DB",
        )
        return

    db_conn_cfg = client_cfg.get("db_connection", {})
    dbkey = db_conn_cfg.get("dbkey")
    if not dbkey:
        raise ValueError(f"Missing db_connection.dbkey for client {client_name}")

    db_name = db_check["db_name"]
    tz_name = db_check.get("timezone", "UTC")
    time_window = db_check.get("time_window", {"type": "today"})
    raw_query = db_check["query"]
    list_threshold = int(db_check.get("list_threshold", 30))
    display_fields = db_check.get("display_fields")

    time_tokens = get_time_window_bounds(tz_name, time_window)
    rendered_query = render_query(raw_query, time_tokens)

    creds = get_db_credentials(dbkey)
    conn_pool = init_pool(creds, db_name)

    try:
        rows = run_db_query(conn_pool, rendered_query)
    finally:
        try:
            conn_pool.closeall()
        except Exception:
            pass

    row_count = len(rows)

    extra = {
        "client": client_name,
        "check_name": check_name,
        "check_type": "DB",
        "db_name": db_name,
        "timezone": tz_name,
        "time_window": time_window,
        "row_count": row_count,
        "list_threshold": list_threshold
    }

    if row_count == 0:
        log_event("INFO", f"No DB rows found for client={client_name}, check={check_name}", context, **extra)
        return

    listed_rows = rows[:list_threshold]
    results_truncated = row_count > list_threshold

    if results_truncated:
        detail_text = f"Returned row count: {row_count} (count only, exceeds list_threshold={list_threshold})"
    else:
        detail_text = format_db_row_details(listed_rows, display_fields)

    log_event(
        "ALERT",
        f"DB rows found for client={client_name}, check={check_name}",
        context,
        listed_row_count=len(listed_rows),
        results_truncated=results_truncated,
        rows=listed_rows if not results_truncated else [],
        rendered_query=rendered_query,
        **extra,
    )

    alert_cfg = client_cfg.get("alert", {})
    subject = f"[ALERT] {context.function_name} DB rows found - {client_name} - {check_name}"
    body = (
        f"Client: {client_name}\n"
        f"Check: {check_name}\n"
        f"Type: DB\n"
        f"Database: {db_name}\n"
        f"Timezone: {tz_name}\n"
        f"Time Window: {json.dumps(time_window)}\n"
        f"Window Start: {time_tokens.get('window_start')}\n"
        f"Window End: {time_tokens.get('window_end')}\n"
        f"Returned Row Count: {row_count}\n\n"
        f"Rendered Query:\n{rendered_query}\n\n"
        f"Details:\n{detail_text}\n\n"
        f"{build_lambda_link(context.function_name)}"
    )
    send_email_alert(client_name, alert_cfg, subject, body, context)


# ----------------------------------------------------------------------
# Client processing
# ----------------------------------------------------------------------
def process_client(client_name, client_cfg, context):
    if not client_cfg.get("enabled", False):
        log_event("INFO", f"Client skipped (disabled): {client_name}", context, client=client_name)
        return

    log_event("INFO", f"Start client {client_name}", context, client=client_name)

    s3_checks = client_cfg.get("s3_checks", []) or []
    db_checks = client_cfg.get("db_checks", []) or []

    enabled_s3 = [c for c in s3_checks if c.get("enabled", False)]
    enabled_db = [c for c in db_checks if c.get("enabled", False)]

    if not enabled_s3 and not enabled_db:
        log_event("INFO", f"No enabled checks configured for client {client_name}", context, client=client_name)
        return

    for s3_check in s3_checks:
        try:
            run_s3_check(client_name, client_cfg, s3_check, context)
        except Exception as e:
            log_event(
                "FAILED",
                f"S3 check failed for client={client_name}, check={s3_check.get('name')}: {e}",
                context,
                client=client_name,
                check_name=s3_check.get("name"),
                check_type="S3",
                traceback=traceback.format_exc(),
            )
            alert_cfg = client_cfg.get("alert", {})
            subject = f"[ALERT] {context.function_name} S3 check failure - {client_name} - {s3_check.get('name')}"
            body = (
                f"Client: {client_name}\n"
                f"Check: {s3_check.get('name')}\n"
                f"Type: S3\n"
                f"Error:\n{e}\n\n"
                f"{traceback.format_exc()}\n\n"
                f"{build_lambda_link(context.function_name)}"
            )
            send_email_alert(client_name, alert_cfg, subject, body, context)

    for db_check in db_checks:
        try:
            run_db_check(client_name, client_cfg, db_check, context)
        except Exception as e:
            log_event(
                "FAILED",
                f"DB check failed for client={client_name}, check={db_check.get('name')}: {e}",
                context,
                client=client_name,
                check_name=db_check.get("name"),
                check_type="DB",
                traceback=traceback.format_exc(),
            )
            alert_cfg = client_cfg.get("alert", {})
            subject = f"[ALERT] {context.function_name} DB check failure - {client_name} - {db_check.get('name')}"
            body = (
                f"Client: {client_name}\n"
                f"Check: {db_check.get('name')}\n"
                f"Type: DB\n"
                f"Error:\n{e}\n\n"
                f"{traceback.format_exc()}\n\n"
                f"{build_lambda_link(context.function_name)}"
            )
            send_email_alert(client_name, alert_cfg, subject, body, context)

    log_event("INFO", f"Client completed: {client_name}", context, client=client_name)


# ----------------------------------------------------------------------
# Lambda handler
# ----------------------------------------------------------------------
def lambda_handler(event, context):
    try:
        log_event("INFO", "Lambda start", context)

        tbl = dynamo.Table(CONFIG_TABLE_DYNAMO)
        resp = tbl.query(
            KeyConditionExpression=Key("clientName").eq("monitoring_alert_config")
        )

        if not resp.get("Items"):
            raise RuntimeError("monitoring_alert_config not found")

        cfg_raw = resp["Items"][0]["clientConfig"]
        master = json.loads(cfg_raw) if isinstance(cfg_raw, str) else cfg_raw
        clients = master.get("clients", {})

        if not clients:
            raise RuntimeError("No clients configured")

        for client_name, client_cfg in clients.items():
            try:
                log_event(
                    "INFO",
                    f"Processing client={client_name}",
                    context,
                    client=client_name,
                )
                process_client(client_name, client_cfg, context)
            except Exception as e:
                log_event(
                    "FAILED",
                    f"Client processing failed for client={client_name}: {e}",
                    context,
                    client=client_name,
                    traceback=traceback.format_exc(),
                )

                alert_cfg = client_cfg.get("alert", {})
                subject = f"[ALERT] {context.function_name} client failure - {client_name}"
                body = (
                    f"Client: {client_name}\n"
                    f"Error:\n{e}\n\n"
                    f"{traceback.format_exc()}\n\n"
                    f"{build_lambda_link(context.function_name)}"
                )
                send_email_alert(client_name, alert_cfg, subject, body, context)

        log_event("ENDED", "Monitoring cron completed successfully", context)
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Monitoring cron completed successfully"})
        }

    except Exception as e:
        log_event("FAILED", f"Unhandled error: {e}\n{traceback.format_exc()}", context)

        # best effort global alert: try to read from all configured clients if possible
        try:
            tbl = dynamo.Table(CONFIG_TABLE_DYNAMO)
            resp = tbl.query(
                KeyConditionExpression=Key("clientName").eq("monitoring_alert_config")
            )
            if resp.get("Items"):
                cfg_raw = resp["Items"][0]["clientConfig"]
                master = json.loads(cfg_raw) if isinstance(cfg_raw, str) else cfg_raw
                clients = master.get("clients", {})
                for client_name, client_cfg in clients.items():
                    alert_cfg = client_cfg.get("alert", {})
                    subject = f"[ALERT] {context.function_name} failure"
                    body = (
                        f"Unhandled error:\n{e}\n\n"
                        f"{traceback.format_exc()}\n\n"
                        f"{build_lambda_link(context.function_name)}"
                    )
                    send_email_alert(client_name, alert_cfg, subject, body, context)
        except Exception as inner_e:
            log_event("WARN", f"Global failure email attempt failed: {inner_e}\n{traceback.format_exc()}", context)

        raise