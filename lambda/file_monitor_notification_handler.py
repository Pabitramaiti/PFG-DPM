#!/usr/bin/env python3
"""
monitor_and_alert.py

- Uses two DynamoDB tables:
    * GROUPS table (tableA)  -> holds Frequency, file_pattern, recipients (per pattern)
    * METADATA table (tableB) -> holds incoming files metadata (created_at, grp_completion_status, file_pattern/file_name ...)

Behavior:
- If today is a holiday (US federal OR weekend defined as Sunday & Monday) -> send holiday email and exit.
- Otherwise, for each group-record (pattern) in GROUPS table:
    - Scan METADATA table for items where:
        - grp_completion_status == "pending"
        - created_at is today and <= 04:00 AM
        - metadata file pattern/value matches the group's file_pattern (regex)
    - If matches found -> send HTML email to recipients defined on that group-record.
"""
from __future__ import annotations
import os
import re
import sys
import calendar
from datetime import datetime, date, time, timedelta, timezone
from typing import List, Dict, Any, Tuple, Optional, Set
from warnings import catch_warnings
import boto3,json
from botocore.exceptions import ClientError
# ---------------------------- Configuration ----------------------------
REGION = os.environ.get("AWS_REGION", "us-east-1")
# Table names; override with env vars if you want
TABLE_GROUPS = ""   # group-frequency records (your tableA)
TABLE_METADATA ="" # metadata table (your tableB)
# Attribute name defaults / fallbacks
ATTR_FREQ = "Frequency"
ATTR_GROUP_PATTERN = "file_pattern"       # group table pattern
ATTR_GROUP_RECIPIENTS = "recipients"      # primary recips attr on group records
ATTR_GROUP_EXPIRE_TIME="expire_time"
ATTR_GROUP_NO_FILE_NOTIFICATION="noFileNotify"
ATTR_GROUP_BUCKET_NAME="destination_bucket"
ATTR_GROUP_KEY_PATH="destination_key_path"
GROUP_EXPIRE_HOUR=4 #default value is 4 am
GROUP_EXPIRE_MINUTE=30 #default value is 30 min
GROUP_NO_File_NOTIFY=False

BUCKET_NAME = ""
KEY_PATH=""

ATTR_META_STATUS = "sgrp_status"
ATTR_META_CREATED = "created_at"          # expected: "YYYY-MM-DD HH:MM:SS"
ATTR_META_PATTERN = "file_pattern"        # metadata pattern/name field (fallback below)
ALT_META_PATTERN = "sgrp_pattern"
ATTR_META_FILENAME = "file_name"
ATTR_META_EMAIL_FLAG ="email_notified"
# SES sender
SES_SENDER = ""
# Weekend rules: Sunday (6) and Monday (0)
WEEKENDS = {6, 0}
# Valid frequencies (normalize to lowercase)
VALID_FREQUENCIES = {"daily", "weekly", "monthly", "yearly"}
# single boto3 resources/clients shared
dynamodb = boto3.resource("dynamodb", region_name=REGION)
ses_client = boto3.client("ses", region_name=REGION)
s3_client = boto3.client("s3", region_name=REGION)

file_found=False
# ---------------------------- Utilities ----------------------------


# Global context to carry the last email payload for printing to file
EMAIL_PRINT_RECEIPTS = {
    "recipients": [],
    "subject": "",
    "html_body": "",
    "extra": {}  # optional: frequency, pattern, etc.
}


def check_file_created_today(bucket_name: str, folder: str, pattern: str) -> bool:

  

    now = datetime.now(timezone.utc)
    today_str = now.strftime("%Y%m%d")

    regex = re.compile(pattern.replace("zip","txt").replace("ZIP","TXT").replace("\\\\","\\"))

    # Build prefix for efficient listing
    prefix = folder.strip("/") + "/"

    # List objects under the folder

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if "Contents" not in response:
        print(f"No files found in {prefix}")
        return False
    for obj in response["Contents"]:
        key = obj["Key"].split("/")[-1]  # filename only
        last_modified = obj["LastModified"].date()
        now = datetime.now(timezone.utc)
        today_date = now.date()
        # Check regex match and creation date
        if regex.match(key) and last_modified == today_date:
            print(f"File found: {key}, created at {obj['LastModified']}")
            return True

    print(f"No file matching pattern found for today ({today_str})")
    return False

def set_email_receipt_context(recipients, subject, html_body, **extra):
    """
    Helper to set the global email receipt context so write_to_file(email=True)
    can print the right content into the file.    """
    global EMAIL_PRINT_RECEIPTS
    EMAIL_PRINT_RECEIPTS = {
        "recipients": list(recipients or []),
        "subject": subject or "",
        "html_body": html_body or "",
        "extra": extra or {}
    }

def write_to_file(BUCKET_NAME: str, file_floader: str, fname: str, email: bool = True) -> str:
    """
    Create a text file and upload it to S3.
    If email=True, the file content will be an 'email print receipt' containing:
      - recipients
      - subject
      - timestamp (UTC)
      - first 512 chars of the HTML body (sanitized)
      - optional extra metadata (frequency, pattern)

    Args:
        BUCKET_NAME: S3 bucket name (str)
        file_floader: S3 prefix/folder to place the file (e.g. 'incoming' or 'receipts/')
        fname: final object key's file name (e.g. 'JSC.STMT.ADD.20251202.114012.txt')
        email: if True, write an email receipt; else write a generic marker

    Returns:
        s3_uri: 's3://bucket/prefix/fname' string
    """
    # Ensure text extension
    base_name = fname
    if not base_name.lower().endswith(".txt"):
        base_name = os.path.splitext(base_name)[0] + ".txt"

    # Normalize prefix (folder)
    prefix = (file_floader or "").strip().strip("/")
    key = f"{prefix}/{base_name}" if prefix else base_name

    # Compose content
    now = datetime.utcnow()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S UTC")

    if email:
        recips = EMAIL_PRINT_RECEIPTS.get("recipients", [])
        subject = EMAIL_PRINT_RECEIPTS.get("subject", "")
        html_body = EMAIL_PRINT_RECEIPTS.get("html_body", "")
        extra = EMAIL_PRINT_RECEIPTS.get("extra", {})

        # Take a safe snippet of HTML body for the receipt
        snippet = (html_body or "").replace("\r", " ").replace("\n", " ")
        if len(snippet) > 512:
            snippet = snippet[:512] + "…"

        # Build a clear, human-readable receipt
        lines = [
            "=== EMAIL PRINT RECEIPT ===",
            f"Timestamp: {timestamp}",
            f"Subject: {subject}",
            f"Recipients ({len(recips)}): {', '.join(recips) if recips else '(none)'}",
        ]
        # Include optional metadata if provided
        if extra:
            lines.append("Metadata:")
            try:
                lines.append(json.dumps(extra, ensure_ascii=False, indent=2))
            except Exception:
                lines.append(str(extra))

        lines.extend([
            "",
            "--- Body (snippet) ---",
            snippet,
            "",
            "=== END RECEIPT ==="
        ])
        content = "\n".join(lines)
    else:
        content = f"Generated file at {timestamp}\nKey: {key}\n"

    # Upload to S3
  
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=content.encode("utf-8"),
        ContentType="text/plain; charset=utf-8"
    )

    s3_uri = f"s3://{BUCKET_NAME}/{key}"
    print(f"Receipt written to {s3_uri}")
    return s3_uri


def generate_filename_from_pattern(pattern: str, force_txt: bool = True) -> str:


    # Work on a copy of the pattern for generation
    gen_pattern = pattern

    # 1) Replace extension group to 'txt' if requested
    if force_txt:
        # Replace common zip group variations with literal 'txt'
        # Handles (zip|ZIP), (ZIP|zip), (zip|ZIP|txt|TXT), etc.
        gen_pattern = re.sub(r"\((?i:zip)(?:\|(?i:zip))*\)", "txt", gen_pattern)  # pure zip group
        gen_pattern = re.sub(r"\((?i:zip)(?:\|(?i:zip))*\|(?i:txt)(?:\|(?i:txt))*\)", "txt", gen_pattern)  # zip/txt group

        # Also handle if the pattern used a non-capturing group or just 'zip' literal
        gen_pattern = re.sub(r"(?i:zip)$", "txt", gen_pattern)

    # 2) Create the concrete filename by substituting placeholders
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y%m%d")  # for \d{8}
    time_str = now.strftime("%H%M%S")  # for \d{6}

    # We construct a template by removing anchors and literal-escaping dots/groups
    # Strategy: take fixed parts and substitute date/time.
    # For known structure like: ^((JSC)\.(STMT)\.(ADD)\.(\d{8})\.(\d{6})\.(txt))$
    # we build: JSC.STMT.ADD.{date}.{time}.txt

    # Extract fixed segments before/after date/time using a simple approach:
    #  - Remove start/end anchors
    core = gen_pattern.strip()
    if core.startswith("^"):
        core = core[1:]
    if core.endswith("$"):
        core = core[:-1]

    # Replace the date/time tokens with our concrete strings
    core = re.sub(r"\\d\{8\}", date_str, core)
    core = re.sub(r"\\d\{6\}", time_str, core)

    # Remove regex grouping syntax to get the literal name:
    # - replace `\.` with `.`
    core = core.replace(r"\.", ".")
    # - remove capturing groups parentheses
    core = re.sub(r"[\(\)]", "", core)
    # - remove non-regex artifacts like multiple pipes in extension (already handled)
    # - remove stray backslashes left from escaping
    core = core.replace("\\", "")

    filename = core

    # 3) Optional: validate against adjusted pattern (with txt)
    # Build a validation regex where the extension is 'txt'
    validation_pattern = pattern
    if force_txt:
        validation_pattern = re.sub(r"\((?i:zip)(?:\|(?i:zip))*\)", "txt", validation_pattern)
        validation_pattern = re.sub(r"\((?i:zip)(?:\|(?i:zip))*\|(?i:txt)(?:\|(?i:txt))*\)", "txt", validation_pattern)
        validation_pattern = re.sub(r"(?i:zip)\)$", "txt)", validation_pattern)

    if not re.match(validation_pattern, filename):
        # If the original pattern had only zip/ZIP, the validation may fail—so validate against a
        # derived pattern that accepts txt explicitly:
        derived = re.sub(r"\((?i:zip)(?:\|(?i:zip))*\)", "(txt|TXT)", pattern)
        derived = re.sub(r"\((?i:zip)(?:\|(?i:zip))*\|(?i:txt)(?:\|(?i:txt))*\)", "(txt|TXT)", derived)
        if not re.match(derived, filename):
            raise ValueError(f"Generated filename '{filename}' does not fit derived validation pattern.")

    return filename

def get_table(table_name: str):
    return dynamodb.Table(table_name)
def today_cutoff_time(hour, minute) -> Tuple[date, datetime]:
    now = datetime.now()
    today_date = now.date()
    cutoff = datetime.combine(today_date, time(hour, minute, 0))
    return today_date, cutoff
def parse_timestamp(ts_str: str) -> Optional[datetime]:
    """Parse 'YYYY-MM-DD HH:MM:SS' — return None on failure."""
    if not ts_str or not isinstance(ts_str, str):
        return None
    try:
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
def fetch_file_details_admail(bucket_name, folders, pattern):
    try:
        file_details = []
        for folder in folders:
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name, Prefix=folder):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    file_name = os.path.basename(key)
                    regex = re.compile(pattern)
                    match_found = regex.match(file_name)
                    if match_found:
                        file_details.append({"file_name": file_name, "modified_date": obj["LastModified"]})
        return file_details
    except Exception as e:
        print(f"[ERROR] failed while checking pattern existance: {str(e)}")

# ---------------------------- Month/Year-end checks ----------------------------
def get_last_working_day_of_month(year: int, month: int) -> date:
    last_day_num = calendar.monthrange(year, month)[1]
    last_day = date(year, month, last_day_num)
    while last_day.weekday() in WEEKENDS:
        last_day -= timedelta(days=1)
    return last_day

def month_year_end_flags(check_date: Optional[date] = None) -> Dict[str, bool]:
    if check_date is None:
        check_date = date.today()
    year, month = check_date.year, check_date.month
    is_month_end = (check_date.day == calendar.monthrange(year, month)[1])
    is_last_working_day_of_month = (check_date == get_last_working_day_of_month(year, month))
    is_year_end = (month == 12 and check_date.day == 31)
    is_last_working_day_of_year = (check_date == get_last_working_day_of_month(year, 12))
    return {
        "is_month_end": is_month_end,
        "is_last_working_day_of_month": is_last_working_day_of_month,
        "is_year_end": is_year_end,
        "is_last_working_day_of_year": is_last_working_day_of_year
    }

# ---------------------------- Holiday logic ----------------------------
def nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first_day = date(year, month, 1)
    offset = (weekday - first_day.weekday() + 7) % 7
    return first_day + timedelta(days=offset + (n - 1) * 7)
def last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    offset = (last_day.weekday() - weekday + 7) % 7
    return last_day - timedelta(days=offset)
def easter_date(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)
def get_us_holidays(year: int) -> Dict[str, date]:
    return {
        "New Year's Day": date(year, 1, 1),
        "Martin Luther King, Jr. Day": nth_weekday(year, 1, 0, 3),
        "Presidents Day": nth_weekday(year, 2, 0, 3),
        "Good Friday": easter_date(year) - timedelta(days=2),
        "Memorial Day": last_weekday(year, 5, 0),
        "Juneteenth": date(year, 6, 19),
        "Independence Day": date(year, 7, 4),
        "Labor Day": nth_weekday(year, 9, 0, 1),
        "Thanksgiving Day": nth_weekday(year, 11, 3, 4),
        "Christmas Day": date(year, 12, 25),
    }
def check_us_holiday(d: date) -> Tuple[bool, str]:
    """Return (True, name) if holiday (weekend or federal), else (False, '')."""
    if d.weekday() in WEEKENDS:
        return True, "Weekend (Sunday/Monday)"
    for n, hd in get_us_holidays(d.year).items():
        if d == hd:
            return True, n
    return False, ""
# ---------------------------- Group table loader ----------------------------
def fetch_grouped_records(group_table: str = TABLE_GROUPS) -> List[Dict[str, Any]]:
    """
    Read the GROUPS table (tableA) and return list of group records.
    We intentionally return the raw items (no grouping) because each record
    represents a pattern + recipients and will be processed individually.
    """
    table = get_table(group_table)
    items: List[Dict[str, Any]] = []
    scan_kwargs = {}
    try:
        while True:
            resp = table.scan(**scan_kwargs)
            items.extend(resp.get("Items", []) or [])
            lek = resp.get("LastEvaluatedKey")
            if not lek:
                break
            scan_kwargs["ExclusiveStartKey"] = lek
    except ClientError as e:
        print("Error scanning group table:", e.response.get("Error", {}).get("Message", str(e)))
    except Exception as e:
        print("Unexpected error scanning group table:", str(e))
    return items
# ---------------------------- Metadata scanner ----------------------------
def _compile_pattern_safe(pat: str) -> Optional[re.Pattern]:
    try:
        return re.compile(pat)
    except re.error:
        print(f"Invalid regex (skipped): {pat}")
        return None

def mark_items_emailed(matched_items: List[Dict[str, Any]], table_name: str = TABLE_METADATA):
    """
    Marks each item in matched_items as emailed in DynamoDB.
    Updates only:
        email_notified = True
    """
    table = get_table(table_name)

    for item in matched_items:
        # DynamoDB tables must have PK + SK or PK
        key_fields = table.key_schema
        key = {}

        for k in key_fields:
            attr = k["AttributeName"]
            if attr not in item:
                print(f" WARNING: Matched item missing key attribute {attr}, skipping update.")
                continue
            key[attr] = item[attr]

        try:
            table.update_item(
                Key=key,
                UpdateExpression="SET email_notified = :val",
                ExpressionAttributeValues={
                    ":val": True
                }
            )
            print(f"Updated DynamoDB record → email_notified=True, Key={key}")

        except ClientError as e:
            print(
                f" DynamoDB update failed for Key={key}:",
                e.response.get("Error", {}).get("Message", str(e))
            )


def scan_metadata_for_pattern(patterns: List[str], metadata_table: str = TABLE_METADATA) -> List[Dict[str, Any]]:
    """
    Scan METADATA table and return items that:
      - grp_completion_status == 'pending' (case-insensitive)
      - created_at exists, is today, and <= 04:00
      - metadata pattern/filename field matches ANY of the regex patterns list
    NOTE: This function is generic and returns raw matched items.
    """
    global file_found
    if not patterns:
        return []

    #compiled = [_f for _f in (_compile_pattern_safe(p) for p in patterns) if _f]
    compiled = [re.compile(p.replace('\\\\', '\\')) for p in patterns if p and re.compile(p.replace('\\\\', '\\'))]
    if not compiled:
        return []

    table = get_table(metadata_table)
    results: List[Dict[str, Any]] = []
    scan_kwargs = {}

    try:
        while True:
            resp = table.scan(**scan_kwargs)
            items = resp.get("Items", []) or []

            for it in items:

                status = (it.get(ATTR_META_STATUS) or "").strip().lower()

                today_date, cutoff_dt = today_cutoff_time(GROUP_EXPIRE_HOUR,GROUP_EXPIRE_MINUTE)
                email_flag =False
                if ATTR_META_EMAIL_FLAG in it:
                    email_flag =it.get(ATTR_META_EMAIL_FLAG)
                if email_flag:
                    continue

                if status != "pending":
                    continue
                created_str = it.get(ATTR_META_CREATED) or ""
                created_dt = parse_timestamp(created_str)
                if not created_dt:
                    continue
                if created_dt.date() != today_date:
                    continue
                if created_dt > cutoff_dt:
                    continue

                # fetch candidate value(s) to match against group's regex:
                # prefer metadata attribute names in order: file_pattern, sgrp_pattern, file_name
                candidate =   it.get(ATTR_META_FILENAME) or it.get(ATTR_META_PATTERN) or ""
                if not isinstance(candidate, str) or not candidate:
                    continue

                # match any compiled regex

                if any(rx.search(candidate) for rx in compiled):
                    if GROUP_NO_File_NOTIFY:
                        file_found=True
                        continue
                    results.append(it)

            lek = resp.get("LastEvaluatedKey")
            if not lek:
                break
            scan_kwargs["ExclusiveStartKey"] = lek

    except ClientError as e:
        raise RuntimeError(f"Metadata scan failed: {e.response.get('Error', {}).get('Message', str(e))}") from e

    return results
# ---------------------------- Email helpers ----------------------------
def normalize_recipients(value: Any) -> List[str]:
    """Accept string (comma separated) or list and return list of cleaned emails."""
    if not value:
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, str):
        return [x.strip() for x in value.split(",") if x.strip()]
    return []

def send_pattern_alert_email(frequency: str, group_record: Dict[str, Any], matched_items: List[Dict[str, Any]]):
    """Send one HTML email for the group_record (pattern) to recipients defined in the group record."""

    # get recipients from group record with fallback key names
    recips = group_record.get(ATTR_GROUP_RECIPIENTS) or group_record.get("recipents")
    recipients = normalize_recipients(recips)
    if not recipients:
        print("No recipients found for group record; skipping email.")
        return

    pattern_str = (group_record.get(ATTR_GROUP_PATTERN) or "").strip()

    if not matched_items:
        # If no matched items, send email with "No file arrived"
        html_body = f"""
        <html><body>
          <h3>No file arrived for pattern: <code>{escape_html(pattern_str)}</code></h3>
          <p>Frequency: {escape_html(frequency)}</p>
          <p style="color:red;font-weight:bold;">No files were received during this period.</p>
        </body></html>
        """
        subject = f"[{frequency.upper()} ALERT] No file arrived: {pattern_str}"
        pattern = r"^((JSC)\.(STMT)\.(ADD)\.(\d{8})\.(\d{6})\.(zip|ZIP))$"
        fname = generate_filename_from_pattern(pattern, force_txt=True)

        set_email_receipt_context(recipients, subject, html_body)
        write_to_file(BUCKET_NAME,KEY_PATH,fname,html_body)
    else:
        # build HTML table rows
        rows = []
        for it in matched_items:
            candidate = it.get(ATTR_META_PATTERN) or it.get(ALT_META_PATTERN) or it.get(ATTR_META_FILENAME) or ""
            created = it.get(ATTR_META_CREATED, "")
            status = it.get(ATTR_META_STATUS, "")
            fn = it.get(ATTR_META_FILENAME, "")
            rows.append((candidate, fn, created, status))

        html_rows = "".join(
            "<tr>"
            f"<td>{escape_html(r[0])}</td>"
            f"<td>{escape_html(r[1])}</td>"
            f"<td>{escape_html(r[2])}</td>"
            f"<td>{escape_html(r[3])}</td>"
            "</tr>"
            for r in rows
        )

        html_body = f"""
        <html><body>
          <h3>Pending files matched for pattern: <code>{escape_html(pattern_str)}</code></h3>
          <p>Frequency: {escape_html(frequency)}</p>
          <table border="1" cellpadding="6" cellspacing="0">
            <tr style="background:#e8f4ff"><th>Matched Value</th><th>File Name</th><th>Created At</th><th>Status</th></tr>
            {html_rows}
          </table>
          <p>Total: {len(rows)}</p>
        </body></html>
        """
        subject = f"[{frequency.upper()} ALERT] Pending files matched: {pattern_str}"

    # Send email
    try:
        ses_client.send_email(
            Source=SES_SENDER,
            Destination={"ToAddresses": recipients},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Html": {"Data": html_body}}
            }
        )
        print(f"Email sent to {', '.join(recipients)} for pattern {pattern_str}")
        if GROUP_NO_File_NOTIFY and not file_found:
            pass

        if matched_items:
            mark_items_emailed(matched_items)



    except ClientError as e:
        print("SES send error:", e.response.get("Error", {}).get("Message", str(e)))

# ---------------------------- Small helpers ----------------------------
def escape_html(s: Any) -> str:
    """Minimal HTML escaping for strings inserted into email body."""
    if s is None:
        return ""
    s = str(s)
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;")
             .replace("'", "&#x27;"))
# ---------------------------- Main processing ----------------------------
def process_group_records_for_frequency(freq_name: str, group_records: List[Dict[str, Any]], metadata_table: str = TABLE_METADATA):
    """
    For each record in group_records (each contains a file_pattern and recipients),
    scan metadata for matches and send email per pattern if matches exist.
    """
    if not group_records:
        return "No records found for pattern"

    for rec in group_records:
        global GROUP_EXPIRE_MINUTE
        global GROUP_EXPIRE_HOUR
        global GROUP_NO_File_NOTIFY
        global BUCKET_NAME
        global KEY_PATH
        patt = (rec.get(ATTR_GROUP_PATTERN) or "").strip()
        GROUP_NO_File_NOTIFY=rec.get(ATTR_GROUP_NO_FILE_NOTIFICATION)
        expire_time=(rec.get(ATTR_GROUP_EXPIRE_TIME)).split(":")
        if GROUP_NO_File_NOTIFY:
            BUCKET_NAME = rec.get(ATTR_GROUP_BUCKET_NAME)
            KEY_PATH =rec.get(ATTR_GROUP_KEY_PATH)
            if check_file_created_today(BUCKET_NAME, KEY_PATH, patt):
                continue
        GROUP_EXPIRE_HOUR=int(expire_time[0])
        GROUP_EXPIRE_MINUTE=int(expire_time[1])
        if not patt:
            continue

        try:
            # Use single-pattern scan for efficiency/clarity
            matched = scan_metadata_for_pattern([patt], metadata_table)
            if GROUP_NO_File_NOTIFY and not file_found:
                send_pattern_alert_email(freq_name, rec,"")

        except RuntimeError as e:
            print(f"Error scanning metadata for pattern {patt}: {e}")
            matched = []

        if matched:
            send_pattern_alert_email(freq_name, rec, matched)
def main():
    today = date.today()
    # 1. Holiday check
    is_holi, hol_name = check_us_holiday(today)
    if is_holi:
        print(f"Holidays founded+ {hol_name}")
        sys.exit()
    # 2. Load group records (tableA)
    group_items = fetch_grouped_records(TABLE_GROUPS)
    # partition group records by normalized frequency
    groups_by_freq: Dict[str, List[Dict[str, Any]]] = {f: [] for f in VALID_FREQUENCIES}
    for g in group_items:
        freq = (g.get(ATTR_FREQ) or "").strip().lower()
        if freq in VALID_FREQUENCIES:
            groups_by_freq[freq].append(g)

    # 3. Evaluate month/year-end flags to decide monthly/yearly processing
    flags = month_year_end_flags(today)
    process_monthly = flags.get("is_month_end", False)
    process_yearly = flags.get("is_year_end", False)

    # 4. Always process daily and weekly if records exist
    process_group_records_for_frequency("daily", groups_by_freq.get("daily", []), TABLE_METADATA)
    process_group_records_for_frequency("weekly", groups_by_freq.get("weekly", []), TABLE_METADATA)

    # 5. Monthly/yearly conditional
    if process_monthly:
        process_group_records_for_frequency("monthly", groups_by_freq.get("monthly", []), TABLE_METADATA)
    if process_yearly:
        process_group_records_for_frequency("yearly", groups_by_freq.get("yearly", []), TABLE_METADATA)

    print("Processing completed.")

def set_event_env_vars(event: dict) -> None:
    for k, v in event.items():
        os.environ[k] = str(v)

def lambda_handler(event, context):

    try:
        global SES_SENDER
        global TABLE_GROUPS
        global TABLE_METADATA
        set_event_env_vars(event)
        SES_SENDER = os.getenv("SES_SENDER")
        TABLE_GROUPS = os.getenv("TABLE_GROUPS")  
        TABLE_METADATA = os.getenv("TABLE_METADATA") 
        main()
    except Exception as e:
        raise e
    return {"statusCode": 200}

# if __name__ == '__main__':
#     main()






