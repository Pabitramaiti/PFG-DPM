import boto3
import boto3
import re
import os
import json
from collections import defaultdict
from botocore.exceptions import ClientError
import splunk
import zipfile
import io
import sys

s3_client = boto3.client("s3")

def get_run_id():
    """Get a unique run identifier based on AWS account ID"""
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:br_icsdev_post_validation:" + account_id

def log_message(status, message):
    """Log messages to Splunk with status and message"""
    file_name = os.getenv("fileName", " ")
    splunk.log_message({'FileName': file_name, 'Status': status, 'Message': message}, get_run_id())

def read_s3_file(bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = response["Body"].read()
        return data.decode("utf-8", errors="ignore")
    except ClientError as e:
        raise RuntimeError(f"Failed to read {key} from {bucket}: {e}")

def get_file_by_pattern(bucket, prefix, pattern):
    regex = re.compile(pattern)
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in resp.get("Contents", []):
        fname = obj["Key"].split("/")[-1]
        if regex.search(fname):
            return obj["Key"]
    return None

def parse_driver_file(driver_text, field_index):
    counts = defaultdict(int)
    for line in driver_text.splitlines():
        parts = [p.strip() for p in line.split("~")]
        if len(parts) > field_index:
            fund_code = parts[field_index]
            if fund_code:
                counts[fund_code] += 1
    return dict(counts)

def extract_check_issued_lines(register_text, issued_markers):
    if isinstance(issued_markers, str):
        issued_markers = [issued_markers]
    extracted = []
    for raw_line in register_text.splitlines():
        clean_line = re.sub(r"[^\x20-\x7E]", " ", raw_line)
        if any(m.upper() in clean_line.upper() for m in issued_markers):
            extracted.append(clean_line)
    return extracted

def parse_check_issued_lines(extracted_lines, fundcode_patterns, issued_count_regex):
    if isinstance(fundcode_patterns, str):
        fundcode_patterns = [fundcode_patterns]
    if not issued_count_regex:
        raise ValueError("issued_count_regex is required in config.json")

    issued_count_pattern = re.compile(issued_count_regex, re.IGNORECASE)

    counts = {}
    for line in extracted_lines:
        count_match = issued_count_pattern.search(line)
        if not count_match:
            continue
        num_checks = int(count_match.group(1))

        fund_code = None
        for pat in fundcode_patterns:
            m = re.search(pat, line)
            if m:
                fund_code = m.group(1)
                break

        if fund_code:
            counts[fund_code] = num_checks
        else:
            log_message("failed", f"Could not extract fund code from line: {line}")
    return counts

def validate_checks_per_fund(bucket, driver_key, register_key, rule):
    driver_text = read_s3_file(bucket, driver_key)
    expected_counts = parse_driver_file(driver_text, rule["driver_fund_field_index"])
    log_message("logging", f"Driver counts {driver_key}: {expected_counts}")

    register_text = read_s3_file(bucket, register_key)
    extracted_lines = extract_check_issued_lines(register_text, issued_markers=rule.get("issued_marker"))
    actual_counts = parse_check_issued_lines(extracted_lines,
                                             fundcode_patterns=rule.get("fundcode_regex"),
                                             issued_count_regex=rule.get("issued_count_regex"))
    log_message("logging", f"Register counts {register_key}: {actual_counts}")

    mismatches = []
    for fund_code, expected in expected_counts.items():
        actual = actual_counts.get(fund_code)
        if actual is None:
            mismatches.append(f"Fund {fund_code}: missing in register (expected {expected})")
        elif expected != actual:
            mismatches.append(f"Fund {fund_code}: expected {expected}, got {actual}")

    if mismatches:
        for m in mismatches:
            log_message("failed", m)
        raise ValueError(" ; ".join(mismatches))

    log_message("success", f"Check count validation SUCCESS for {driver_key} vs {register_key}")
    return True

def validate_trailer_count(bucket, trailer_key, rule):
    detail_id = rule["detail_record_identifier"]
    trailer_id = rule["trailer_record_identifier"]
    field_index = rule["trailer_count_field_index"]

    text = read_s3_file(bucket, trailer_key)
    lines = text.splitlines()

    block_num = 0
    detail_count = 0
    for line in lines:
        if line.startswith(detail_id):
            detail_count += 1
        elif line.startswith(trailer_id):
            block_num += 1
            parts = line.split("~")
            if field_index >= len(parts):
                raise ValueError(f"Invalid field index {field_index} for trailer: {line}")
            expected_count = int(parts[field_index].strip())
            if detail_count != expected_count:
                raise ValueError(f"Block {block_num}: expected {expected_count}, found {detail_count}")
            else:
                log_message("success", f"Block {block_num}: Trailer count matched ({expected_count})")
            detail_count = 0
    return True

def validate_layout(file_text, rule):
    """
    Validate layout:
    - Header must contain Date (YYYYMMDD or MM/DD/YYYY) + Count
    - Details must contain CheckNumber + AccountNumber (numeric)
    - Header count must = number of detail records
    Raises ValueError on any failure.
    """

    lines = [line.strip() for line in file_text.splitlines() if line.strip()]

    if not lines:
        msg = f"{rule['id']}: File empty"
        log_message("failed", msg)
        raise ValueError(msg)

    # --- Validate header line ---
    header = lines[0]
    header_match = re.match(rule["layout_pattern"], header)
    if not header_match:
        msg = f"{rule['id']}: Invalid header format -> '{header}'"
        log_message("failed", msg)
        raise ValueError(msg)

    # Extract date and count
    try:
        date_val, count_str = header_match.groups()
        expected_count = int(count_str)
    except Exception:
        msg = f"{rule['id']}: Invalid header values -> '{header}'"
        log_message("failed", msg)
        raise ValueError(msg)

    log_message("success", f"{rule['id']}: Valid header -> Date={date_val}, Count={expected_count}")

    # --- Validate detail lines ---
    detail_pattern = re.compile(rule["detail_pattern"])
    actual_count = 0

    for idx, line in enumerate(lines[1:], start=2):
        if not detail_pattern.match(line):
            msg = f"{rule['id']}, line {idx}: Invalid detail -> '{line}'"
            log_message("failed", msg)
            raise ValueError(msg)   # Fail immediately on any bad detail
        else:
            log_message("success", f"{rule['id']}, line {idx}: Valid record")
            actual_count += 1

    # --- Validate record count ---
    if actual_count != expected_count:
        msg = f"{rule['id']}: Header count {expected_count} does not match actual detail count {actual_count}"
        log_message("failed", msg)
        raise ValueError(msg)

    log_message("success", f"{rule['id']}: Header count matches actual detail count ({expected_count})")
    return True


def set_job_params_as_env_vars():
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            value = sys.argv[i + 1]
            os.environ[key] = value
            # log_message('logging',f" Set environment variable {key} to {value}")

def main():
    set_job_params_as_env_vars()
    
    s3_bucket = os.getenv("s3_bucket")
    s3_prefix = os.getenv("ascii_file_path", "")
    config_file_key = os.getenv("config_file")
    file_name = os.getenv("fileName", "")


    if not (s3_bucket or config_file_key):
        raise RuntimeError("Missing required env vars: s3_bucket, config_file")

    # Load config JSON
    config_text = read_s3_file(s3_bucket, config_file_key)
    config = json.loads(config_text)

    if not file_name:
        raise RuntimeError("Missing required env var: fileName")

    log_message("logging", f"Received fileName={file_name}, checking against config patterns")

    executed_any = False

    # process check_count rules
    for rule in config.get("check_count", []):
        if re.match(rule["trigger_file_pattern"], file_name, flags=re.IGNORECASE):
            log_message("logging", f"Matched check_count rule: {rule['id']} for file {file_name}")
            driver_key = get_file_by_pattern(s3_bucket, s3_prefix, rule["driver_file_pattern"])
            register_key = get_file_by_pattern(s3_bucket, s3_prefix, rule["register_file_pattern"])

            if not driver_key or not register_key:
                log_message("failed", f"Files not found for rule {rule['id']}")
                continue
            validate_checks_per_fund(s3_bucket, driver_key, register_key, rule)
            executed_any = True

    # process trailer_count rules
    for rule in config.get("trailer_count", []):
        if re.match(rule["trigger_file_pattern"], file_name, flags=re.IGNORECASE):
            log_message("logging", f"Matched trailer_count rule: {rule['id']} for file {file_name}")
            trailer_key = get_file_by_pattern(s3_bucket, s3_prefix, rule["ascii_file_pattern"])
            if not trailer_key:
                log_message("failed", f"Trailer file not found for rule {rule['id']}")
                continue
            validate_trailer_count(s3_bucket, trailer_key, rule)
            executed_any = True

    # --- Layout Validation Rules ---
    for rule in config.get("layout_validation", []):
        if re.match(rule["trigger_file_pattern"], file_name, flags=re.IGNORECASE):
            log_message("logging", f"Running layout validation rule: {rule['id']}")
    
            # First try explicit key from payload
            layout_key = os.getenv("key", "")
    
            if not layout_key:
                base_path = rule.get("layout_base_path")
                if not base_path:
                    log_message("failed", f"No layout_base_path configured for {rule['id']}")
                    raise ValueError(f"No layout_base_path configured for {rule['id']}")
                
                # Ensure no duplicate slashes
                layout_key = base_path.rstrip("/") + "/" + file_name
    
                log_message("logging", f"Derived layout key from config: {layout_key}")
    
            if not layout_key:
                log_message("failed", f"Layout file not provided or derived for rule {rule['id']}")
                raise ValueError(f"No layout key provided/derived for {rule['id']}")
    
            text = read_s3_file(s3_bucket, layout_key)
            validate_layout(text, rule)   # raises if layout fails
            executed_any = True

    if not executed_any:
        log_message("logging", f"No matching rules found for fileName: {file_name}")

if __name__ == "__main__":
    main()