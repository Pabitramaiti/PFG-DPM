import sys
import re
import boto3
import json
import logging
import os
from botocore.exceptions import ClientError
import splunk

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_run_id():
    # Get AWS account ID and return a unique run identifier for logging
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:metadata_validation:" + account_id

# Use a global variable for file_name, set in main()
file_name = ""
# Initialize global S3 client at module level
s3_client = boto3.client('s3')

def log_message(status, message):
    # Log to Splunk with run_id and file_name context
    splunk.log_message({'FileName': file_name, 'Status': status, 'Message': message}, get_run_id())

def load_config_from_s3(bucket_name: str, config_key: str, file_pattern: str) -> dict:
    # Load config JSON from S3, return as a single config block
    log_message("success", f"Loading config from s3://{bucket_name}/{config_key}")
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=config_key)
    except Exception as e:
        log_message("failure", f"Failed to fetch config from s3://{bucket_name}/{config_key}: {e}")
        logger.exception("Failed to fetch config from S3")
        raise RuntimeError(f"Failed to fetch config from s3://{bucket_name}/{config_key}: {e}")

    try:
        content = obj["Body"].read().decode("utf-8")
        config_json = json.loads(content)
    except Exception as e:
        log_message("failure", f"Failed to parse config JSON from s3://{bucket_name}/{config_key}: {e}")
        logger.exception("Failed to parse config JSON")
        raise RuntimeError(f"Failed to parse config JSON from s3://{bucket_name}/{config_key}: {e}")

    log_message("success", f"Config loaded with top-level keys: {list(config_json.keys())}")
    return config_json

def _parse_s3_path_hint(path_hint: str, default_bucket: str):
    # Parse a path hint, returning (bucket, prefix)
    if not path_hint:
        return default_bucket, ""
    ph = path_hint.strip()
    if ph.startswith("s3://"):
        rest = ph[5:]
        parts = rest.split('/', 1)
        b = parts[0]
        key = parts[1] if len(parts) > 1 else ''
        return b, key.lstrip('/')
    return default_bucket, ph.lstrip('/')

def _s3_key_exists(s3, bucket: str, key: str) -> bool:
    # Check if an S3 object exists
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = (e.response or {}).get('Error', {}).get('Code')
        if code in ('404', 'NoSuchKey', 'NotFound'):
            return False
        return False

def find_matching_files(bucket_name, s3_prefix, file_pattern):
    # Find all files in S3 under the prefix that match the regex pattern
    log_message("success", f"Searching for files in s3://{bucket_name}/{s3_prefix} matching '{file_pattern}'")
    matching_files = []
    try:
        regex = re.compile(file_pattern)
    except re.error as e:
        log_message("failure", f"Invalid file_pattern regex '{file_pattern}': {e}")
        logger.exception("Invalid regex in find_matching_files")
        raise ValueError(f"Invalid file_pattern regex '{file_pattern}': {e}")

    try:
        if s3_prefix and not s3_prefix.endswith('/'):
            s3_prefix += '/'
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix):
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                s3_key = obj['Key']
                file_name_local = s3_key.split('/')[-1]
                if regex.match(file_name_local):
                    matching_files.append({'key': s3_key, 'filename': file_name_local})
    except Exception as e:
        log_message("failure", f"Error listing objects in s3://{bucket_name}/{s3_prefix}: {e}")
        logger.exception("S3 listing error in find_matching_files")
        raise RuntimeError(f"Error listing objects in s3://{bucket_name}/{s3_prefix}: {e}")

    log_message("success", f"Found {len(matching_files)} files matching pattern")
    # Only allow exactly one match, otherwise fail
    if len(matching_files) == 1:
        return matching_files
    elif len(matching_files) == 0:
        return []
    else:
        log_message("failure", f"Multiple files found matching pattern '{file_pattern}' under {bucket_name}/{s3_prefix}: {[f['key'] for f in matching_files]}")
        raise RuntimeError(f"Multiple files found matching pattern '{file_pattern}' under {bucket_name}/{s3_prefix}: {[f['key'] for f in matching_files]}")

def check_missing_files(data_bucket: str, file_info: dict, config: dict):
    # Check for missing referenced files as per config checks
    log_message("success", f"Checking references for data file: s3://{data_bucket}/{file_info.get('key')}")
    delimiter = config.get('delimiter')
    field_names = config.get('fields')
    file_checks = config.get('file_validations') or {}
    if not delimiter:
        log_message("failure", "Config missing 'delimiter'")
        raise ValueError('delimiter is required in config')
    if not isinstance(field_names, list) or not field_names:
        log_message("failure", "Config 'fields' must be a non-empty list")
        raise ValueError('fields must be a non-empty list')
    if file_checks and not isinstance(file_checks, dict):
        log_message("failure", "Config 'file_validations' must be an object when provided")
        raise ValueError('checks must be an object keyed by source_field when provided')

    try:
        obj = s3_client.get_object(Bucket=data_bucket, Key=file_info['key'])
        file_content = obj['Body'].read().decode('utf-8', errors='ignore')
        lines = [ln for ln in file_content.split('\n') if ln.strip()]
    except Exception as e:
        log_message("failure", f"Failed to read data file s3://{data_bucket}/{file_info.get('key')}: {e}")
        logger.exception("Failed to read data file in check_missing_files")
        raise RuntimeError(f"Failed to read data file s3://{data_bucket}/{file_info.get('key')}: {e}")

    field_index_map = {name: i for i, name in enumerate(field_names)}
    file_extension_map = {}
    for field_name, check_rule in file_checks.items():
        if isinstance(check_rule, dict) and check_rule.get('filepresent'):
            extension = check_rule.get('extension')
            if isinstance(extension, str) and extension:
                file_extension_map[field_name] = extension if extension.startswith('.') else f'.{extension}'

    def ensure_extension(filename: str, extension: str) -> str:
        # Ensure the filename ends with the required extension
        filename_stripped = filename.strip()
        if not filename_stripped:
            return filename_stripped
        if filename_stripped.lower().endswith(extension.lower()):
            return filename_stripped
        return filename_stripped + extension

    missing_files = set()
    total_file_references_checked = 0
    expected_fields_count = len(field_names)
    for line_no, line in enumerate(lines, 1):
        parts = line.split(delimiter)
        # Check if the number of fields matches the config
        if len(parts) != expected_fields_count:
            log_message("failure", f"Line {line_no}: Field count mismatch. Expected {expected_fields_count}, got {len(parts)}")
            raise ValueError(f"Line {line_no}: Field count mismatch. Expected {expected_fields_count}, got {len(parts)}")
        for field_name, check_rule in file_checks.items():
            if not isinstance(check_rule, dict) or not check_rule.get('filepresent'):
                log_message("success", f"Skipping check for {field_name}: not a filepresent check")
                continue
            if field_name not in field_index_map:
                continue
            field_pos = field_index_map[field_name]
            field_value = parts[field_pos] if field_pos < len(parts) else None
            if not field_value or not str(field_value).strip():
                missing_files.add(f'<blank:{field_name}>')
                continue
            file_name_local = str(field_value).strip()
            if field_name in file_extension_map:
                file_name_local = ensure_extension(file_name_local, file_extension_map[field_name])
            bucket_for_check, prefix = _parse_s3_path_hint(check_rule.get('path', ''), data_bucket)
            s3_key_to_check = prefix.rstrip('/') + '/' + file_name_local.lstrip('/') if prefix else file_name_local.lstrip('/')
            total_file_references_checked += 1
            try:
                exists = _s3_key_exists(s3_client, bucket_for_check, s3_key_to_check)
            except Exception as e:
                # If the S3 helper raised something unexpected, log it and surface runtime error
                log_message("failure", f"S3 check error for {bucket_for_check}/{s3_key_to_check}: {e}")
                logger.exception("S3 check failed in check_missing_files")
                # Treat transient/unknown S3 errors as fatal for the job
                raise RuntimeError(f"S3 check error for {bucket_for_check}/{s3_key_to_check}: {e}")
            if not exists:
                missing_files.add(file_name_local)

    log_message("success", f"Checked {total_file_references_checked} file references; missing {len(missing_files)}")
    return {
        'data_file': file_info['key'],
        'total_lines': len(lines),
        'file_references_checked': total_file_references_checked,
        'missing_count': len(missing_files),
        'missing_files': sorted(missing_files),
    }

def set_job_params_as_env_vars():
    """Translate '--key value' (Glue job argument style) pairs into environment variables."""
    i = 1  # skip script name
    while i < len(sys.argv):
        token = sys.argv[i]
        if token.startswith('--'):
            key = token[2:]
            if i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith('--'):
                os.environ[key] = sys.argv[i + 1]
                i += 2
            else:
                # No value provided; skip
                i += 1
        else:
            i += 1


def main():

    global file_name
    global s3_client
    set_job_params_as_env_vars()

    # Read environment variables for job configuration
    file_key = os.getenv('file_key')
    bucket_name = os.getenv('s3_bucket')
    source_file_path = os.getenv('file_path')
    file_pattern = os.getenv('file_pattern')
    config_key = os.getenv('metadata_config')
    file_name = os.getenv('file_name', file_pattern or "")

    try:
        log_message("success", f"Starting edit job with s3bucket={bucket_name} filepath={source_file_path} file_pattern={file_pattern} config={config_key}")

        if not bucket_name or not source_file_path or not file_pattern or not config_key:
            log_message("failure", "Missing required environment variables. Aborting.")
            raise ValueError('Environment vars required: s3bucket, file_path (or filepath), file_pattern, config')

        if source_file_path == " " and file_pattern == " ":
            # Use file_key directly, do not move the file
            if  file_key == " ":
                log_message("failure", "file_key must be provided when source_file_path and file_pattern are blank.")
                raise ValueError("file_key must be provided when source_file_path and file_pattern are blank.")
            file_info = {'key': file_key, 'filename': file_key.split('/')[-1]}
            log_message("success", f"Using source file directly: {file_key}")
        else:
            matching_files = find_matching_files(bucket_name, source_file_path, file_pattern)
            if not matching_files:
                log_message("failure", f"No files found matching pattern '{file_pattern}' under {bucket_name}/{source_file_path}")
                raise FileNotFoundError(f'No files match pattern {file_pattern} under {bucket_name}/{source_file_path}')
            file_info = matching_files[0]
            log_message("success", f"Using file: {file_info['key']}")

        # Load config and process the file 
        config = load_config_from_s3(bucket_name, config_key, file_pattern)
        if not config:
            log_message("failure", f"No config block found for given file_pattern '{file_pattern}'")
            raise ValueError('No config block found for given file_pattern')

        summary = check_missing_files(bucket_name, file_info, config)
        if summary['missing_count']:
            log_message("failure", f"Missing files ({summary['missing_count']}): {', '.join(summary['missing_files'])}")
            raise RuntimeError(f"Missing files ({summary['missing_count']}): {', '.join(summary['missing_files'])}")

        log_message("success", f"All referenced files present. Summary: {json.dumps(summary)}")
        return summary
    except Exception as e:
        # Concise error handling: log stack trace locally and send a short message to Splunk
        log_message("failure", f"Unhandled exception in main: {e}")
        raise

if __name__ == '__main__':
    main()