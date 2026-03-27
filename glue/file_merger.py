import os
import sys
import json
import time
import argparse
import tempfile
import zipfile
import boto3
import shutil
import splunk
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr


def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:file_merger: " + account_id

run_id = get_run_id()
inputFileName = ""  # Global variable for Splunk logging

def log_message(status, message):
    try:
        splunk.log_message({'FileName': inputFileName, 'Status': status, 'Message': message}, run_id)
    except Exception as e:
        print(f"Logging failed: {e} | Status: {status} | Message: {message}")

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--group_name", required=True, help="Run ID or group name")
    p.add_argument("--group_items", required=True, help="List of file items in JSON string")
    p.add_argument("--output_prefix", required=True, help="S3 prefix for merged output file")
    p.add_argument("--file_monitoring_table", required=True, help="DynamoDB table to update")
    p.add_argument("--grp_initial_status", required=False)
    p.add_argument("--grp_completion_status", required=False)
    args, _ = p.parse_known_args()

    try:
        args.group_items = json.loads(args.group_items)
        log_message('success', f"Successfully parsed group_items for group: {args.group_name}")
    except Exception as e:
        print(f"[ERROR] Failed to parse group_items JSON: {e}", file=sys.stderr)
        log_message('failed', f"Failed to parse group_items JSON: {e}")
        raise
    return args

# AWS clients
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

def s3_upload_with_retry(local_path, bucket, key, retries=3, backoff=5):
    """Upload file to S3 with retries"""
    for attempt in range(retries):
        try:
            s3.upload_file(local_path, bucket, key)
            print(f"[OK] Uploaded -> s3://{bucket}/{key}")
            log_message('success', f"Uploaded file to s3://{bucket}/{key}")
            return True
        except ClientError as e:
            print(f"[WARN] Upload attempt {attempt+1} failed for {key}: {e}")
            log_message('warning', f"Upload attempt {attempt+1} failed for {key}: {e}")
            time.sleep(backoff * (attempt+1))
    print(f"[ERROR] Failed to upload {key} after {retries} retries")
    log_message('failed', f"Failed to upload {key} after {retries} retries")
    return False

def make_merged_zip(source_folder, output_zip_path):
    """Zip all files from source_folder into a single archive"""
    with zipfile.ZipFile(output_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(source_folder):
            for f in files:
                abs_path = os.path.join(root, f)
                rel_path = os.path.relpath(abs_path, start=source_folder)
                z.write(abs_path, rel_path)
    return output_zip_path

def update_dynamodb_status(table_name, run_id, status, output_path=None):
    """Update DynamoDB file_merge_status and related fields for all items in a run"""
    table = dynamodb.Table(table_name)
    timestamp = datetime.now(timezone.utc).isoformat()
    
    log_message('success', f"Updating DynamoDB table {table_name} for run_id: {run_id}, status: {status}")
    
    # Scan for all items with same run_id
    response = table.scan(FilterExpression=Attr("grp_run_id").eq(run_id))
    items = response.get("Items", [])

    for item in items:
        try:
            table.update_item(
                Key={"id": item["id"]},
                UpdateExpression="SET grp_merge_status = :s, file_merge_at = :t, file_merge_output_path = :o",
                ExpressionAttributeValues={
                    ":s": status,
                    ":t": timestamp,
                    ":o": output_path if output_path else "N/A"
                }
            )
            print(f"[OK] Updated Dynamo row id={item['id']} -> {status}")
        except Exception as e:
            print(f"[ERROR] Failed to update row id={item['id']}: {e}", file=sys.stderr)
            log_message('failed', f"Failed to update DynamoDB row id={item['id']}: {e}")

def move_merged_files_to_save(group_items, bucket_name, table_name):
    """
    Move successfully merged files from monitoring/ to monitoring/save/
    """
    log_message('success', f"Starting to move {len(group_items)} merged files to save folder")
    
    table = dynamodb.Table(table_name)
    timestamp = datetime.now(timezone.utc).isoformat()
    moved_count = 0
    failed_count = 0
    
    for item in group_items:
        try:
            source_key = item.get("object_key", "")
            file_name = item.get("file_name", "unknown")
            item_id = item.get("id")
            
            if not source_key or item_id is None:
                print(f"[SKIP] Missing required fields for item: {file_name}")
                failed_count += 1
                continue
            
            if source_key.endswith("/"):
                source_key = source_key + file_name
                print(f"[INFO] Constructed full source_key: {source_key}")
            
            if "/monitoring/" not in source_key or "/monitoring/save/" in source_key:
                print(f"[SKIP] File not in monitoring or already in save: {source_key}")
                continue
            
            try:
                s3.head_object(Bucket=bucket_name, Key=source_key)
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    print(f"[SKIP] File does not exist in S3: {source_key}")
                    failed_count += 1
                    continue
                else:
                    raise
            
            dest_key = source_key.replace("/monitoring/", "/monitoring/save/")
            
            print(f"[INFO] {source_key} to {dest_key}")
            
            s3.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': source_key},
                Key=dest_key
            )
            
            s3.delete_object(Bucket=bucket_name, Key=source_key)
            
            table.update_item(
                Key={"id": item_id},
                UpdateExpression="SET object_key = :new_key, moved_to_save = :flag, moved_at = :t",
                ExpressionAttributeValues={
                    ":new_key": dest_key,
                    ":flag": True,
                    ":t": timestamp
                }
            )
            
            moved_count += 1
            print(f"[OK] Successfully moved {file_name}")
            log_message('success', f"Moved {file_name} from monitoring/ to monitoring/save/")
            
        except Exception as e:
            failed_count += 1
            error_msg = f"Failed to move {item.get('file_name', 'unknown')}: {str(e)}"
            print(f"[ERROR] {error_msg}")
            log_message('failed', error_msg)
            continue
    
    summary = f"Move complete: {moved_count} succeeded, {failed_count} failed out of {len(group_items)} files"
    print(f"[SUMMARY] {summary}")
    log_message('success', summary)

def process_run(group_name, group_items, output_prefix, table_name, grp_initial_status=None, grp_completion_status=None, default_output_prefix="file_monitoring/merged/"):
    global inputFileName
    inputFileName = group_name
    
    log_message('success', f"Starting merge process for group: {group_name}")
    
    if not group_items:
        print(f"[SKIP] No items for run {group_name}")
        log_message('warning', f"No items found for run {group_name}")
        return

    # Validate file counts
    exp = group_items[0].get("file_group_count", 0)
    have = len({it["file_name"] for it in group_items})
    if exp and have < exp:
        msg = f"[WAIT] run_id={group_name} expected={exp}, have={have}. Not merging yet."
        print(msg, file=sys.stderr)
        log_message('warning', msg)
        raise Exception(msg)

    log_message('success', f"File count validation passed. Expected: {exp}, Have: {have}")

    try:
        with tempfile.TemporaryDirectory() as tmp:
            # Creating extract directory
            extract_root = os.path.join(tmp, "extracted")
            os.makedirs(extract_root, exist_ok=True)
            
            log_message('success', f"Processing {len(group_items)} files for merge")
            
            # Validate all files exist before processing
            missing_files = []
            for it in group_items:
                bkt = it["bucket"]
                key = it["object_key"]
                fname = it["file_name"]
                
                if key.endswith("/"):
                    key = key + fname
                
                # Check if file exists
                try:
                    s3.head_object(Bucket=bkt, Key=key)
                    print(f"[OK] File exists: {key}")
                except ClientError as e:
                    if e.response['Error']['Code'] == '404' or e.response['Error']['Code'] == 'NoSuchKey':
                        missing_files.append(f"{fname}")
                        print(f"[ERROR] File not found: {key}")
                        log_message('failed', f"File not found in S3: {key}")
            
            if missing_files:
                error_msg = f"Cannot proceed with merge. Missing {len(missing_files)} file(s): {', '.join(missing_files)}"
                print(f"[ERROR] {error_msg}")
                log_message('failed', error_msg)
                update_dynamodb_status(table_name, group_name, "failed", output_path=f"ERROR: {str(error_msg)[:200]}")
                raise Exception(error_msg)
            
            # Process files one by one
            for it in group_items:
                bkt = it["bucket"]
                key = it["object_key"]
                fname = it["file_name"]
                
                if key.endswith("/"):
                    key = key + fname
                
                dest = os.path.join(tmp, fname)
                print(f"[INFO] Downloading {key} from {bkt}")
                log_message('success', f"Downloading {key} from bucket {bkt}")
                
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                try:
                    s3.download_file(bkt, key, dest)
                    print(f"[OK] Successfully downloaded {fname}")
                except ClientError as e:
                    error_msg = f"Unexpected error downloading {key}: {str(e)}"
                    print(f"[ERROR] {error_msg}")
                    log_message('failed', error_msg)
                    raise
                
                is_zip_name = fname.lower().endswith(".zip")

                try:
                    if is_zip_name or zipfile.is_zipfile(dest):
                        print(f"[INFO] Extracting {fname}")
                        log_message('success', f"Extracting zip file: {fname}")
                        with zipfile.ZipFile(dest, "r") as zf:
                            _ = zf.testzip()  # optional; None means OK
                            
                            # checking if zip is empty and throwing a warning
                            if len(zf.namelist()) == 0:
                                warning_msg = f"Zip file is empty: {fname}"
                                print(f"[WARNING] {warning_msg}")
                                log_message('warning', warning_msg)
                                # Continue processing - empty zip is valid but contains no files
                            else:
                                for member in zf.namelist():
                                    member_clean = member.replace("..", "_").lstrip("/\\").replace("\\", "/")
                                    out_path = os.path.join(extract_root, member_clean)
                                    os.makedirs(os.path.dirname(out_path), exist_ok=True)
                                    with zf.open(member) as src, open(out_path, "wb") as dst:
                                        shutil.copyfileobj(src, dst, length=1024*1024)
                    else:
                        # non-zip inputs (e.g., .TXT) copy through
                        shutil.copy(dest, os.path.join(extract_root, fname))
                        log_message('success', f"Copied non-zip file: {fname}")

                except Exception as e:
                    if is_zip_name:
                        log_message('failed', f"Failed to extract {fname}: {e}")
                        raise Exception(f"Failed to extract {fname}: {e}")
                    # non-zip fallback: copy through
                    shutil.copy(dest, os.path.join(extract_root, fname))

                # DELETE immediately to free space
                try:
                    os.remove(dest)
                    print(f"[INFO] Deleted temp {fname}")
                except Exception:
                    pass

            # Building merged zip
            safe_group = group_name.replace(":", "_").replace("/", "_")
            merged_name = f"{safe_group}.zip"
            merged_local = os.path.join(tmp, merged_name)
            
            log_message('success', f"Creating merged zip file: {merged_name}")
            
            # allowZip64 for large files
            with zipfile.ZipFile(merged_local, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as z:
                for root, _, files in os.walk(extract_root):
                    for f in files:
                        abs_path = os.path.join(root, f)
                        rel_path = os.path.relpath(abs_path, start=extract_root)
                        z.write(abs_path, rel_path)

            target_bucket = group_items[0]["bucket"]
            output_prefix = output_prefix or default_output_prefix
            out_key = output_prefix.rstrip("/") + "/" + merged_name

            print(f"[INFO] Uploading merged file to s3://{target_bucket}/{out_key}")
            log_message('success', f"Uploading merged file to s3://{target_bucket}/{out_key}")
            
            with open(merged_local, "rb") as f:
                s3.upload_fileobj(f, target_bucket, out_key)
            
            print(f"[OK] Uploaded -> s3://{target_bucket}/{out_key}")
            log_message('success', f"Successfully uploaded merged file to s3://{target_bucket}/{out_key}")

        print(f"[DONE] run {group_name} merged -> s3://{target_bucket}/{out_key}")
        log_message('success', f"Merge completed for group {group_name}")

        # Mark ALL items as completed
        update_dynamodb_status(table_name, group_name, grp_completion_status or "completed", out_key)
        
        # Move merged source files to save folder
        print(f"[INFO] Moving merged source files to save folder")
        move_merged_files_to_save(group_items, target_bucket, table_name)
        log_message('success', f"Completed moving merged files to save folder for group {group_name}")

        return {
            "run_id": group_name,
            "file_merge_status": "completed",
            "file_merge_at": datetime.now(timezone.utc).isoformat(),
            "file_merge_output_path": out_key,
            "bucket": target_bucket
        }
    except Exception as e:
        print(f"[ERROR] Merge process failed: {e}", file=sys.stderr)
        log_message('failed', f"Merge process failed for group {group_name}: {e}")
        update_dynamodb_status(table_name, group_name, "failed", output_path=f"ERROR: {str(e)[:200]}")
        raise

def main():
    log_message('success', "File Merger job started")
    args = parse_args()

    try:
        result = process_run(args.group_name, args.group_items, args.output_prefix, args.file_monitoring_table, args.grp_initial_status, args.grp_completion_status)
        if result:
            print(json.dumps(result))
            log_message('success', f"File Merger job completed successfully for group: {args.group_name}")
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        log_message('failed', f"File Merger job failed: {e}")
        raise

if __name__ == "__main__":
    main()