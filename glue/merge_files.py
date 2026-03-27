import boto3
import os
import re
import json
import sys
import splunk
from awsglue.utils import getResolvedOptions
import traceback
from datetime import datetime, timezone
from botocore.exceptions import ClientError
import pandas as pd
import io

# -------------------------------------------------------------------------
# Initialize S3 client and account info
# -------------------------------------------------------------------------
s3 = boto3.client("s3")
AWS_ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

# -------------------------------------------------------------------------
# Logging and notification helpers (unchanged)
# -------------------------------------------------------------------------
def get_job_name():
    job_name = "merge_files"
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
    if not email:
        return False
    return re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", email) is not None


def log_event(status: str, message: str):
    event_data = {
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "clientName": os.getenv("clientName", ""),
        "clientID": os.getenv("clientID", ""),
        "applicationName": os.getenv("applicationName", ""),
        "step_function": "merge_files",
        "component": "glue",
        "job_name": get_job_name(),
        "job_run_id": get_run_id(),
        "execution_name": os.getenv("execution_name", ""),
        "execution_id": os.getenv("execution_id", ""),
        "status": status,
        "message": message,
        "aws_account_id": AWS_ACCOUNT_ID,
    }

    print(json.dumps(event_data))
    try:
        splunk.log_message(event_data, get_run_id())
    except Exception as e:
        print(f"[ERROR] Failed to write log to Splunk: {e}")

    # Send email on failure if teamsId provided
    if status == "FAILED" and is_valid_email(os.getenv("teamsId")):
        glue_link = f"https://us-east-1.console.aws.amazon.com/gluestudio/home?region=us-east-1#/editor/job/{event_data['job_name']}/runs"
        stepfn_link = f"https://us-east-1.console.aws.amazon.com/states/home?region=us-east-1#/v2/executions/details/{event_data['execution_id']}"
        subject = f"[ALERT] Glue job Failure - {event_data['job_name']} ({status})"
        body_text = (
            f"Glue job failed.\n\n"
            f"Event Time: {event_data['event_time']}\n"
            f"Client: {event_data['clientName']} ({event_data['clientID']})\n"
            f"Job: {event_data['job_name']}\n"
            f"Status: {status}\n"
            f"Message: {message}\n"
            f"{glue_link}\n{stepfn_link}\n"
        )
        sender = os.getenv("ALERT_SENDER_EMAIL", "noreply@broadridge.com")
        ses_client = boto3.client("ses", region_name="us-east-1")
        try:
            response = ses_client.send_email(
                Destination={"ToAddresses": [os.getenv("teamsId")]},
                Message={"Body": {"Text": {"Data": body_text}},
                         "Subject": {"Data": subject}},
                Source=sender,
            )
            print(f"Email sent! sender:{sender}::Recipient:{os.getenv('teamsId')}")
            print("SES send_email full response:\n", json.dumps(response, indent=2))
        except ClientError as e:
            print(f"[ERROR] Failed to send email: {e}")


def set_job_params_as_env_vars():
    messages = []
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i+1]
            os.environ[key] = value
            messages.append(f"{key}={value}")

    log_event("INFO", f"Env vars set from job params: {', '.join(messages)}")

# -------------------------------------------------------------------------
# Merge helper functions
# -------------------------------------------------------------------------
def list_s3_objects(bucket, prefix):
    """
    Yield dicts of {'Key': str, 'LastModified': datetime}
    """
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            yield {"Key": item["Key"], "LastModified": item["LastModified"]}


def find_latest_per_type(file_entries, file_types, ext_type):
    """
    Return the latest file (by LastModified) for each file_type where both
    file_type and ext_type appear in the S3 key name.
    """
    log_event("INFO", f"Finding latest files for types: {file_types} with ext_type: {ext_type}")
    latest_map = {}
    for entry in file_entries:
        key = entry.get("Key", "")
        last_modified = entry.get("LastModified")
        if not key or not last_modified:
            continue
        for ftype in file_types:
            if ftype in key and key.endswith(ext_type):
                prev = latest_map.get(ftype)
                if not prev or last_modified > prev["LastModified"]:
                    latest_map[ftype] = entry
    
    log_event("INFO", f"Latest files found for types: {list(latest_map.keys())}, details: {latest_map}, with ext_type: {ext_type}")
    return {ft: v["Key"] for ft, v in latest_map.items()}


def s3_to_dataframe(bucket, key):
    """
    Read an S3 CSV safely for large files by streaming in chunks.
    Prevents Out-of-Memory errors.
    """
    log_event("INFO", f"Reading large CSV from s3://{bucket}/{key} with chunking")
    obj = s3.get_object(Bucket=bucket, Key=key)
    chunks = pd.read_csv(obj["Body"], chunksize=500000, low_memory=False, dtype=str)
    df = pd.concat(chunks, ignore_index=True)
    return df


def upload_large_csv(df, bucket, key):
    """Upload a DataFrame as a CSV using multipart upload; works with >5 GB files."""
    with io.BytesIO() as buf:
        df.to_csv(buf, index=False)
        buf.seek(0)
        s3.upload_fileobj(buf, bucket, key)

    log_event("INFO", f"Uploaded merged file s3://{bucket}/{key}")


def move_to_archive(bucket, key, archive_prefix):

    filename = key.rsplit("/", 1)[-1]
    dest_key = f"{archive_prefix.rstrip('/')}/{filename}"
    s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=dest_key)
    s3.delete_object(Bucket=bucket, Key=key)

    log_event("INFO", f"Archived {key} -> {dest_key}")


def process_merge_task(mergeInfo, bucket):
    try:
        for task in mergeInfo:
            input_prefix = task["input_folder"].lstrip("/")
            dest_prefix  = task["destination_folder"].lstrip("/")
            arch_prefix  = task["archive_folder"].lstrip("/")

            all_entries = list(list_s3_objects(bucket, input_prefix))

            log_event("INFO", f"Found {len(all_entries)} files in input folder s3://{bucket}/{input_prefix}, looking for task = {task}, all_entries = {all_entries}")
            latest_map = find_latest_per_type(all_entries, task["file_types"], task["ext_type"])

            missing = [ft for ft in task["file_types"] if ft not in latest_map]

            if missing:
                msg = f"Skipping merge for {task['output_file']} - missing {missing}, for task = {task}"
                log_event("INFO", msg)
                continue

            # --- collect stats per file ---
            file_stats = []
            first_chunk = True
            output_file = task["output_file"]
            if "<TS>" in output_file:
                ts_format = task.get("ts_format", "%m%d%Y%H%M%S")
                ts_str = datetime.now().strftime(ts_format)
                output_file = output_file.replace("<TS>", ts_str)
            output_key = f"{dest_prefix.rstrip('/')}/{output_file}"

            # --- write output (streaming) ---
            buf = io.BytesIO()
            total_written = 0

            for ftype, key in latest_map.items():
                obj = s3.get_object(Bucket=bucket, Key=key)
                reader = pd.read_csv(obj["Body"], chunksize=500000, low_memory=False, dtype=str)
                for chunk in reader:
                    data = chunk.to_csv(index=False, header=first_chunk).encode("utf-8")
                    buf.write(data)
                    total_written += len(data)
                    first_chunk = False
                header_cols = list(chunk.columns)
                num_rows = len(chunk)
                file_stats.append({
                    "file_type": ftype,
                    "key": key,
                    "rows": num_rows,
                    "columns": len(header_cols),
                    "header": header_cols
                })

            # --- upload depending on total size ---
            if total_written < 5 * 1024 * 1024:
                buf.seek(0)
                s3.put_object(Bucket=bucket, Key=output_key, Body=buf)
                log_event("INFO", f"Uploaded small merged file s3://{bucket}/{output_key}")
            else:
                buf.seek(0)
                upload = s3.create_multipart_upload(Bucket=bucket, Key=output_key)
                upload_id = upload["UploadId"]
                parts, part_num = [], 1
                try:
                    while True:
                        part_data = buf.read(8 * 1024 * 1024)
                        if not part_data:
                            break
                        part = s3.upload_part(
                            Bucket=bucket,
                            Key=output_key,
                            PartNumber=part_num,
                            UploadId=upload_id,
                            Body=io.BytesIO(part_data),
                        )
                        parts.append({"ETag": part["ETag"], "PartNumber": part_num})
                        part_num += 1
                    s3.complete_multipart_upload(
                        Bucket=bucket,
                        Key=output_key,
                        MultipartUpload={"Parts": parts},
                        UploadId=upload_id,
                    )
                    log_event("INFO", f"Uploaded large merged file s3://{bucket}/{output_key}")
                except Exception as e:
                    s3.abort_multipart_upload(Bucket=bucket, Key=output_key, UploadId=upload_id)
                    raise

            # --- archive sources ---
            for key in latest_map.values():
                move_to_archive(bucket, key, arch_prefix)

            # --- summary logging ---
            msg = (
                f"Merged {len(file_stats)} files into {task['output_file']} - "
                f"{len(file_stats)} source files processed, "
                f"Output at s3://{bucket}/{output_key}"
            )
            log_event("INFO", msg)

    except Exception as err:
        log_event("FAILED", f"Merge processing failed: {err}")
        raise

# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
def main():
    try:
        set_job_params_as_env_vars()
        log_event("STARTED", f"glue_merge_files job started")

        bucket = os.getenv("bucket")
        objectKey = os.getenv("objectKey", " ")
        fileId = os.getenv("fileId")
        key = os.getenv("key")
        file_name = os.getenv("file_name")
        sfInputFile = os.getenv("sfInputFile")
        mergeInfo = json.loads(os.getenv("mergeInfo"))

        log_event("INFO", f"Job parameters: mergeInfo={mergeInfo}, bucket={bucket}, objectKey={objectKey}, "
                          f"fileId={fileId}, key={key}, file_name={file_name}, sfInputFile={sfInputFile}")


        process_merge_task(mergeInfo, bucket)

        log_event("ENDED", f"glue_merge_files job completed successfully for mergeInfo={mergeInfo}")
    except Exception as e:
        log_event("FAILED", f"glue_merge_files job failed: Exception occurred={str(e)}::traceback={traceback.format_exc()}")
        raise


if __name__ == "__main__":
    main()