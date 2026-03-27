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
    job_name = "merge_insert_files"
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

    log_event("INFO", f"Reading simple text file from s3://{bucket}/{key}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8").strip().splitlines()

    # Split on whitespace – preserves account and optional code
    rows = [line.strip().split() for line in body if line.strip()]

    # If single‑column
    if all(len(r) == 1 for r in rows):
        df = pd.DataFrame(rows, columns=["InsertIdentifierValue"])
    else:
        # Normalize: ensure each row has 2 elements (Account, Code)
        normalized = [(r[0], r[1] if len(r) > 1 else "") for r in rows]
        df = pd.DataFrame(normalized, columns=["InsertIdentifierValue", "Code"])
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


def process_merge_task(mergeInsertInfo, bucket):
    try:
        for task in mergeInsertInfo:
            input_prefix = task["input_folder"].lstrip("/")
            dest_prefix  = task["destination_folder"].lstrip("/")
            arch_prefix  = task["archive_folder"].lstrip("/")

            all_entries = list(list_s3_objects(bucket, input_prefix))

            log_event("INFO", f"Found {len(all_entries)} files in input folder s3://{bucket}/{input_prefix}, looking for task = {task}, all_entries = {all_entries}")
            latest_map = find_latest_per_type(all_entries, task["file_types"], task["ext_type"])
            print(f"Latest map for task {task}: {latest_map}")

            missing = [ft for ft in task["file_types"] if ft not in latest_map]

            if missing:
                msg = f"Skipping merge for {task['output_file']} - missing {missing}, for task = {task}"
                log_event("INFO", msg)
                continue

            # --- collect stats per file ---
            output_file = task["output_file"]
            if "<TS>" in output_file:
                ts_format = task.get("ts_format", "%Y-%m-%dT%H%M%S")
                ts_str = datetime.now().strftime(ts_format)
                output_file = output_file.replace("<TS>", ts_str)
                print(f"Generated timestamped output file name: {output_file}")

            output_key = f"{dest_prefix.rstrip('/')}/{output_file}"
            print(f"Processing merge for task {task}, output_key = {output_key}")

            buf = io.BytesIO()
            total_written = 0
            file_stats = []

            # ------------------------------------------------------------------
            # MERGE LOGIC
            # ------------------------------------------------------------------
            insert_file_key = None
            insert_list_key = None
            for ft, k in latest_map.items():
                if "INSERT_FILE" in ft:
                    insert_file_key = k
                elif "INSERT_LIST" in ft:
                    insert_list_key = k

            print(f"Insert file key: {insert_file_key}, Insert list key: {insert_list_key}")

            if not insert_file_key and not insert_list_key:
                log_event("INFO", "No INSERT_FILE or INSERT_LIST files found — skipping merge.")
                continue

            # --- Read both files as DataFrames ---
            df_file = pd.DataFrame(columns=["InsertIdentifierValue"])
            if insert_file_key:
                print(f"Reading insert file from s3://{bucket}/{insert_file_key}")
                df1 = s3_to_dataframe(bucket, insert_file_key)
                # handle case of one or multiple columns
                if df1.shape[1] >= 1:
                    df1 = df1.iloc[:, [0]]

                df1.columns = ["InsertIdentifierValue"]
                df_file = df1.astype(str).drop_duplicates()
            
            print(f"DataFrame from insert file:\n{df_file.head()}")
            print(f"Insert file DataFrame columns: {df_file.columns.tolist()}")
            print(f"Insert file DataFrame dtypes:\n{df_file.dtypes}")

            df_list = pd.DataFrame(columns=["InsertIdentifierValue", "Code"])
            if insert_list_key:
                print(f"Reading insert list from s3://{bucket}/{insert_list_key}")
                df2 = s3_to_dataframe(bucket, insert_list_key)
                if df2.shape[1] >= 2:
                    df2 = df2.iloc[:, [0, 1]]
                    df2.columns = ["InsertIdentifierValue", "Code"]
                else:
                    df2.columns = ["InsertIdentifierValue", "Code"]
                df_list = df2.astype(str).drop_duplicates()

            print(f"DataFrame from insert list:\n{df_list.head()}")
            print(f"Insert list DataFrame columns: {df_list.columns.tolist()}")
            print(f"Insert list DataFrame dtypes:\n{df_list.dtypes}")


            # --- Combine all unique accounts ---
            combined_series = pd.concat(
                [df_file["InsertIdentifierValue"], df_list["InsertIdentifierValue"]],
                ignore_index=True
            ).drop_duplicates()

            all_vals = pd.DataFrame({"InsertIdentifierValue": combined_series})
            all_vals["InsertIdentifierType"] = "Account"

            # initialize all bins Bin1-Bin6 = N
            for b in range(1, 7):
                all_vals[f"Bin{b}"] = "N"

            # (1) every account in file1 -> Bin6 = Y
            if not df_file.empty:
                all_vals.loc[
                    all_vals["InsertIdentifierValue"].isin(df_file["InsertIdentifierValue"]),
                    "Bin6"
                ] = "Y"

            # (2) apply mappings for file2 (code column)
            if not df_list.empty:
                for _, row in df_list.iterrows():
                    acc = row["InsertIdentifierValue"]
                    code = str(row["Code"]).strip().upper()

                    if acc.upper().startswith("GLOBAL"):
                        # Global row does not come from Account list.
                        # Update type and value to 'Global'
                        all_vals.loc[all_vals["InsertIdentifierValue"] == acc, "InsertIdentifierType"] = "Global"
                        all_vals.loc[all_vals["InsertIdentifierValue"] == acc, "InsertIdentifierValue"] = "Global"
                        all_vals.loc[all_vals["InsertIdentifierValue"] == "Global", "Bin5"] = "Y"

                    elif acc.upper().startswith("PRODUCT"):
                        # Product row should contain type/value as GWP
                        all_vals.loc[all_vals["InsertIdentifierValue"] == acc, "InsertIdentifierType"] = "Product_Class_Detail"
                        all_vals.loc[all_vals["InsertIdentifierValue"] == acc, "InsertIdentifierValue"] = "GWP"
                        all_vals.loc[all_vals["InsertIdentifierValue"] == "GWP", "Bin2"] = "Y"

                    elif code == "A":
                        all_vals.loc[all_vals["InsertIdentifierValue"] == acc, "Bin4"] = "Y"

                    elif code == "B":
                        all_vals.loc[all_vals["InsertIdentifierValue"] == acc, "Bin3"] = "Y"

                    elif code == "C":
                        all_vals.loc[all_vals["InsertIdentifierValue"] == acc, ["Bin3", "Bin4"]] = "Y"

            # --- Column ordering and write ---
            out_cols = [
                "InsertIdentifierType",
                "InsertIdentifierValue",
                "Bin1", "Bin2", "Bin3", "Bin4", "Bin5", "Bin6"
            ]
            all_vals[out_cols].to_csv(buf, index=False, header=True)
            total_written = buf.tell()
            buf.seek(0)

            file_stats.append({
                "insert_file_key": insert_file_key,
                "insert_list_key": insert_list_key,
                "output_records": len(all_vals)
            })
            # ------------------------------------------------------------------

            # --- upload depending on total size ---
            if total_written < 5 * 1024 * 1024:
                buf.seek(0)
                s3.put_object(Bucket=bucket, Key=output_key, Body=buf)
                log_event("INFO", f"Uploaded merged file s3://{bucket}/{output_key}")
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
                    log_event("FAILED", f"Multipart upload failed: {str(e)}")
                    raise

            # --- archive sources ---
            for key in latest_map.values():
                move_to_archive(bucket, key, arch_prefix)

            log_event(
                "",
                f"Merged {len(file_stats)} tasks into {output_file}, records={len(all_vals)}, "
                f"output s3://{bucket}/{output_key}"
            )

    except Exception as err:
        log_event("FAILED", f"Merge processing failed: {err}\n{traceback.format_exc()}")
        raise

# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
def main():
    try:
        set_job_params_as_env_vars()
        log_event("STARTED", f"glue_merge_insert_files job started")

        bucket = os.getenv("bucket")
        objectKey = os.getenv("objectKey", " ")
        fileId = os.getenv("fileId")
        key = os.getenv("key")
        file_name = os.getenv("file_name")
        sfInputFile = os.getenv("sfInputFile")
        mergeInsertInfo = json.loads(os.getenv("mergeInsertInfo"))

        log_event("INFO", f"Job parameters: mergeInsertInfo={mergeInsertInfo}, bucket={bucket}, objectKey={objectKey}, "
                          f"fileId={fileId}, key={key}, file_name={file_name}, sfInputFile={sfInputFile}")


        process_merge_task(mergeInsertInfo, bucket)

        log_event("ENDED", f"glue_merge_insert_files job completed successfully for mergeInsertInfo={mergeInsertInfo}")
    except Exception as e:
        log_event("FAILED", f"glue_merge_insert_files job failed: Exception occurred={str(e)}::traceback={traceback.format_exc()}")
        raise


if __name__ == "__main__":
    main()