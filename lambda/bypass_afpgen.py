import os
import json
import re
import zipfile
from datetime import datetime
from pathlib import PurePosixPath

import boto3
from botocore.exceptions import ClientError

# AWS clients
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client("s3")

VALID_CYCLES = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ12345")


# ---------------------------------------------------------
# Utility
# ---------------------------------------------------------
def parse_s3_uri(uri: str):
    m = re.match(r"s3://([^/]+)/(.+)", uri)
    if not m:
        raise ValueError(f"Invalid S3 URI: {uri}")
    return m.group(1), m.group(2)


def upload_to_s3(local_path: str, bucket: str, key: str) -> str:
    try:
        s3.upload_file(local_path, bucket, key)
        return f"s3://{bucket}/{key}"
    except ClientError as e:
        print("Upload failed:", e)
        return ""


def zip_s3_files(s3_uris, target_bucket, target_key, download_dir="/tmp"):
    os.makedirs(download_dir, exist_ok=True)
    zip_path = os.path.join(download_dir, os.path.basename(target_key))

    downloaded = []

    for uri in (s3_uris if isinstance(s3_uris, list) else [s3_uris]):
        try:
            b, k = parse_s3_uri(uri)
            local_file = os.path.join(download_dir, os.path.basename(k))
            s3.download_file(b, k, local_file)
            downloaded.append(local_file)
        except Exception as e:
            print(f"Download failed for {uri}: {e}")

    if not downloaded:
        return ""

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for f in downloaded:
            z.write(f, os.path.basename(f))

    uploaded_uri = upload_to_s3(zip_path, target_bucket, target_key)

    # cleanup
    for f in downloaded:
        os.remove(f)
    os.remove(zip_path)

    return uploaded_uri


# ---------------------------------------------------------
# CORP Logic
# ---------------------------------------------------------
def corp_rd_cyc_handler(client_name, file_name, corp_table, cycle_table):
    table = dynamodb.Table(corp_table)

    response = table.get_item(Key={'clientName': client_name})
    corp_cfg = json.loads(response["Item"]["corpConfig"])

    file_cfg = next(
        (f for f in corp_cfg.get("filesToProcess", [])
         if re.match(f.get("FILENAME", ""), file_name)),
        {}
    )

    return get_corp_rundate_cycle(client_name, file_cfg, file_name, cycle_table)


def get_corp_rundate_cycle(client_name, cfg, file_name, cycle_table):
    run_type = "TEST" if "tst" in file_name.lower() else "PROD"
    rt_info = cfg.get("RUNTYPE", {}).get(run_type, {})

    corp = rt_info.get("CORP")

    rundate = (
        get_current_rundate()
        if cfg.get("RUNDATE") == "Current_date"
        else get_file_rundate(file_name)
    )

    cycle_cfg = cfg.get("CYCLE")

    if cycle_cfg == "Current_date":
        cycle = get_cycle_from_rundate(rundate)

    elif cycle_cfg == "Increment":
        rollover = rt_info.get("ROLLOVER_CORP_APPLICABLE")
        rollover_frequency = rt_info.get("ROLLOVER_FREQUENCY" , "Daily")
        print("Rollover corp: ", rollover , "Frequency: ", rollover_frequency)
        corp, cycle = get_incremented_cycle(
            client_name, corp, rollover, rollover_frequency, rundate, cycle_table
        )

    else:
        cycle = "A"

    return {
        "CORP": corp,
        "RUNDATE": rundate,
        "CYCLE": cycle,
        "OUTPUT_FILE_PREFIX": cfg.get("OUTPUT_FILE_PREFIX"),
        "OUTPUT_FILE_SUFFIX": cfg.get("OUTPUT_FILE_SUFFIX"),
    }


def get_incremented_cycle(client_name, corp, rollover, rollover_frequency, rundate, cycle_table):
    table = dynamodb.Table(cycle_table)
    rd_key = rundate[:2] + rundate[4:]

    try:
        resp = table.get_item(Key={'clientName': client_name})
        cycle_data = json.loads(resp["Item"].get(rd_key, "{}"))
    except Exception:
        cycle_data = update_new_month(client_name, rd_key, corp, cycle_table)

    current = cycle_data.get(corp, {}).get("CYCLE", "A")
    last_cycle = False
    new_rc_corp = None
    if rollover and rollover != "false":
        if rollover_frequency == "Daily" and current == "5":
            for rc in rollover.split(","):
                c = cycle_data.get(rc, {}).get("CYCLE")
                if c is None:
                    last_cycle = True
                    new_rc_corp = rc
                elif c and c != "5":
                    corp = rc
                    current = c
                    break
                elif c and c == "5":
                    corp = rc
                    last_cycle = True
                    continue
        elif rollover_frequency == "Monthly" and current == "A":
            for rc in rollover.split(","):
                c = cycle_data.get(rc, {}).get("CYCLE")
                if c is None:
                    last_cycle = True
                    new_rc_corp = rc
                elif c and c != "A":
                    corp = rc
                    current = c
                    break
                elif c and c == "A":
                    corp = rc
                    last_cycle = True
                    continue
          
    if(last_cycle):
        new_cycle = "A"
        cycle_data[new_rc_corp] = {"CYCLE": new_cycle} 
    else:
        new_cycle = get_next_cycle(current)
        cycle_data[corp] = {"CYCLE": new_cycle} 
    
    table.update_item(
        Key={'clientName': client_name},
        UpdateExpression="SET #r = :v",
        ExpressionAttributeNames={"#r": rd_key},
        ExpressionAttributeValues={":v": json.dumps(cycle_data)},
    )

    return corp, current


def get_next_cycle(c):
    try:
        return VALID_CYCLES[VALID_CYCLES.index(c) + 1]
    except:
        return "A"


def update_new_month(client_name, rd_key, corp, cycle_table):
    table = dynamodb.Table(cycle_table)
    data = {corp: {"CYCLE": "A"}}

    table.update_item(
        Key={'clientName': client_name},
        UpdateExpression="SET #r = :v",
        ExpressionAttributeNames={"#r": rd_key},
        ExpressionAttributeValues={":v": json.dumps(data)},
    )
    return data


def get_cycle_from_rundate(rundate):
    day = int(rundate[2:4])
    return VALID_CYCLES[day - 1]


def get_current_rundate():
    return datetime.now().strftime("%m%d%Y")


def get_file_rundate(file_name):
    for pat in (r"\d{8}", r"\d{6}"):
        for match in re.findall(pat, file_name):
            for fmt in ("%m%d%Y", "%d%m%Y", "%Y%m%d", "%y%m%d"):
                try:
                    return datetime.strptime(match, fmt).strftime("%m%d%Y")
                except ValueError:
                    pass
    raise ValueError("No valid date found in filename")


# ---------------------------------------------------------
# AFPGEN BYPASS
# ---------------------------------------------------------
def bypass_afpgen(crc, file_path, file_name, event):
    parts = file_path.split("/")
    idx = parts.index("from_smartcomm")
    prefix = "/".join(parts[:idx]) + "/"

    out = crc["OUTPUT_FILE_PREFIX"]

    for s in crc["OUTPUT_FILE_SUFFIX"]:
        if s in file_name:
            out += f".{crc['CORP']}.{crc['RUNDATE']}.{crc['CYCLE']}{s}.zip"
            break

    bucket =  os.getenv("bucket")
    txn = os.getenv("transactionId")

    tkey = f"{prefix}from_bypass_afpgen/wip/{txn}/{out}"

    source_uri = f"s3://{bucket}/{os.getenv('key')}"

    result = zip_s3_files([source_uri], bucket, tkey)
    if result:
        print("ZIP created:", result)


def set_event_env_vars(event: dict) -> None:
    for k, v in event.items():
        os.environ[k] = str(v)


# ---------------------------------------------------------
# LAMBDA HANDLER  (NO os.getenv usage)
# ---------------------------------------------------------
def lambda_handler(event, context):

    try:
        set_event_env_vars(event)
        # Required event params
        client =  os.getenv("clientName")
        key =  os.getenv("key")
        corp_table =  os.getenv("corp_table_name")
        cycle_table =  os.getenv("cycle_table_name")

        # Add cycle table to config lookup context
        # event["cycle_table"] = cycle_table
        # os.environ["cyclle_table"] =cycle_table

        file_name = key.split("/")[-1]
        print("Processing:", file_name)

        crc_data = corp_rd_cyc_handler(client, file_name, corp_table, cycle_table)

        bypass_afpgen(crc_data, key, file_name, event)

    except KeyError as e:
        raise ValueError(f"Missing required event field: {str(e)}")

    except Exception as e:
        print("ERROR:", e)
        raise

    return {
        "statusCode": 200,
        "body": json.dumps("Lambda executed successfully")
    }
