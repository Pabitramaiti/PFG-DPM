import os
import sys
import json
import time
import re
from asyncio.log import logger
from datetime import datetime
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# AWS Clients
dynamodb = boto3.resource('dynamodb')
ecs_client = boto3.client("ecs")

POLL_INTERVAL = 30  # seconds
corp_config = {}
metadata = None
VALID_CYCLES = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ12345")
bypass_afpgen = False

s3 = boto3.client("s3")



def parse_s3_uri(s3_uri: str):
    """
    Parse an S3 URI like 's3://bucket/key/path.txt' into (bucket, key).
    """
    match = re.match(r"s3://([^/]+)/(.+)", s3_uri)
    if not match:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    return match.group(1), match.group(2)


def upload_to_s3(file_path: str, bucket_name: str, key: str) -> str:
    """
    Upload a local file to S3 and return the uploaded S3 path.
    """
    try:
        s3.upload_file(file_path, bucket_name, key)
        s3_path = f"s3://{bucket_name}/{key}"
        print(f"✅ Uploaded ZIP → {s3_path}")
        return s3_path
    except ClientError as e:
        print(f"❌ Upload failed: {e}")
        return ""


# ---------------- Core Method ---------------- #

def zip_s3_files(s3_uris: list[str], target_bucket: str, target_key: str, download_dir: str = "/tmp") -> str:
    """
    Download specific S3 files (provided as URIs), zip them, and upload the zip to S3.

    :param s3_uris: List of S3 URIs to download (e.g., ['s3://bucket/file1.txt', 's3://bucket/file2.csv'])
    :param target_bucket: Target S3 bucket for the output ZIP
    :param target_key: Key (path) for the uploaded ZIP file (e.g., 'archives/mydata.zip')
    :param download_dir: Local directory to temporarily store files
    :return: The uploaded S3 URI of the ZIP file
    """
    os.makedirs(download_dir, exist_ok=True)
    zip_path = os.path.join(download_dir, os.path.basename(target_key))

    downloaded_files = []

    # Step 1: Download each S3 file
    for uri in s3_uris:
        try:
            bucket, key = parse_s3_uri(uri)
            local_path = os.path.join(download_dir, os.path.basename(key))
            print(f"⬇️ Downloading {uri}")
            s3.download_file(bucket, key, local_path)
            downloaded_files.append(local_path)
        except (ClientError, ValueError) as e:
            print(f"❌ Failed to download {uri}: {e}")

    if not downloaded_files:
        print("⚠️ No files were downloaded. Exiting.")
        return ""

    # Step 2: Create ZIP
    print(f"📦 Creating ZIP: {zip_path}")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for file_path in downloaded_files:
            arcname = os.path.basename(file_path)
            zipf.write(file_path, arcname)
            print(f"✅ Added to ZIP: {arcname}")

    # Step 3: Upload ZIP back to S3
    uploaded_uri = upload_to_s3(zip_path, target_bucket, target_key)

    # Step 4: Optional cleanup
    for file_path in downloaded_files:
        os.remove(file_path)
    os.remove(zip_path)

    return uploaded_uri


def corp_rd_cyc_handler(client_name, file_name):
    global corp_config
    print(f"Glue triggered with file_name {file_name}: and client_name : {client_name} ")

    if not client_name:
        raise ValueError("Missing client_name in event.")

    config_table = dynamodb.Table(os.getenv("config_table_name"))

    try:
        response = config_table.get_item(Key={'clientName': client_name})
        corp_config_raw = response['Item'].get('corpConfig')
        corp_config = get_sf_info(corp_config_raw, file_name)
        print(corp_config)
    except Exception as e:
        raise ValueError(f"Failed to fetch or parse corpConfig for {client_name}: {str(e)}")

    return get_corp_rundate_cycle_by_keys(client_name, corp_config, file_name)


def get_sf_info(corp_config_raw, file_name):
    print("In SF_INFO ")
    try:
        corp_config = json.loads(corp_config_raw)
        files_to_process_sf = corp_config.get('filesToProcess', [])
        config_data = "";
        for file_config in files_to_process_sf:
            filename_regex = file_config.get('FILENAME', '')
            if re.match(filename_regex, file_name):
                print("File matched " + file_name + " with regex " + filename_regex)
                config_data = file_config
                break

        return config_data
    except Exception as e:
        return e


def get_corp_rundate_cycle_by_keys(client_name, item, file_name):
  
    try:
        if "tst" in file_name:
            run_type = "TEST"
        else:
            run_type = "PROD"

        runtype_info = item.get('RUNTYPE', {}).get(run_type, {})
        corp = runtype_info.get('CORP')

        rundate_type = item.get('RUNDATE', '')
        rundate = get_current_rundate() if rundate_type == 'Current_date' else get_file_rundate(file_name)

        cycle_type = item.get('CYCLE', '')
        if cycle_type == 'Current_date':
            cycle = get_cycle_from_rundate(rundate)
        elif cycle_type == 'Increment':
            rollover = runtype_info.get('ROLLOVER_CORP_APPLICABLE')
            corp, cycle = get_incremented_cycle(client_name, corp, rollover, rundate)
        if "BYPASS_AFPGEN" in item and item.get("BYPASS_AFPGEN"):
            bypass_afpgen = True

        return {"CORP": corp, "RUNDATE": rundate, "CYCLE": cycle, "OUTPUT_FILE_PREFIX": item.get("OUTPUT_FILE_PREFIX"),
                "OUTPUT_FILE_SUFFIX": item.get("OUTPUT_FILE_SUFFIX")}

    except Exception as e:
        # Get key from the corpConfig
        raise ValueError("Failed to get run info by file: " + file_name)


def get_incremented_cycle(client_name, corp, rollover_applicable, rundate):
    table = dynamodb.Table(str(os.getenv("CYCLE_TABLE_NAME")).strip())
    rundate_key = rundate[:2] + rundate[4:]

    try:
        response = table.get_item(Key={'clientName': client_name})
        cycle_data = json.loads(response['Item'].get(rundate_key, '{}'))
    except Exception:
        cycle_data = update_db_for_new_month(client_name, rundate_key, corp)

    cycle = cycle_data.get(corp, {}).get('CYCLE', 'A')

    if cycle == 'Z' and rollover_applicable != 'false':
        for ro_corp in rollover_applicable.split(','):
            cycle = cycle_data.get(ro_corp, {}).get('CYCLE')
            if cycle and cycle != 'Z':
                corp = ro_corp
                break
        else:
            cycle = '1'

    updated_cycle = get_next_cycle(cycle)

    # Update DB
    cycle_data[corp] = {'CYCLE': updated_cycle}
    table.update_item(
        Key={'clientName': client_name},
        UpdateExpression='SET #r = :val',
        ExpressionAttributeNames={'#r': rundate_key},
        ExpressionAttributeValues={':val': json.dumps(cycle_data)}
    )

    return corp, cycle


def get_next_cycle(current):
    try:
        idx = VALID_CYCLES.index(current)
        return VALID_CYCLES[idx + 1]
    except (ValueError, IndexError):
        return '1'


def update_db_for_new_month(client_name, rundate_key, corp):
    table = dynamodb.Table(str(os.getenv("CYCLE_TABLE_NAME")).strip())
    cycle_data = {corp: {'CYCLE': 'A'}}
    table.update_item(
        Key={'clientName': client_name},
        UpdateExpression='SET #r = :val',
        ExpressionAttributeNames={'#r': rundate_key},
        ExpressionAttributeValues={':val': json.dumps(cycle_data)}
    )
    return cycle_data


def get_cycle_from_rundate(rundate):
    try:
        day = int(rundate[2:4])
        return VALID_CYCLES[day - 1]
    except Exception as e:
        raise ValueError(f"Invalid rundate for cycle extraction: {rundate}")


def get_current_rundate():
    return datetime.now().strftime("%m%d%Y")


def get_file_rundate(file_name):
    date_patterns = [r"\d{8}", r"\d{6}"]

    for pattern in date_patterns:
        matches = re.findall(pattern, file_name)
        for match in matches:
            for fmt in ("%m%d%Y", "%d%m%Y", "%Y%m%d", "%y%m%d"):
                try:
                    date_obj = datetime.strptime(match, fmt)
                    return date_obj.strftime("%m%d%Y")
                except ValueError:
                    continue

    raise ValueError("No valid date found in file name")


def remove_last_s3_dir(s3_path):
    if s3_path.endswith("/"):
        s3_path = s3_path[:-1]
    path_no_scheme = s3_path.replace("s3://", "")
    bucket, *key_parts = path_no_scheme.split("/")
    key = PurePosixPath("/".join(key_parts)).parent
    return f"s3://{bucket}/{key}"


def wait_for_task_completion(ecs_client, cluster, task_arn, timeout=3600):
    """Poll ECS task status until completion."""
    start = time.time()
    while True:
        try:
            response = ecs_client.describe_tasks(cluster=cluster, tasks=[task_arn])

            failures = response.get("failures") or []
            if failures:
                raise RuntimeError(f"describe_tasks failures for {task_arn}: {failures}")

            tasks = response.get("tasks", [])
            if not tasks:
                raise RuntimeError(f"No task info found for {task_arn}")

            task = tasks[0]
            status = task.get("lastStatus")

            if status == "STOPPED":
                reason = task.get("stoppedReason", "Unknown")
                containers = task.get("containers", [])

                for container in containers:
                    exit_code = container.get("exitCode")  # don't default to 0
                    if exit_code not in (0,):
                        logger.error(f"Container failed: exitCode={exit_code}, reason={reason}")
                        raise RuntimeError(f"Task failed with exit code {exit_code}: {reason}")
                break

            if time.time() - start > timeout:
                raise TimeoutError(f"ECS task timeout after {timeout}s: {task_arn}")
            print("response :=>", response)
            time.sleep(POLL_INTERVAL)

        except ClientError as e:
            logger.error(f"Error describing ECS task: {e}")
            raise


def execute_afpgen(crc_data, file, metadata, file_name, sf_txn_id, data):
    cluster = os.getenv("ecs_cluster")
    task_def = os.getenv("ecs_task_defination")
    container_name = os.getenv("container_name")
    parts = file.split("/")
    # prefix = "/".join(parts[:4]) + "/"
    idx = parts.index("to_afpgen")

    # Join everything before "to_afpgen"of_path
    prefix = "/".join(parts[:idx]) + "/"

    output_file_name = crc_data["OUTPUT_FILE_PREFIX"]
    for suf in crc_data["OUTPUT_FILE_SUFFIX"]:
        if suf in file_name:
            output_file_name += f".{crc_data['CORP']}.{crc_data['RUNDATE']}.{crc_data['CYCLE']}{suf}.zip"
            break

    of_prefix = "/".join(parts[:idx]) + "/"
    of_path = of_prefix + f"from_afpgen/wip/{sf_txn_id}/"


    if bypass_afpgen:
        s3_files = [
            data["InputFile"],
            data["MetaDataFile"],
        ]
        target_bucket = str(os.getenv("bucket_name"))
        target_key = of_prefix + f"from_afpgen/wip/{sf_txn_id}/" + output_file_name

        result = zip_s3_files(s3_files, target_bucket, target_key)
        if result:
            print(f"🎉 ZIP available at: {result}")
            return

    command = [
        "-if", file,
        "-of", of_path,
        "-corp", crc_data["CORP"],
        "-rd", crc_data["RUNDATE"],
        "-cyc", crc_data["CYCLE"],
        "-config", prefix + "config/" + corp_config["AFPGEN_MASTER_CONFIG_FILE"],
        "-tran", sf_txn_id,
        "-outputfile", output_file_name
    ]
    if corp_config["NO_FC_CONTROL_LOAD_APPLICABLE"]:
        command.extend([
            "-urconfig", prefix + "config/" + corp_config["NO_FC_CONTROL_CONFIG"]
        ])
    if corp_config["SAVE_METADATA_KEY"]:
        os.environ["SAVE_METADATA_KEY"]=corp_config["SAVE_METADATA_KEY"]
    
    if "AFPGEN_SYSTEM_CONFIG_FILE" in corp_config and corp_config["AFPGEN_SYSTEM_CONFIG_FILE"]:
        command.extend([
            "-sysconfig", prefix + "config/" + corp_config["AFPGEN_SYSTEM_CONFIG_FILE"]
        ])
    if "AFPGEN_CUSTOM_CONFIG_FILE" in corp_config and corp_config["AFPGEN_CUSTOM_CONFIG_FILE"]:
        command.extend([
            "-customconfig", prefix + "config/" + corp_config["AFPGEN_CUSTOM_CONFIG_FILE"]
        ])

    if metadata:
        command.extend([
            "-metadatafile", metadata
        ])

    try:
        if not all([cluster, task_def, container_name]):
            logger.info(f"Running ECS task on cluster={cluster}, task_def={task_def}, container={container_name}")
            raise RuntimeError(
                f"Missing required ECS parameters: "
                f"cluster={cluster}, task_def={task_def}, container_name={container_name}"
            )

        response = ecs_client.run_task(
            cluster=cluster,
            taskDefinition=task_def,
            launchType="EC2",
            overrides={
                "containerOverrides": [
                    {
                        "name": container_name,
                        "command": command
                    }
                ]
            },
            count=1,
        )

        failures = response.get("failures", [])
        if failures:
            logger.error(f"ECS run_task failures: {failures}")
            raise RuntimeError("ECS run_task failed.")

        task_arn = response["tasks"][0]["taskArn"]
        wait_for_task_completion(ecs_client, cluster, task_arn)

    except ClientError as e:
        logger.exception("Error starting ECS task")
        raise


def set_job_params_as_env_vars():
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            os.environ[key] = sys.argv[i + 1]


def read_json_from_s3(bucket_name, key):
    s3 = boto3.client("s3")
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        content = response["Body"].read().decode('utf-8')
        return json.loads(content)
    except ClientError as e:
        print(f"Error reading object {key} from {bucket_name}:{e}")
        raise e


if __name__ == '__main__':

    try:
        set_job_params_as_env_vars()
        data = {}

        if os.getenv("smartcom_metadata"):
            data = json.loads(os.getenv("smartcom_metadata"))
        elif os.getenv("bucket_name") and os.getenv("key"):
            data = read_json_from_s3(os.getenv("bucket_name"), os.getenv("key"))
        file = data["InputFile"]

        file_name = os.path.basename(file)

        crc_data = corp_rd_cyc_handler(os.getenv("clientName"), file_name)

        sf_txn_id = (
                os.getenv("transactionId")
                or data.get("TransactionID")
                or crc_data.get("transaction_id")
        )
        if not sf_txn_id:
            raise RuntimeError("Missing transactionId from Step Function / input")

        crc_data["transaction_id"] = sf_txn_id

        metadata = ""
        if data["MetaDataFile"]:
            metadata = data["MetaDataFile"][:data["MetaDataFile"].rfind('.')] + ".xml"

        execute_afpgen(crc_data, file, metadata, file_name, sf_txn_id, data)
        if os.getenv("SAVE_METADATA_KEY") and metadata:
            save_metadata_file_name = crc_data["CORP"] + "_" + crc_data["RUNDATE"] + "_" + crc_data["CYCLE"] + "_" + \
                                      data["MetaDataFile"].split("/")[-1]
            save_metadata_file_key = os.getenv("SAVE_METADATA_KEY").rstrip("/") + f"/{save_metadata_file_name}"

            copy_source = {'Bucket': os.getenv("bucket_name"),
                           'Key': data["MetaDataFile"].replace("s3://", "").split("/", 1)[1]}
            s3.copy_object(CopySource=copy_source, Bucket=os.getenv("bucket_name"), Key=save_metadata_file_key)


        output_payload = {
            "bucket": str(os.getenv("bucket_name")),
            "key": f"jhi/from_afpgen/wip/{sf_txn_id}/",
            "transactionId": sf_txn_id,
            "outputPrefix": f"jhi/from_afpgen/wip/{sf_txn_id}/"
        }
        print(json.dumps(output_payload))

    except Exception as e:
        raise e
