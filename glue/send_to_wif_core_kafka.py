# Author : Abhinav Gugulothu
"""
This module sends BRX OUTPUT JSON file locations to the WIF Core Kafka topic.

During batching, all JSON files are moved from <client>/<product>/brx/out to the wip folder along with done.json.
This module opens the done.json file in wip, sends a start message, then publishes each BRX OUTPUT JSON file name listed in done.json (sample message), and finally sends an end message.
Message Structures: https://confluence.broadridge.net/spaces/ICSDIGCOE/pages/962697917/Upstream+-+Wif+Core+Handshake+-+Wedbush

"""

import datetime
import json
import sys
import os
import boto3
import socket
import traceback

from confluent_kafka import Producer

def set_job_params_as_env_vars():
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i+1]
            os.environ[key] = value
            print(f"Set environment variable: {key}={value}")

def initialize_kafka_producer(end_points, username, password):
    """Init Kafka producer using Confluent Client"""
    try:
        conf = {
            'bootstrap.servers': end_points,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': username,
            'sasl.password': password,
            'linger.ms': 5,
            'message.max.bytes': 1000000,
            'socket.timeout.ms': 30000,
            'enable.idempotence': True,
            'session.timeout.ms': 45000
        }

        producer = Producer(conf)
        return producer
    except Exception as e:
        print(f"Failed to initialize Kafka producer: {str(e)}")
        raise

def get_aws_parameter(environment, param_name, region_name):
    ssm_client = boto3.client('ssm', region_name=region_name)
    return ssm_client.get_parameter(Name="/"+environment+param_name, WithDecryption=True)['Parameter']['Value']

def check_connect(end_points: str, timeout: int = 5) -> bool:
    brokers = [ep.strip() for ep in end_points.split(",") if ep.strip()]
    for broker in brokers:
        try:
            host, port = broker.split(":")
            port = int(port)
        except ValueError:
            print(f"Skipping invalid endpoint format: {broker}")
            continue
        print(f"Testing connectivity to {host}:{port} ...")

        try:
            sock = socket.create_connection((host, port), timeout=timeout)
            print(f"Success: Glue can reach {host}:{port}")
            sock.close()
            return True
        except Exception as e:
            print(f"Failed to connect to {host}:{port} → {str(e)}")

    return False

def send_to_kafka(producer, key, wif_core_message, topic):
    try:
        headers = [
            ("Content-Type", "application/json"),
            ("source", "send_to_wif_core_kafka_glue"),
            ("Accept", "application/json"),
        ]

        producer.produce(
            topic=topic,
            key=key,
            value=json.dumps(wif_core_message),
            headers=headers
        )

        producer.flush()

        print(f"Sent message to Kafka topic '{topic}': {wif_core_message}")
    except Exception as e:
        print(f"Error sending message to Kafka: {str(e)}")
        print(f"StackTrace: {traceback.format_exc()}")
        raise

def main():
    # 1. Get the manifest file path from $.StatePayload.SM_ARN_BATCHING.sfStepsToDone.moveFiles.manifest_file_path
    # 2. Location of the files from $.StatePayload.SM_ARN_BATCHING.sfStepsToDone.moveFiles.output_dir
    # 4. Create the path of the manifest file in the wip directory.
    # 3. Open the manifest file and check if there are any files inside it, if found send them to WIF Core Kafka topic each file one by one.
    try:
        print("Starting send_to_wif_core_kafka process")
        set_job_params_as_env_vars()

        move_files_object   = json.loads(os.environ.get("move_files_object", "{}"))
        wif_kafka_object    = json.loads(os.environ.get("wif_kafka_object", "{}"))

        bucket              = os.environ.get("bucket")
        environment         = os.environ.get("environment")
        region              = os.environ.get("region")

        wip_directory       = move_files_object.get("output_dir").rstrip("/")
        manifest_file_path  = move_files_object.get("manifest_file_path")
        topic               = wif_kafka_object.get("kafkaTopic","")

        print(f"WIP Directory      : {wip_directory}")
        print(f"Manifest File Path : {manifest_file_path}")

        # Get the triggered file name from the manifest file path by removing the _done.json suffix and replacing it with .TXT
        manifest_file_name        = os.path.basename(manifest_file_path)
        triggered_file_name       = manifest_file_name.replace("_done.json", ".TXT")
        original_file_name        = manifest_file_name.replace("_done.json", "")
        manifest_file_path_in_wip = os.path.join(wip_directory, manifest_file_name)

        # Read manifest file from S3
        s3_client           = boto3.client('s3')
        response            =  s3_client.get_object(Bucket=bucket, Key=manifest_file_path_in_wip)
        manifest_content    = response['Body'].read().decode('utf-8')
        manifest_data       = json.loads(manifest_content)

        debatched_files = manifest_data.get(triggered_file_name, {}).get("files", [])
        if debatched_files is None or len(debatched_files) == 0:
            print(f"No debatched files found in manifest for {triggered_file_name}. Exiting process.")
            return

        brokers            = get_aws_parameter(environment, wif_kafka_object.get("brokers"),  region)
        username           = get_aws_parameter(environment, wif_kafka_object.get("username"), region)
        password           = get_aws_parameter(environment, wif_kafka_object.get("password"), region)
        
        if not check_connect(brokers):
            print("Kafka end points are not reachable. Exiting process.")
            return

        producer = initialize_kafka_producer(brokers, username, password)
        print("Kafka producer initialized")

        client_name  = wif_kafka_object.get("clientName", "")
        account_type = "SINGLE"
        batchId      = wip_directory.split("/")[-1]
        run_type     = "DAILY"

        # start message
        start_message = {
            "clientName": client_name,
            "status": "STARTED",
            "uuid": f"START-{batchId}",
            "batchId": batchId,
            "runType": run_type,
            "createdTimestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "env": environment,
            "corp":"F8884",
            "zipFileName":f"{original_file_name}.zip"
        }
        send_to_kafka(producer, f"START-{batchId}", start_message, topic)

        for item in debatched_files:
            # brx_output_file = 01230201_0000297_BIOS.C025.OUT.WEDBUSH.BPS.CNFM.1212202500005.txt.json
            brx_output_file = item.get("dataFile")
            account_number  = brx_output_file.split("_")[0]  # Extract account number from file name 

            wif_core_message = {
                "clientName" : client_name,
                "accountType": account_type,
                "accountNumber": account_number,
                "uuid": f"{account_number}-{batchId}",
                "batchId": batchId,
                "bucketName": bucket,
                "runType": run_type,
                "createdTimestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "env": environment,
                "corp":"F8884",
                "zipFileName":f"{original_file_name}.zip",
                "fileLocation": [
                    {
                        "accountNumber": account_number,
                        "type": account_type,
                        "fileName": f"{wip_directory}/{brx_output_file}"
                    }
                ]
            }
            send_to_kafka(producer, f"{account_number}-{batchId}", wif_core_message, topic)

        # end message
        end_message = {
            "clientName": client_name,
            "status": "COMPLETED",
            "uuid": f"END-{batchId}",
            "batchId": batchId,
            "runType": run_type,
            "createdTimestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "env": environment,
            "corp":"F8884",
            "zipFileName":f"{original_file_name}.zip",
            "batchCounts": {
                "processedCounts": {
                    "single": str(len(debatched_files)),
                    "interestedParty": ""
                }
            }
        }
        send_to_kafka(producer, f"END-{batchId}", end_message, topic)

    except Exception as e:
        error_message = f"Error in send_to_wif_core_kafka: {str(e)}"
        print(error_message)
        print(f"StackTrace: {traceback.format_exc()}")
        raise

if __name__ == "__main__":
    main()