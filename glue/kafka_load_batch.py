import sys
import boto3
import json
import os
from datetime import datetime
from awsglue.utils import getResolvedOptions
import splunk
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
import socket
# install confluent kafka at runtime
from confluent_kafka import Producer

# ----------------------------------------------------------------------------------------
def get_run_id(program_name: str) -> str:
    return f"arn:dpm:glue:{program_name}:{boto3.client('sts').get_caller_identity()['Account']}"
# ----------------------------------------------------------------------------------------
def get_configs():
    """Load Glue job parameters"""
    args_list = ['bucket_name', 'file_name', 'send_folder', 'save_folder',
                 'failed_folder', 'topic_name', 'end_points', 'region_name',
                 'username', 'password', 'file_format']
    return getResolvedOptions(sys.argv, args_list)
# ----------------------------------------------------------------------------------------
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
        print("✅ Kafka producer initialized")
        return producer
    except Exception as e:
        message = f"❌ Failed to initialize Kafka producer: {str(e)}"
        splunk.log_message({"Status": "Failed", "Message": message}, get_run_id(program_name))
        print(message)
        raise
# ----------------------------------------------------------------------------------------
def delivery_report(err, msg):
    """Kafka delivery report callback"""
    if err is not None:
        print(f"❌ Delivery failed: {err}")
# ----------------------------------------------------------------------------------------
def process_s3_file(file_key, bucket_name, send_folder, save_folder,
                    failed_folder, kafka_topic, producer, file_format):
    """Read either JSON or TXT from S3, send to Kafka, move file accordingly"""
    s3_client = boto3.client('s3')
    file_name = os.path.basename(file_key)

    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = obj['Body'].read().decode('utf-8')

        # --- Handle JSON ---
        if file_format.lower() == "json":
            try:
                json_data = json.loads(file_content)
                payload = json.dumps(json_data)
            except json.JSONDecodeError as e:
                print(f"❌ {file_name} invalid JSON: {str(e)}")
                raise

        # --- Handle TXT ---
        elif file_format.lower() == "txt":
            # treat raw text as payload directly
            payload = file_content

        else:
            print(f"⚠️ Unsupported format: {file_format}")
            return "skipped"

        # Produce to Kafka
        producer.produce(
            topic=kafka_topic,
            value=payload.encode('utf-8'),
            key=file_name.encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)

        # Move to save folder
        dest_key = f"{save_folder.rstrip('/')}/{file_name}"
        s3_client.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': file_key}, Key=dest_key)
        s3_client.delete_object(Bucket=bucket_name, Key=file_key)
        return "success"

    except Exception as e:
        print(f"❌ Failed sending {file_name}: {str(e)}")
        dest_key = f"{failed_folder.rstrip('/')}/{file_name}"
        try:
            s3_client.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': file_key}, Key=dest_key)
            s3_client.delete_object(Bucket=bucket_name, Key=file_key)
        except Exception as e2:
            print(f"❌ Could not move file {file_name} to failed_folder: {str(e2)}")
        return "fail"
# ----------------------------------------------------------------------------------------
def produce_messages(end_points, kafka_topic, bucket_name, send_folder,
                     username, password, save_folder, failed_folder, MAX_WORKS, file_format):
    """Scan S3 for {file_format} files, produce to Kafka"""
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    send_folder = send_folder if send_folder.endswith("/") else send_folder + "/"
    producer = initialize_kafka_producer(end_points, username, password)

    ext = "." + file_format.lower()
    all_file_keys = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=send_folder):
        for file in page.get('Contents', []):
            if file['Key'].lower().endswith(ext):
                all_file_keys.append(file['Key'])

    total = len(all_file_keys)
    if total == 0:
        print(f"⚠️ No {file_format.upper()} files found in {send_folder}")
        return

    print(f"📤 Producing {total} {file_format.upper()} messages to Kafka topic {kafka_topic} using {MAX_WORKS} threads...")
    success = fail = skipped = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKS) as executor:
        futures = {
            executor.submit(process_s3_file, key, bucket_name, send_folder, save_folder,
                            failed_folder, kafka_topic, producer, file_format): key
            for key in all_file_keys
        }
        for fut in as_completed(futures):
            result = fut.result()
            if result == "success":
                success += 1
            elif result == "fail":
                fail += 1
            else:
                skipped += 1

    producer.flush(30)
    summary = f"✅ Summary for {file_format.upper()} — Total: {total}, Success: {success}, Failed: {fail}, Skipped: {skipped}"
    splunk.log_message({"Status": "Info", "Message": summary}, get_run_id(program_name))
    print(summary)
# ----------------------------------------------------------------------------------------
def check_connect(end_points: str, timeout: int = 5) -> bool:
    """Test TCP connectivity to one or more Kafka brokers."""
    brokers = [ep.strip() for ep in end_points.split(",") if ep.strip()]
    for broker in brokers:
        try:
            host, port = broker.split(":")
            port = int(port)
        except ValueError:
            print(f"⚠️ Skipping invalid endpoint format: {broker}")
            continue
        print(f"🔌 Testing connectivity to {host}:{port} ...")
        try:
            sock = socket.create_connection((host, port), timeout=timeout)
            print(f"✅ Success: Glue can reach {host}:{port}")
            sock.close()
            return True
        except Exception as e:
            print(f"❌ Failed to connect to {host}:{port} → {str(e)}")
    return False
# ----------------------------------------------------------------------------------------
def get_kafka_credentials(secret_name, region_name):
    """Retrieve Kafka credentials from Secrets Manager"""
    client = boto3.client('secretsmanager', region_name=region_name)
    try:
        secret_value = client.get_secret_value(SecretId=secret_name)
        return json.loads(secret_value['SecretString'])
    except Exception as e:
        message = f"Error retrieving secret: {str(e)}"
        splunk.log_message({"Status": "Failed", "Message": message}, get_run_id(program_name))
        print(message)
        return None
# ----------------------------------------------------------------------------------------
def get_aws_parameter(param_name, region_name):
    ssm_client = boto3.client('ssm', region_name=region_name)
    return ssm_client.get_parameter(Name=param_name, WithDecryption=True)['Parameter']['Value']
# ----------------------------------------------------------------------------------------
if __name__ == '__main__':
    program = os.path.basename(sys.argv[0])
    program_name = program.split('.')[0]
    args = get_configs()

    bucket_name   = args['bucket_name']
    send_folder   = args['send_folder']
    topic_name    = args['topic_name']
    end_points    = args['end_points']
    file_name     = args['file_name']
    save_folder   = args['save_folder']
    failed_folder = args['failed_folder']
    region_name   = args['region_name']
    username      = args['username']
    password      = args['password']
    file_format   = args['file_format'].lower()

    username = get_aws_parameter(username, region_name)
    password = get_aws_parameter(password, region_name)

    MAX_WORKS = 64

    if not all([bucket_name, send_folder, topic_name, save_folder, failed_folder, region_name, username, password, file_format]):
        message = f"{program_name} failed: missing required parameters!"
        splunk.log_message({"Status": "Failed", "Message": message}, get_run_id(program_name))
        raise ValueError(message)

    if file_format not in ["json", "txt"]:
        raise ValueError("file_format must be either 'json' or 'txt'")

    if not check_connect(end_points):
        message = f"{program_name} connection check failed."
        splunk.log_message({"Status": "Failed", "Message": message}, get_run_id(program_name))
        raise ValueError(message)

    start_msg = f"🚀 Starting Glue Kafka Load ({file_format.upper()}) at {datetime.now()}"
    splunk.log_message({"Status": "Info", "Message": start_msg}, get_run_id(program_name))
    print(start_msg)

    produce_messages(end_points, topic_name, bucket_name, send_folder,
                     username, password, save_folder, failed_folder, MAX_WORKS, file_format)

    finish_msg = f"🏁 Finished Glue Kafka Load ({file_format.upper()}) at {datetime.now()}"
    splunk.log_message({"Status": "Info", "Message": finish_msg}, get_run_id(program_name))
    print(finish_msg)