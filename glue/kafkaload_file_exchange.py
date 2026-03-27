import sys, boto3, os, json, time, subprocess
from datetime import datetime
from awsglue.utils import getResolvedOptions

# --- Install confluent-kafka in Glue runtime ---
subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", "/tmp", "confluent-kafka"])
sys.path.insert(0, "/tmp")

import splunk
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException




# ----------------------------------------------------------------------------------------
def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return f"arn:dpm:glue:kafka_job:{account_id}"
# ----------------------------------------------------------------------------------------
def set_job_params_as_env_vars():
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]
            value = sys.argv[i + 1]
            os.environ[key] = value
# ----------------------------------------------------------------------------------------
def get_ssm_parameter(name, region):
    ssm = boto3.client("ssm", region_name=region)
    return ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
# ----------------------------------------------------------------------------------------
def initialize_kafka_producer(brokers, username, password):
    conf = {
        'bootstrap.servers': brokers,
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
# ----------------------------------------------------------------------------------------
def initialize_kafka_consumer(brokers, username, password, group_id):
    conf = {
        'bootstrap.servers': brokers,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'SCRAM-SHA-512',
        'sasl.username': username,
        'sasl.password': password,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'session.timeout.ms': 45000,
        'max.poll.interval.ms': 3600000,  # 1 hr
        'socket.timeout.ms': 30000
    }
    return Consumer(conf)
# ----------------------------------------------------------------------------------------
def send_message_to_kafka(targetFileName, producer, topic, message, key):
    try:
        # Ensure all header values are bytes
        headers = [
            ("Content-Type", b"application/json"),
            ("Accept", b"application/json"),
            ("source", b"Kafka Glue"),
            ("filename", os.environ.get("filename")),
            ("targetfilename", ("\"" + targetFileName + "\"").encode("utf-8")),
            ("file_path", (os.environ.get("file_path") or "").encode("utf-8"))
        ]

        # Serialize message dict to JSON and encode
        message_bytes = json.dumps(message).encode("utf-8")

        print("headers:", headers)
        print("message:", message_bytes.decode("utf-8"))  # safe for debugging

        producer.produce(
            topic=topic,
            key=key,
            value=message_bytes,
            headers=headers
        )

        producer.flush()

        print(f"✅ Sent message to Kafka topic '{topic}': {targetFileName}")
        splunk.log_message({
            'Status': 'Success',
            'InputFileName': targetFileName,
            'Message': f"Message published successfully to Kafka topic {topic}"
        }, get_run_id())
        return True

    except Exception as e:
        print(f"❌ Failed to send message: {e}")
        splunk.log_message({
            'Status': 'Failed',
            'InputFileName': targetFileName,
            'Message': f"Failed to send message: {str(e)}"
        }, get_run_id())
        return False

# ----------------------------------------------------------------------------------------
def consume_kafka_message(consumer, topic, target_filename, timeout_minutes, poll_interval):
    consumer.subscribe([topic])
    start_time = time.time()
    max_wait_seconds = timeout_minutes * 60

    print(f"👂 Starting Kafka consumer for {topic} up to {timeout_minutes} minutes...")

    while True:
        msg = consumer.poll(timeout=poll_interval)
        elapsed = time.time() - start_time
        if msg is None:
            print(f"[{int(elapsed)}s] Waiting for messages...")
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Kafka Error: {msg.error()}")
                continue
        else:
            print("enter into else")
            try:

                raw_value = msg.value()
                if raw_value is None:
                    print(f"[{int(elapsed)}s] ⚠️ Received message with no value. Skipping.")
                    continue
                decoded_value = raw_value.decode('utf-8')
                print("json.loads(msg.value().decode", decoded_value)

                record_value = json.loads(decoded_value)
                print("record_value", record_value)
                print(f"[{int(elapsed)}s] Received message: {record_value}")

                if isinstance(record_value, dict) and record_value.get("filename") == target_filename:
                    splunk.log_message({
                        'Status': 'Success',
                        'InputFileName': 'consume_from_kafka_sdk',
                        'Message': f"File '{target_filename}' consumed successfully from Kafka"
                    }, get_run_id())
                    print("🎯 Matching file found — stopping consumer.")
                    break
            except json.JSONDecodeError:
                print(f"[{int(elapsed)}s] Invalid JSON message received.")

        if elapsed >= max_wait_seconds:
            print(f"⌛ Timeout reached ({timeout_minutes} min). No matching message found.")
            splunk.log_message({
                'Status': 'Timeout',
                'InputFileName': 'consume_from_kafka_sdk',
                'Message': f"File '{target_filename}' not received after {timeout_minutes} minutes"
            }, get_run_id())
            break

    consumer.close()
# ----------------------------------------------------------------------------------------
def main():
    try:
        set_job_params_as_env_vars()
        region = os.environ.get("region")
        brokers = os.environ.get("kafka_host")
        topic = os.environ.get("topic")
        bucket = os.environ.get("bucket")
        filename = os.environ.get("filename")
        group_id = os.environ.get("group_id", "pathfinder")
        duration_minutes = int(os.environ.get("expected_duration"))
        wait_time_seconds = int(os.environ.get("consumer_interval"))

        print("UserNameEnv",duration_minutes)
        username = get_ssm_parameter(os.environ.get("user_name"), region)
        password = get_ssm_parameter(os.environ.get("password"), region)

        producer = initialize_kafka_producer(brokers, username, password)
        consumer = initialize_kafka_consumer(brokers, username, password, group_id)

        timestamp = str(int(time.time()))
        targetFileName = filename + "." + timestamp

        message = {
            "bucket": bucket,
            "filename": targetFileName
        }

        if send_message_to_kafka(targetFileName, producer, topic, message, key=filename):
            consume_kafka_message(consumer, topic, targetFileName, duration_minutes, wait_time_seconds)

    except Exception as e:
        print(f"❌ Main process failed: {e}")
        splunk.log_message({
            'Status': 'Failed',
            'InputFileName': 'KafkaSDKFlow',
            'Message': f"Main process failed: {str(e)}"
        }, get_run_id())
        raise

# ----------------------------------------------------------------------------------------
if __name__ == '__main__':
    main()
