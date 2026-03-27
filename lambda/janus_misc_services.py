import boto3
import os
import re
from urllib.parse import unquote_plus

# AWS clients
ecs_client = boto3.client("ecs")
s3_client = boto3.client("s3")

# ECS configuration
CLUSTER_NAME = os.environ.get("ECS_CLUSTER")
TASK_DEFINITION = os.environ.get("ECS_TASK_DEFINITION")
CONTAINER_NAME = os.environ.get("ECS_CONTAINER_NAME")
INPUT_FILE_KEY=""

def fetch_file(bucket_name, object_key, extension_pattern):
    prefix = object_key.replace(os.path.basename(object_key), "")
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' not in response:
        return ""
    matches = [obj['Key'] for obj in response['Contents'] if re.search(extension_pattern, obj['Key'], re.IGNORECASE)]
    return matches[0] if matches else ""

def parse_filename(filename):
    """Extract details from filename."""
    match = re.match(r"(\w+)\.(\w+)\.(\w+)\.(\d{8})\.(\w)\..+", filename)
    if not match:
        raise ValueError(f"Filename does not match expected pattern: {filename}")
    return match.groups()


def build_ecs_command(bucket_name, object_key, corp, rundate, cycle , product_type):

    INPUT_FILE_KEY=fetch_file(bucket_name,object_key.replace(os.path.basename(object_key),""),".TXT|.txt")
    """Build ECS container command arguments."""
    print(TASK_DEFINITION)
    print(CONTAINER_NAME)
    print(CLUSTER_NAME)
    
    return [
        "java", "-jar", "miscreportst-0.0.1.jar",
        "-inputBucket", bucket_name,
        "-inputKey", INPUT_FILE_KEY,
        "-corp", corp,
        "-rundate", rundate,
        "-cyc", cycle,
        "-gpid", "3", "-did", "4",
		"-recipents", "no-reply@broadridge.com,KalaJyothi.Mamidisetty@broadridge.com",
        "-subject", "List Bill Report"           
    ]

def run_ecs_task(command):
    """Run ECS task and return task ARN."""
 
    response = ecs_client.run_task(
        cluster=os.getenv("ECS_CLUSTER"),
        taskDefinition=os.getenv("ECS_TASK_DEFINITION"),
        launchType="EC2",
        overrides={"containerOverrides": [{"name": os.getenv("ECS_CONTAINER_NAME"), "command": command}]}
    )
    print(response)
    tasks = response.get("tasks", [])
    if not tasks:
        
        raise RuntimeError(f"ECS task failed to start: {response.get('failures', [])}")
    return tasks[0]["taskArn"]


def move_file(bucket_name, object_key, save_bucket, save_prefix):
    """Move file to processed location."""
    filename = os.path.basename(object_key)
    save_key = os.path.join(save_prefix, filename)
    s3_client.copy_object(Bucket=save_bucket, CopySource={'Bucket': bucket_name, 'Key': object_key}, Key=save_key)
    s3_client.delete_object(Bucket=bucket_name, Key=object_key)
    return f"s3://{save_bucket}/{save_key}"


def lambda_handler(event, context):
    try:
        bucket_name = event['detail']['bucket']['name']
        object_key = unquote_plus(event['detail']['object']['key'])
        filename = os.path.basename(object_key)

        system, product_type, corp, rundate, cycle = parse_filename(filename)
        if product_type == 'MISC_LIST' or 'MISC_INTS':
            command = build_ecs_command(bucket_name, object_key, corp, rundate, cycle , product_type)
            task_arn = run_ecs_task(command)
        saved_file = move_file(bucket_name, object_key,bucket_name,INPUT_FILE_KEY)

        return {"status": "SUCCESS", "taskArn": task_arn, "savedFile": saved_file}

    except Exception as e:
        print(f"Error: {e}")
        return {"status": "ERROR", "message": str(e)}
