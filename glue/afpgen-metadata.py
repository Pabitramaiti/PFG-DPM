import boto3
import os
import sys
import time
import json


# Initialize ECS client
ecs_client = boto3.client('ecs')


def set_job_params_as_env_vars():
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            value = sys.argv[i + 1]
            os.environ[key] = value
            print(f'Set environment variable {key} to {value}')

def get_s3_filename(s3_path):
    if s3_path.startswith("s3://"):
        s3_path = s3_path[5:]  # Remove 's3://'
        s3_path = s3_path.split("/", 1)[1]  # Remove bucket name
    return os.path.basename(s3_path)
    
def remove_last_s3_dir(s3_path):
    if s3_path.endswith("/"):
        s3_path = s3_path[:-1]
    path_no_scheme = s3_path.replace("s3://", "")
    bucket, *key_parts = path_no_scheme.split("/")
    key = PurePosixPath("/".join(key_parts)).parent
    return f"s3://{bucket}/{key}"
    
def print_all_env_vars():
    for key, value in os.environ.items():
        print(f'{key}: {value}')


def wait_for_task_completion(cluster, task_arn):

    """Wait for the ECS task to complete and return the final status."""
    while True:
        response = ecs_client.describe_tasks(cluster=cluster, tasks=[task_arn])
        task_status = response['tasks'][0]['lastStatus']
        if task_status == 'STOPPED':
            stopped_reason = response['tasks'][0].get('stoppedReason', 'Unknown')
            containers = response['tasks'][0].get('containers', [])
            for container in containers:
                exit_code = container.get('exitCode', 0)
                if exit_code != 0:
                    print(f"Task failed with exit code {exit_code}: {stopped_reason}")
                    raise RuntimeError(f"Task failed with exit code {exit_code}: {stopped_reason}")
            print("ECS task completed successfully.")
            break
        print(f"ECS task status: {task_status}. Waiting for completion...")
        time.sleep(30)


def main():
    print("ECS task submission started")

    try:
        set_job_params_as_env_vars()
        print_all_env_vars()
    except Exception as e:
        print(f"Error setting environment variables: {e}")
        raise e

    # # Read ECS launch info from environment
    cluster_name =os.getenv("cluster_name")
    task_definition_name = os.getenv("task_definition_name")
    container_name = os.getenv("container_name")
    # cluster_name ="br_icsdev_dpmtest_app_ecs_cluster"
    # task_definition_name = "janus_metadata_task:7"
    # container_name = "janus_metadata_cntr"
    
    base_path = os.path.dirname(os.getenv("key"))
    output_filename = os.path.basename(os.getenv("key"))+".xml"
    new_base_path = '/'.join(base_path.split('/')[:-1]) + f'/to_afpgen/{os.getenv("transactionId")}'
    out_file = f"{new_base_path}/{output_filename}"
    

    try:
        # Run ECS task
        response = ecs_client.run_task(
            cluster=cluster_name,
            launchType='EC2',
            taskDefinition=task_definition_name,
            count=1,
            
            overrides={
                'containerOverrides': [
                    {
                        'name': container_name,
                        "command": [
                          "java", "-jar", "janus_metadata-0.0.1.jar",
                          "-if", os.getenv("fileName"),
                          "-of", out_file,
                          "-producerPublishUrl", os.getenv("producerPublishUrl"),
                          "-bucketName", os.getenv("s3_bucket_input"),
                          "-key", os.getenv("key"),
                          "-communicationID", os.getenv("comunicationId"),
                          "-transactionID", os.getenv("transactionId"),
                          "-clientName", os.getenv("clientName"),
                        #   "-copy_metadata_path",os.getenv("copy_metadata_path").strip("/")+f"/{output_filename}"
                        ]
                       
                    }
                ]
            }
        )

 
        print(response)
        task_arn = response['tasks'][0]['taskArn']
        print(f'Successfully started ECS task: {task_arn}')

        # Wait for task to finish
        wait_for_task_completion(cluster_name, task_arn)

    except Exception as e:
        print(f'Error executing ECS task: {e}')
        raise

if __name__ == "__main__":

    main()