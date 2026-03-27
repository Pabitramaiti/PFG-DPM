import splunk
import boto3
import os
import sys
import json
import time

# Initialize clients
ecs_client = boto3.client('ecs')


def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:lmbd-java-functions-executor:" + account_id


def set_function_param_as_env(json_string):
    data = json.loads(json_string)

    for key, value in data.items():
        os.environ[key] = str(value)


def wait_for_task_completion(cluster, task_arn):
    """Wait for the ECS task to complete and return the final status."""
    while True:
        response = ecs_client.describe_tasks(cluster=cluster, tasks=[task_arn])
        task_status = response['tasks'][0]['lastStatus']
        if task_status in ['STOPPED']:
            # Check if the task stopped with a failure
            stopped_reason = response['tasks'][0].get('stoppedReason', 'Unknown')
            containers = response['tasks'][0].get('containers', [])
            for container in containers:
                exit_code = container.get('exitCode', 0)
                if exit_code != 0:
                    ##                   splunk.log_message({'Status': 'failed', 'di-glue-b228_conversion': exit_code "-" stopped_reason}, get_run_id())
                    print({'Status': 'failed', 'Exit Code': exit_code})
                    print({'Status': 'failed', 'Stopped Reason': stopped_reason})
                    raise RuntimeError(f"Task failed with exit code {exit_code}: {stopped_reason}")
            message = "ECS task completed successfully."
            print({'Status': 'success', 'Message': message})
            print('ECS task completed successfully.')
            break

        print(f'ECS task status: {task_status}. Waiting for completion...')
        time.sleep(30)  # Poll every 30 seconds

def set_job_params_as_env_vars():
    # print("sys.argv", sys.argv)
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    i = 1
    while i < len(sys.argv):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            if i + 1 < len(sys.argv) and not sys.argv[i + 1].startswith('--'):
                value = sys.argv[i + 1]
                if key=="function_input":
                    splunk.log_message({'Status': 'info', 'Message': "Setting java function param in env"}, get_run_id())
                    set_function_param_as_env(value)
                else:
                    os.environ[key] = value
                i += 2  # Move to the next key-value pair
            else:
                # Handle cases where the key doesn't have a value
                # print(f"Key '{key}' does not have a value.")
                i += 1  # Move to the next key
        else:
            i += 1  # Move to the next argument

def main():
    splunk.log_message({'Status': 'info', 'Message': "Setting env from params"}, get_run_id())
    try:
        set_job_params_as_env_vars()
    except Exception as e:
        message = f"failed to retrieve input parameters. {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
        raise Exception(message)

    message = "di-glue-java-initiator processing Started"
    splunk.log_message({'Status': 'success', 'Message': message}, get_run_id())
    cluster_name = os.environ.get('cluster_name')
    task_definition_name = os.environ.get('task_definition_name')
    container_name = os.environ.get('container_name')
    env_vars = os.environ
    ecs_env=[]
    for key, value in env_vars.items():
        print(f'{key}: {value}')
        ecs_env.append({'name':key,'value':f"{value}"})
    try:
        # Trigger ECS task
        response = ecs_client.run_task(
            cluster=cluster_name,
            launchType='EC2',
            taskDefinition=task_definition_name,
            count=1,
            overrides={
                'containerOverrides': [
                    {
                        'name': container_name,
                        'environment':ecs_env
                    }
                ]
            }
        )

        # Retrieve task ARN
        task_arn = response['tasks'][-1]['taskArn']
        print(f'Successfully started ECS task: {task_arn}')
        splunk.log_message({'Status': 'Success', 'Successfully started ECS task': task_arn}, get_run_id())

        # Wait for the ECS task to complete
        wait_for_task_completion(cluster_name, task_arn)

    except Exception as e:
        print('Error executing ECS task:', str(e))
        splunk.log_message({'Status': 'failed', 'di-glue-b228_conversion': message}, get_run_id())

        raise

    except Exception as e:
        print('Error executing ECS task:', str(e))
        splunk.log_message({'Status': 'failed', 'di-glue-b228_conversion': message}, get_run_id())

        raise



if __name__ == "__main__":
    print({'Status': 'info', 'Message': "Setting env from params"}, get_run_id())
    program_name = os.path.basename(sys.argv[0])
    main()
