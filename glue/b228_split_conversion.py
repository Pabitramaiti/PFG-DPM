import boto3
import os
import sys
import time
import splunk

# Initialize clients
ecs_client = boto3.client('ecs')


def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:dpmdev-di-glue-load_stage:" + account_id


def set_job_params_as_env_vars():
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            value = sys.argv[i + 1]
            os.environ[key] = value
            print(f'Set environment variable {key} to {value}')


def print_all_env_vars():
    for key, value in os.environ.items():
        print(f'{key}: {value}')


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
                    splunk.log_message({'Status': 'failed', 'Exit Code': exit_code}, get_run_id())
                    splunk.log_message({'Status': 'failed', 'Stopped Reason': stopped_reason}, get_run_id())
                    raise RuntimeError(f"Task failed with exit code {exit_code}: {stopped_reason}")
            message = "ECS task completed successfully."
            splunk.log_message({'Status': 'success', 'Message': message}, get_run_id())
            print('ECS task completed successfully.')
            break

        print(f'ECS task status: {task_status}. Waiting for completion...')
        time.sleep(30)  # Poll every 30 seconds


def main():
    # Log start message
    message = "di-glue-b228_conversion processing Started"
    splunk.log_message({'Status': 'success', 'Message': message}, get_run_id())


    try:
        set_job_params_as_env_vars()
        print_all_env_vars()

    except Exception as e:
        splunk.log_message({'Message': f"Error setting environment variables: {e}", "Status": "ERROR"}, get_run_id())
        raise e

    # Access environment variables
    input_bucket = os.environ['input_bucket']
    output_bucket = os.environ['output_bucket']
    config_bucket = os.environ['config_bucket']
    config_filename = os.environ['config_filename']
    input_folder = os.environ['input_folder']
    output_queue_folder = os.environ['output_queue_folder']
    header_queue_folder = os.environ['header_queue_folder']
    hold_queue_folder = os.environ['hold_queue_folder']
    header_input_folder = os.environ['header_input_folder']
    queue_folder = os.environ['queue_folder']
    config_folder = os.environ['config_folder']
    thread_model = os.environ['thread_model']
    thread_num = os.environ['thread_num']
    input_filepath_ebcdic = os.environ['input_filepath_ebcdic']
    batch_size = os.environ['batch_size']
    run_type = os.environ['run_type']
    trid_file = os.environ['trid_file']
    b1_settings = os.environ['b1_settings']
    file_validation = os.environ['file_validation']
    account_filter = os.environ['account_filter']
    #s3_thread_count = os.environ['s3_thread_count']
    s3_batchsize = os.environ['s3_batchsize']
    printSuppInd=os.environ['printSuppInd']
    rbcfilterenable = os.environ['rbcfilterenable']
    rbcacffile = os.environ['rbcacffile']
    rbcbranchfile = os.environ['rbcbranchfile']

    basename = input_filepath_ebcdic.rsplit(".txt", 1)[0]
    extension = '.json'
##    recon_extension = '-ASCII.recon_rpt'
    recon_extension = '.recon_rpt'
    recon_acct_list_extension = '.reconAccountList_rpt'

##    output_filename_ascii = basename + '-ASCII' + extension
    output_filename_ascii = basename + extension
    output_filename_trailer = basename + '-TRAILER' + extension
    output_filename_sup = basename + '-SUPPRESSED' + extension
    output_filename_recon = basename + recon_extension
    output_filename_acct_list = basename + recon_acct_list_extension
    

    cluster_name = os.environ.get('cluster_name')
    task_definition_name = os.environ.get('task_definition_name')
    container_name = os.environ.get('container_name')

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
                        'environment': [
                            {'name': 'INPUT_BUCKET', 'value': input_bucket},
                            {'name': 'INPUT_FILEPATH', 'value': input_filepath_ebcdic},
                            {'name': 'CONFIG_BUCKET', 'value': config_bucket},
                            {'name': 'CONFIG_FILENAME', 'value': config_filename},
                            {'name': 'OUTPUT_BUCKET', 'value': output_bucket},
                            {'name': 'OUTPUT_FILEPATH_ASCII', 'value': output_filename_ascii},
                            {'name': 'OUTPUT_FILEPATH_TRAILER', 'value': output_filename_trailer},
                            {'name': 'OUTPUT_FILEPATH_SUP', 'value': output_filename_sup},
                            {'name': 'INPUT_FOLDER', 'value': input_folder},
                            {'name': 'OUTPUT_QUEUE_FOLDER', 'value': output_queue_folder},
                            {'name': 'HEADER_QUEUE_FOLDER', 'value': header_queue_folder},
                            {'name': 'HOLD_QUEUE_FOLDER', 'value': hold_queue_folder},
                            {'name': 'QUEUE_FOLDER', 'value': queue_folder},
                            {'name': 'HEADER_INPUT_FOLDER', 'value': header_input_folder},
                            {'name': 'CONFIG_FOLDER', 'value': config_folder},
                            {'name': 'THREAD_MODEL', 'value': thread_model},
                            {'name': 'THREAD_NUM', 'value': thread_num},
                            {'name': 'BATCH_SIZE', 'value': batch_size},
                            {'name': 'RUN_TYPE', 'value': run_type},
                            {'name': 'TRID_FILE', 'value': trid_file},
                            {'name': 'B_SETTINGS', 'value': b1_settings},
                            {'name': 'FILE_VALIDATION', 'value': file_validation},
                            {'name': 'RECON_RPT', 'value': output_filename_recon},
                            {'name': 'RECON_ACCT_LIST', 'value': output_filename_acct_list},
                            {'name': 'ACCOUNTFILTER_FILE', 'value': account_filter},
                        #    {'name': 'S3_THREAD_COUNT', 'value': s3_thread_count},
                            {'name': 'SBATCH_SIZE', 'value': s3_batchsize},
                            {'name': 'printSuppInd', 'value': printSuppInd},
                            {'name': 'RBC_FILTER_ENABLE', 'value': rbcfilterenable},
                            {'name': 'RBC_ACF_FILE', 'value': rbcacffile},
                            {'name': 'RBC_BRANCH_FILE', 'value': rbcbranchfile},
                        ]
                    }
                ]
            }
        )

        # Retrieve task ARN
        print(f"size:{response}")
        # Fix for the IndexError when increasing CPU
        if 'tasks' in response and len(response['tasks']) > 0:
            task_arn = response['tasks'][-1]['taskArn']
            print(f'Successfully started ECS task: {task_arn}')
            splunk.log_message({'Status': 'Success', 'Successfully started ECS task': task_arn}, get_run_id())

            # Wait for the ECS task to complete
            wait_for_task_completion(cluster_name, task_arn)
        else:
            error_msg = f"Failed to start ECS task. No tasks were created. Response: {response}"
            print(error_msg)
            splunk.log_message({'Status': 'failed', 'Message': error_msg}, get_run_id())
            raise RuntimeError(error_msg)

    except Exception as e:
        print('Error executing ECS task:', str(e))
        splunk.log_message({'Status': 'failed', 'di-glue-b228_conversion': str(e)}, get_run_id())
        raise


if __name__ == "__main__":
    program_name = os.path.basename(sys.argv[0])
    main()
