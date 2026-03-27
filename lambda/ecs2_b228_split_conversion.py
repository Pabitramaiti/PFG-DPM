import boto3
import json
import re
import os
import datetime
from dpm_splunk_logger_py import splunk
import subprocess
##import click
##import cmd
from dpmdev_di_layer_common_functions import client_config
from dpmdev_di_layer_common_functions import functions

s3_client = boto3.client('s3')
Lambda = boto3.client('lambda')


def lambda_handler(event, context):
    '''
    This function will call the Docker Image using the container, cluster and task definition.
    The Docker Image is for the Java program that converts the B228 file from Ebcdic to Ascii json
    format.
    :param event:
    :param context:
    :return:
    '''

    splunk.log_message({'Message': "di-lambda-b228_conversion processing Started"}, context)

    #accessing environment variables
    #parms for the java program
    input_bucket = event['input_bucket']
    output_bucket = event['output_bucket']
    config_bucket = event['config_bucket']
    config_filename = event['config_filename']
    input_folder = event['input_folder']
    output_queue_folder = event['output_queue_folder']
    header_queue_folder = event['header_queue_folder']
    hold_queue_folder = event['hold_queue_folder']
    queue_folder = event['queue_folder']
    config_folder = event['config_folder']
    thread_model = event['thread_model']
    thread_num = event['thread_num']
    input_filepath_ebcdic = event['input_filepath_ebcdic']
    batch_size = event['batch_size']
    run_type = event['run_type']
    trid_file = event['trid_file']
    b1_settings = event['b1_settings']

    basename = input_filepath_ebcdic.rsplit(".txt", 1)[0]
    
    extension = '.json'
    # construct output ascii , trailer  and suppressed accounts filepaths
    output_filename_ascii = basename + '-ASCII' + extension
    output_filename_trailer = basename + '-TRAILER' + extension
    output_filename_sup = basename + '-SUPPRESSED' + extension


    #parms for the contaner, cluster and task definition
    cluster_name = os.environ.get('cluster_name')
##    task_role_arn = os.environ.get('task_role_arn')
    task_definition_name = os.environ.get('task_definition_name')
##    task_definition_name = 'dpmdev-di-testing-task-definition-ec2-b228-split-conversion'    
    
    print('task_definition_name -> '+ task_definition_name)
    container_name = os.environ.get('container_name')

    try:
    
	# print variables to log
        env_vars = {
            'input_bucket': input_bucket,
            'input_filepath_ebcdic': input_filepath_ebcdic,
            'config_bucket': config_bucket,
            'config_filename': config_filename,
            'output_bucket': output_bucket,
            'output_filename_ascii': output_filename_ascii,
            'output_filename_trailer': output_filename_trailer,
            'output_filename_sup': output_filename_sup,
            'input_folder': input_folder,
            'output_queue_folder': output_queue_folder,
            'header_queue_folder': header_queue_folder,
            'hold_queue_folder': hold_queue_folder,
            'queue_folder': queue_folder,
            'config_folder': config_folder,
            'thread_model': thread_model,
            'thread_num': thread_num,
            'batch_size': batch_size,
            'run_type': run_type,
            'trid_file': trid_file,
            'b1_settings': b1_settings,
        }

        print(env_vars)

        # trigger new ECS job to process the file
        client = boto3.client('ecs')

        response = client.run_task(
            cluster=cluster_name,
            launchType='EC2',
            taskDefinition=task_definition_name,
            count=1,
            overrides={
###                'taskRoleArn': task_role_arn,
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
                            {'name': 'CONFIG_FOLDER', 'value': config_folder},
                            {'name': 'THREAD_MODEL', 'value': thread_model},
                            {'name': 'THREAD_NUM', 'value': thread_num},
                            {'name': 'BATCH_SIZE', 'value': batch_size},                            
                            {'name': 'RUN_TYPE', 'value': run_type},                            
                            {'name': 'TRID_FILE', 'value': trid_file},                            
                            {'name': 'B_SETTINGS', 'value': b1_settings},                            
                        ]
                    }
                ]
            }
        )
#        print("response -> ")
#        print(response)
        return 'ecs run task response: %s' % response
        splunk.log_message({'Status': 'success', 'di-lambda-b228_conversion': input_filepath_ebcdic, 'Message': response}, context)

    except Exception as e:
        message = f"{str(e)}"
        splunk.log_message({'Status': 'failed', 'di-lambda-b228_conversion': task_definition_name, 'Message': message}, context)
        print('cluster_name -> '+ cluster_name)
        print('task_definition_name -> '+ task_definition_name)
        print('container_name -> '+ container_name)
        raise Exception("Failed to Process : di-lambda-b228_conversion" + message)



