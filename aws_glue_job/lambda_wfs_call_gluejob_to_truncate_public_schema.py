import json
import boto3
import time

client = boto3.client('glue')

def callWfsGlueToTruncatePublicSchemaTables():
    # Start the Glue job
    response = client.start_job_run(JobName='pabi_glue_wfs_truncate_schema')
    job_run_id = response['JobRunId']
    print("Started Glue job:", job_run_id)

    # Poll until job completes
    while True:
        job_status = client.get_job_run(JobName='pabi_glue_wfs_truncate_schema',
                                        RunId=job_run_id,
                                        PredecessorsIncluded=False)
        state = job_status['JobRun']['JobRunState']
        print("Current state:", state)

        if state in ['SUCCEEDED', 'FAILED', 'STOPPED']:
            break
        time.sleep(5)  # wait before polling again

    # If your Glue job sets a message in 'JobRun' metadata, capture it
    # Otherwise, you may need to read CloudWatch logs for the actual text
    message = job_status['JobRun'].get('ErrorMessage', 'Glue job finished with state: ' + state)
    return message

def lambda_handler(event, context):
    message = callWfsGlueToTruncatePublicSchemaTables()
    return {
        'statusCode': 200,
        'body': json.dumps(message)
    }
