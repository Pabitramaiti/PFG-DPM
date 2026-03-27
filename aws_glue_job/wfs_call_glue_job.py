import boto3

def lambda_handler(event, context):
    glue = boto3.client('glue')
    response = glue.start_job_run(
        JobName='pabi_wfs_recon_job',
        # optional job arguments
        Arguments={'--param1': 'value1'
        ,'--param2': 'value2'
        }
    )
    print("Started Glue job:", response['JobRunId'])
#return {
#    'statusCode': 200,
#    'body': json.dumps(message)
#}
