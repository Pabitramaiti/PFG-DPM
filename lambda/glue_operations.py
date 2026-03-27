import json
import boto3
import time
from dpm_splunk_logger_py import splunk

glue_client = boto3.client('glue')  # Create a Glue client

def check_glue_job_status(job_run_id, job_name, max_retries):
    retries = 0
    job_status = None

    # Poll the status of the Glue job every 10 seconds until it finishes
    while retries < max_retries:
        try:
            # Get the current status of the Glue job
            response = glue_client.get_job_run(
                JobName=job_name,
                RunId=job_run_id
            )
        
            # Get the job status from the response
            job_status = response['JobRun']['JobRunState']

            # If the job is finished, exit the loop
            if job_status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
                break

            # If the job is still running, wait for 10 seconds before checking again
            time.sleep(10)
            retries += 1
    
        except glue_client.exceptions.EntityNotFoundException:
            raise ValueError(f"Glue job with JobRunId {job_run_id} not found.")
    
        except Exception as e:
            raise Exception(f"Error while checking Glue job status: {str(e)}")

    return job_status
    
def lambda_handler(event, context):
    try:
        action = event.get('Action')
        
        if action == 'GlueJobStatus':
            job_run_id = event.get('JobRunId')
            job_name = event.get('JobName')
            max_retries = event.get('MaxRetries', 5)  # Default to 5 retries if not provided

            if not job_run_id:
                raise ValueError("JobRunId is required in the input event")

            # Call the function to check Glue job status
            job_status = check_glue_job_status(job_run_id, job_name, max_retries)

            # Return the result once the job is done
            if job_status == 'SUCCEEDED':
                return {
                    'Status': 'SUCCEEDED',
                    'Message': 'Glue job succeeded',
                    'JobRunId': job_run_id,
                    'JobRunState': job_status
                }
            else:
                return {
                    'Status': 'FAILED',
                    'Message': f'Glue job failed with status: {job_status}',
                    'JobRunId': job_run_id,
                    'JobRunState': job_status
                }

        else:
            return {
                'Error': 'Invalid Action specified in the event.',
                'StatusCode': 400
            }

    except Exception as e:
        error_message = str(e)
        splunk.log_message({"Message": error_message, "Status": "Failed"}, context)
        return {
            'statusCode': 500,
            'body': json.dumps({"error": error_message})
        }
