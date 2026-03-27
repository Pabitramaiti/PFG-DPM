import splunk
import traceback
import boto3 # type: ignore

def copyToDestination(srcUri, destUri, context):
    
    try:    
        srcBucket, srcKey = processS3Uri(srcUri)
        destBucket, destKey = processS3Uri(destUri)
        boto3.client('s3').copy_object(Bucket=destBucket, CopySource={'Bucket': srcBucket, 'Key': srcKey}, Key=destKey)
        #splunk.log_message({'Status': 'success', 'Message': f'Moved file from {srcUri} to {destUri}'}, context)
    except Exception:
        splunk.log_message({'Status': 'failed', 'Message': traceback.format_exc()}, context)
        raise ValueError(traceback.format_exc())

def processS3Uri(uri):
    pvtIndex = uri.index('/')
    srcBucket, srcKey = uri[:pvtIndex], uri[pvtIndex + 1:]

    return srcBucket, srcKey