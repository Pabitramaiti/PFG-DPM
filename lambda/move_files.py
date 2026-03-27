import boto3
import json
import os
def copy_objects_within_bucket(bucketName, sourcePrefix, destinationPrefix):
    
    s3 = boto3.resource('s3')
    try:
        bucket = s3.Bucket(bucketName)
        response =[]
        for obj in bucket.objects.filter(Prefix=sourcePrefix):
            copySource = {
                'Bucket': bucketName,
                'Key': obj.key
            }
            destinationKey = obj.key.replace(sourcePrefix, destinationPrefix, 1)
            bucket.copy(copy_source, destinationKey)
            
            response.append(obj.key)
        return response
        
    except Exception as e:
        raise e
    
def lambda_handler(event, context):
    try:
        # Retrieve input parameters from the Step Functions event
        bucket_name = event['bucketName']

        # Specify the source and destination prefixes (folders)
        source_prefix = 'fcsa/source/'
        destination_prefix = 'fcsa/to_transmission/'
        
        # Copy objects from source prefix to destination prefix within the bucket
        res =copy_objects_within_bucket(bucket_name, source_prefix, destination_prefix)

        response = {
            'statusCode': 200,
            'body': json.dumps(f'{res}\nprocessed successfully')
        }

        return response
    except Exception as e:
        raise 
   