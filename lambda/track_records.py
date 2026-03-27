import json
import boto3
import datetime
import uuid
import os
from decimal import Decimal
from dpm_splunk_logger_py import splunk

os.chdir('/opt/')
step = None

Destination_Table = os.environ.get('DDBTRACKRECORDS')
#creating instance of DynamoDB resource
dynamoDBResource = boto3.resource('dynamodb')

def lambda_handler(event, context):
    body = {}
        
    if event['body'] is not None:
        #if the body is in dictionary format
        if isinstance(event['body'], dict):
            body = event['body']
        else:
            #else converting to dictionary
            body = json.loads(event['body'])
    else:
        response = {
            'statusCode': 500,
            'body': {
                'message': "body parameter is empty",
            }
        }
        splunk.log_message(response, context)
        return response
        
    current_time = datetime.datetime.now()
    body['TRANSACTION_ID'] = str(uuid.uuid1())
    body['CURRENT_TIME'] = str(current_time)
    
    return saveItem(body, context)

#defining a function to save the response in the DynamoDB table    
def saveItem(requestBody, context):
    try:
        #Creating instance of specified table in DynamoDB
        track_table = dynamoDBResource.Table(Destination_Table)
        response = track_table.put_item(Item = requestBody)
        splunk.log_message({'event': response, 'message': 'successfully updated dynamodb table'}, context)
        return BuildResponse(200, response)
        
    except Exception as e:
        
        splunk.log_message({"status code": 500,"message":'OOps!!! Error saving Table Items, Following exception occurred -> {}'.format(e)}, context)
        return  BuildResponse(500, {
                'error': 'OOps!!! Error saving Table Items, Following exception occurred -> {}'.format(e)})
                
                
#class to convert object decimal to float
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
    
        return json.JSONEncoder.default(self, obj)

#sending the response back to the triggering action    
def BuildResponse(statusCode, body=None):
    response = {
        'statusCode': statusCode
    }
    if body is not None:
        #converting the decimal received from DynamoDB using the Custom_Encoder class
        response['body'] = json.dumps(body, cls=CustomEncoder)
    return response