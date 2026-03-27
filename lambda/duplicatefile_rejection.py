import os
import json
import boto3
from dpm_splunk_logger_py import splunk
from boto3.dynamodb.types import TypeDeserializer

# dynamodb = boto3.resource('dynamodb')
dynamodb = boto3.client('dynamodb')

def reject_duplicate_file(table_name, file_name, context):
    '''function to reject file based on 
    file_name and isRejected = False'''
    
    # Initialize the exclusive start key for pagination
    exclusive_start_key = None

    # Initialize variables
    all_items = []
    result = {'Items': []}
    
    try:
        # Initialize query params
        keyconditionexpression = "#fileName = :fileValue"
        filterexpression="#isRejected = :bool"
        expressionattributenames = {
            "#fileName": "fileName",
            "#isRejected": "isRejected"
        }
        expressionattributevalues = {
            ":fileValue": {'S': file_name},
            ":bool": {'BOOL': False}
        }
        while True:
            # Perform the query with pagination support
            if exclusive_start_key:
                response = dynamodb.query(
                    TableName=table_name,
                    KeyConditionExpression=keyconditionexpression,
                    FilterExpression=filterexpression,
                    ExpressionAttributeNames=expressionattributenames,
                    ExpressionAttributeValues=expressionattributevalues,
                    ExclusiveStartKey=exclusive_start_key
                )
            else:
                response = dynamodb.query(
                    TableName=table_name,
                    KeyConditionExpression=keyconditionexpression,
                    FilterExpression=filterexpression,
                    ExpressionAttributeNames=expressionattributenames,
                    ExpressionAttributeValues=expressionattributevalues
                )

            # Access the items returned and add them to the all_items list
            items = response['Items']
            all_items.extend(items)

            # Check if there are more pages; if not, break the loop
            if 'LastEvaluatedKey' not in response:
                break
            exclusive_start_key = response['LastEvaluatedKey']

        if len(all_items) > 0:
            deserializer = TypeDeserializer()
            
            def deserialize(value):
                '''function to deserialize the json returned from dynamodb
                due to query operation'''
                
                if isinstance(value, dict):
                    # If the dictionary has one of the DynamoDB type keys, deserialize it
                    if set(value.keys()) & {'S', 'N', 'B', 'BOOL', 'NULL', 'M', 'L', 'SS', 'NS', 'BS'}:
                        return deserializer.deserialize(value)
                    else:
                        return {k: deserialize(v) for k, v in value.items()}
                elif isinstance(value, list):
                    return [deserialize(v) for v in value]
                else:
                    return value

            result = {'Items': deserialize(all_items)}
        
        # table = dynamodb .Table(table_name)
        # response = table.scan(
        #     FilterExpression="contains(#pk, :val) AND isRejected = :bool_val",
        #     ExpressionAttributeNames={
        #         "#pk": "fileId"
        #     },
        #     ExpressionAttributeValues={
        #         ":val": file_name,
        #         ":bool_val": False
        #     }
        # )
        # item = response.get('Items')
        
        if len(result['Items']) > 0:
            update_expressions = "SET isRejected = :rejectedValue"
            primary_key ={
                "fileName":{'S' : file_name},
                "transactionId":{'S' : result['Items'][0]['transactionId']}
            }
            response = dynamodb.update_item(
                TableName=table_name,
                UpdateExpression=update_expressions,
                Key=primary_key,
                ExpressionAttributeValues={
                    ":rejectedValue": {'BOOL' : True}
                }
    
            )
            splunk.log_message({'Message':f'{file_name} is updated.', 'Status':'Success'}, context)
            return f'{file_name} is rejected'
        else:
            splunk.log_message({'Message':f'{file_name} already rejected or doesn\'t exist', 'Status':'Success'}, context)
            return f'{file_name} already rejected or doesn\'t exist'

    except Exception as e:
        splunk.log_message({'Message':f'{e}' , 'fileName': file_name, 'Status':'Failed', }, context)
        raise e


def lambda_handler(event, context):
    
    if 'body' in event:
        response = reject_duplicate_file(event['body']['table_name'], event['body']['file_name'], context)
    else:
        response = reject_duplicate_file(event['table_name'], event['file_name'], context)
    
    return {
        'statusCode': 200,
        'body': response
    }
