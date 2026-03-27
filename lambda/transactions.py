import json
import boto3
import os
from decimal import Decimal
from dpm_splunk_logger_py import splunk
from datetime import datetime, timedelta
from boto3.dynamodb.types import TypeDeserializer

#accessing environment variables
destination_table = os.environ.get('table')
pkey = os.environ.get('partition_key')
skey = os.environ.get('sort_key')

#creating instance of DynamoDB resource
dynamo_db_resource = boto3.resource('dynamodb')
dynamodb = boto3.client('dynamodb')

# creating instance of s3 resource
s3_client = boto3.client('s3')

#Creating instance of target table in DynamoDB
track_table = dynamo_db_resource.Table(destination_table)
FileMonitor_tb = os.environ.get("FileMonitor_TB")
transaction_tb =  os.environ.get("Transaction_TB")



def transaction_lambda_handler(event, context):
    
    body = {}
        
    if event is not None:
        #if the body is in dictionary format
        if isinstance(event, dict):
            body = event
        else:
            #else converting to dictionary
            body = json.loads(event)
    else:
        response = {
            'statusCode': 500,
            'body': {
                'message': "body parameter is empty",
            }
        }
        splunk.log_message({"message": response, "Status": "Failed"}, context)
        raise ValueError("body parameter is empty")
        
    #checking if partition and sort key are present in passed parameters
    if pkey in body and skey in body:
        
        req_keys = {'fileId', 'isRejected', 'stepFunctions', 'startDateTime', 'clientID', 
                'clientName', 'endDateTime', 'fileStatus', 'fileLog', 'documentType', 'arnStepName','sfInstance', 
                'sfStatus', 'sfLog', 'sfStartTime', 'sfEndTime', 'sfInputFile', 'sfOutputFile'
                }
                
        param_validation(body, req_keys, context)
        
        #checking if secondary index key is present
        if 'secIndexKeyName' in body:
            if body['secIndexKeyValue'] != "":
                body[body['secIndexKeyName']] = body['secIndexKeyValue']
                del body['secIndexKeyName'], body['secIndexKeyValue'] #deleting secondary key and value from body
            else:
                del body['secIndexKeyName'], body['secIndexKeyValue'] #deleting secondary key and value from body
        
        transaction_time = get_transaction_start_time()
        
        #Additional code for failure
        #---------------------------------------------------------
        #Extract actual error message from log incase of failure
        if body['sfStatus'].lower() == "failed":
            bucket = body['sfInputFile'].split('/', 1)[0]
            input_path = body['inputPath']
            destination_path = input_path.replace('input', 'failed')
            move_file_to_failed(bucket, input_path, destination_path, body['fileName'], context)
            temp_msg = body['sfLog']
            if is_json(temp_msg):
                error_dict = json.loads(temp_msg)
                error_dict_capitalized = {key.upper():value for key,value in error_dict.items()}
                if "ERRORMESSAGE" in error_dict_capitalized:
                    error_msg = error_dict_capitalized["ERRORMESSAGE"]
                    body.update({"sfLog" : error_msg, "fileLog": error_msg, "endDateTime": transaction_time})
                else:
                    body.update({"sfLog" : temp_msg, "fileLog": temp_msg, "endDateTime": transaction_time})
            else:
                body.update({"sfLog" : temp_msg, "fileLog": temp_msg, "endDateTime": transaction_time})
            del body['inputPath']
        #---------------------------------------------------------
        if '.' not in body['sfStartTime']:
            body['sfStartTime'] = body['sfStartTime'].replace('Z','.000Z')
            
        sfjson = {
                "arnStepName": body['arnStepName'],
                "sfInstance": body['sfInstance'],
                "sfStatus": body['sfStatus'],
                "sfLog": body['sfLog'],
                "sfStartTime": datetime.strptime(body['sfStartTime'], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S:%f"),
                "sfEndTime": transaction_time,
                "sfInputFile": body['sfInputFile'],
                "sfOutputFile": body['sfOutputFile']
                }
                
        del body['arnStepName'], body['sfInstance'], body['sfStatus'], body['sfStartTime'], body['sfEndTime'], body['sfInputFile'], body['sfOutputFile'], body['sfLog']
    
        body['stepFunctions'][0] = sfjson
        body.update({'modifiedDateTime' : body['startDateTime']})
        
        #checking if item is present in dynamodb with same primary key
        response = filter_table(body['fileName'], False, context)
        if len(response['Items'])>0:
            #modifying the parameters
            res = response['Items']
            body['transactionId'] = res[0]['transactionId']
            body["endDateTime"] = transaction_time
            response = modify_umb_item(body, res[0]['stepFunctions'], context)
            return build_response(200, response)
            #saving passed parameters directly
            
        del body['initStep'], body['endStep']
        return save_item(body, sfjson, context)
            
    else:
        splunk.log_message({"Message": "failed because Api call parameters has no part key and sort key", "Status": "Failed"}, context)
        return build_response(500, {'error': 'Api call parameters has no part key or sort key or both'})

def param_validation(body, req_keys, context):
    '''
    to validate if we are getting all the params expected and
    to validate the datetime format of certain params
    '''
    snd_keys = []
    time_format_chk = []
    dt_time_format = "%Y-%m-%d %H:%M:%S:%f"
    for key in req_keys:
        if key not in body:
            snd_keys.append(key)
        if key in {'startDateTime'}:
            #validating the datatime formats
            try:
                datetime.strptime(body[key], dt_time_format)
            except ValueError:
                time_format_chk.append(key)
                
    if len(snd_keys) != 0:
        splunk.log_message({"Message": "failed because you need to provide the required keys "+str(snd_keys), "Status": "Failed"}, context)
        raise ValueError("please provide required keys"+str(snd_keys))
    elif time_format_chk:
        splunk.log_message({"Message": "failed because you need to send the correct datetime formats for "+str(time_format_chk), "Status": "Failed"}, context)
        raise ValueError('please send correct datetime formats for '+str(time_format_chk))

#function to save the passed parameters in the DynamoDB table
def save_item(request_body, sfjson, context):
    try:
        request_body['stepFunctions'][0] = sfjson
        response = track_table.put_item(Item = request_body)
        splunk.log_message({'event': response, 'message': f"successfully saved Item with partition key: {request_body[pkey]} and sort key: {request_body[skey]} in dynamodb table"}, context)
        return build_response(200, response)
        
    except Exception as e:
        splunk.log_message({"status code": 500,"message":'OOps!!! Error saving Table Items, Following exception occurred -> {}'.format(e), "Status": "Failed"}, context)
        raise e

def filter_table(fileName, isRejected, context):
    '''
    function to query the dynamodb table
    based on fileName and isRejected attributes
    '''
    
    # Initialize the exclusive start key for pagination
    exclusive_start_key = None
    
    # Initialize variables
    all_items = []
    result = {'Items': []}
    
    try:
        # Initialize query params
        keyconditionexpression="#fileName = :fileValue"
        expressionattributenames={
            "#isRejected": "isRejected",
            "#fileName": "fileName"
        }
        expressionattributevalues={
            ":fileValue": {'S': fileName},
            ":isRejected": {'BOOL': isRejected}
        }
        filterexpression="#isRejected = :isRejected"
    
        response = dynamodb.query(
                    TableName=destination_table,
                    KeyConditionExpression=keyconditionexpression,
                    ExpressionAttributeNames=expressionattributenames,
                    ExpressionAttributeValues=expressionattributevalues,
                    FilterExpression=filterexpression
        )
        
        all_items = response['Items']
        
        while 'LastEvaluatedKey' in response:
            # Perform the query with pagination support
            response = dynamodb.query(
                TableName=destination_table,
                KeyConditionExpression=keyconditionexpression,
                ExpressionAttributeNames=expressionattributenames,
                ExpressionAttributeValues=expressionattributevalues,
                FilterExpression=filterexpression,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items = response['Items']
            all_items.extend(items)
        
        if len(all_items)>0:
            deserializer = TypeDeserializer()
            
            def deserialize(value):
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
        
        if len(result['Items'])>0:
             splunk.log_message({'event': json.dumps(result, cls=custom_encoder), 'message': f'successfully retrieved Item with {fileName} and {isRejected} from dynamodb table'}, context)
             return result
        else:
            return result
    except Exception as e:
        splunk.log_message({'event': json.dumps(result, cls=custom_encoder), 'message': f'Failed to fetch record with {fileName} and {isRejected} from dynamodb table due to:'+str(e), "Status": "Failed"}, context)
        raise e
        

def modify_umb_item(update_dict, old_dict, context):
    '''
    function to update the passed parameters into the DynamoDB table
    This function will only update already existing params in a record of dynamodb
    '''
    try:
        response = {}
        
        exp_attribute_values = {
            ':val' : update_dict['stepFunctions'][0],
            ':status': update_dict['fileStatus']
        }
        exp_attribute_names = {
            '#sfn': 'stepFunctions',
            '#status': 'fileStatus'
        }
        
        update_expression = "SET #sfn = :val, #status = :status"
        
        # if update_dict['stepFunctions'][0]['arnStepName'].lower() == "validation":
        if update_dict['initStep'] == 'True':
            exp_attribute_names['#strt_time'] = 'startDateTime'
            exp_attribute_values[':strt_time'] = update_dict['startDateTime']
            exp_attribute_names['#md_strt_time'] = 'modifiedDateTime'
            exp_attribute_values[':md_strt_time'] = update_dict['modifiedDateTime']
            update_expression = "SET #sfn = :val, #status = :status, #strt_time = :strt_time, #md_strt_time = :md_strt_time"
        # elif update_dict['stepFunctions'][0]['arnStepName'].lower() == "archival":
        if update_dict['endStep'] == 'True':
            exp_attribute_names['#msg'] = 'fileLog'
            exp_attribute_values[':msg'] = update_dict['fileLog']
            exp_attribute_names['#end_time'] = 'endDateTime'
            exp_attribute_values[':end_time'] = update_dict['endDateTime']
            update_expression = "SET #sfn = :val, #status = :status, #end_time = :end_time, #msg = :msg"
        
        del update_dict['initStep'], update_dict['endStep']
        
        pos = 0
        for key in old_dict:
            if update_dict['stepFunctions'][0]['arnStepName'].lower() == key['arnStepName'].lower():
                old_dict[pos] = update_dict['stepFunctions'][0]
                update_dict['stepFunctions'] = old_dict
                exp_attribute_names['#sfn'] = 'stepFunctions'
                exp_attribute_values[':val'] = update_dict['stepFunctions']
                break
            pos += 1
        
        if len(old_dict) == pos:
            old_dict.append(update_dict)
            update_dict['stepFunctions'] = old_dict
            exp_attribute_names['#sfn'] = 'stepFunctions'
            exp_attribute_values[':val'] = update_dict['stepFunctions']
        
        primary_key = {
            pkey : update_dict[pkey],
            skey : update_dict[skey]
        }
        
        response = track_table.update_item(
            Key = primary_key,
            UpdateExpression = update_expression,
            ExpressionAttributeValues = exp_attribute_values,
            ExpressionAttributeNames = exp_attribute_names,
            ReturnValues = 'UPDATED_NEW'
            )
        if 'Attributes' in response:
            splunk.log_message({'event': json.dumps(response, cls=custom_encoder), 'message': f"successfully updated Item with partition key: {update_dict[pkey]} and sort key: {update_dict[skey]} from dynamodb table"}, context)
            return response
        else:
            splunk.log_message({'event': json.dumps(response, cls=custom_encoder), 'message': 'failed due to part_key : %s or sort_key : %s not found'%(update_dict[pkey], update_dict[skey]), "Status": "Failed"}, context)
            raise KeyError("part_key : %s or sort_key : %s not found"%(update_dict[pkey], update_dict[skey]))
            
    except Exception as e:
        splunk.log_message({'event': json.dumps(response, cls=custom_encoder), 'message': f"Failed to update record with partition key: {update_dict[pkey]} and sort key: {update_dict[skey]} from dynamodb table due to:"+str(e), "Status": "Failed"}, context)
        raise e

#get transaction time
def get_transaction_start_time():
    try:
        start_date_time = datetime.now()
        start_date_time = start_date_time.strftime("%Y-%m-%d %H:%M:%S:%f")
        return start_date_time
    except Exception as e:
        raise e

#finds if jsonstring or not
def is_json(string):
    try:
        json.loads(string)
    except ValueError:
        return False
    return True
                
#class to convert object decimal to float
class custom_encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
    
        return json.JSONEncoder.default(self, obj)

#sending the response back to the triggering action    
def build_response(status_code, body=None):
    response = {
        'statusCode': status_code
    }
    if body is not None:
        #converting the decimal received from DynamoDB using the Custom_Encoder class
        response['body'] = json.dumps(body, cls=custom_encoder)
    return response

# function to fmove file to failed folder
def move_file_to_failed(bucket_name, input_path, output_path, file_name, context):

    '''
    function to move the file which triggered the process to the failed folder
    '''
    try:
        # Get the list of objects in the S3 bucket at the specified path
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=input_path)
        
        # Check if 'Contents' is in the response (it's not if the path is empty)
        if 'Contents' not in response:
            message = f'{bucket_name} is empty'
            splunk.log_message({'Status': 'failed', 'Message': message}, context)
            raise KeyError(message)
        
        found = False
        
        # Loop through all files in the specified path
        for obj in response['Contents']:
            if file_name in obj['Key']:
                # copy the source file from the source S3 bucket
                found = True
                source_key = input_path + file_name
                destination_key = output_path + file_name
                copy_source = {'Bucket': bucket_name, 'Key': source_key}
                response = s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=destination_key)
            
                # Delete the source file from the source S3 bucket
                s3_client.delete_object(Bucket=bucket_name, Key=source_key)
                message = f'{file_name} is moved from {input_path} to {output_path} within {bucket_name}'
                splunk.log_message({'Status': 'success', 'Message': message}, context)

        if not found:
            message = f'{file_name} not found in {input_path}'
            splunk.log_message({'Status': 'failed', 'Message': message}, context)
            raise KeyError(message)
    except Exception as e:
        message = f'failed to move {file_name} from {input_path} to {output_path} within {bucket_name} due to: {str(e)}'
        splunk.log_message({'Status': 'failed', 'Message': message}, context)
        raise Exception(message)

# +++++++++++++++++++++++++++++ file Monitor functions starts +=============================================


def create_timestamp(time):
    # Get current time
    current_time = datetime.now()

    timestamp = current_time + timedelta(minutes=time)

    # Return the timestamp as a string
    return timestamp.strftime('%Y-%m-%d %H:%M:%S')


def is_timestamp_expired(timestamp_str):
    # Convert timestamp string to datetime object
    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')

    # Get current time
    current_time = datetime.now()

    # Check if current time is greater than the timestamp
    return current_time > timestamp

def insert_data(FileMonitor_tb, transactionId, fileName, json_Data, nextStepName, context):
    # Define the item to be inserted
    item = {
        'TransactionID': {'S': f'{transactionId}'},  # Replace 'transaction_id_value' with the actual transaction ID
        'FileName': {'S': f'{fileName}'},  # Replace 'file_name_value' with the actual file name
        'JsonData': {'S': json.dumps(json_Data)}  # Replace {'key': 'value'} with your JSON data
    }

    # Insert the item into the DynamoDB table
    try:
        response = dynamodb.put_item(
            TableName=FileMonitor_tb,
            Item=item
        )
        return response
    except Exception as e:
        splunk.log_message({'Status': f'failed in inserting the data in {FileMonitor_tb} with transactionId {transactionId} for {nextStepName}', "FileName": fileName,'Message': str(e)},  context)
        raise e


def delete_item(FileMonitor_tb, transactionId, file_name, nextStepName, context):
    # Define the key of the item to be deleted
    key = {
        'TransactionID': {'S': f'{transactionId}'},  # Replace 'transaction_id_value' with the actual transaction ID
        'FileName': {'S': f'{file_name}'}  # Replace 'file_name_value' with the actual file name
    }

    # Delete the item from the DynamoDB table
    try:
        dynamodb.delete_item(
            TableName=FileMonitor_tb,
            Key=key
        )
    except Exception as e:
        splunk.log_message({'Status': f'failed in deleting item from {FileMonitor_tb} with transactionId {transactionId} for step {nextStepName}', "FileName": file_name, 'Message': str(e)},context)
        raise e


def fetch_item_by_id(FileMonitor_tb, fileName, nextStepName, context):
    # Fetch the item from the DynamoDB table
    try:
        response = dynamodb.query(
            TableName=FileMonitor_tb,
            KeyConditionExpression='fileName = :tid',
            ExpressionAttributeValues={
                ':tid': {'S': fileName}
            }
        )
        items = response.get('Items', [])
        if items:
            return items
        return None

    except Exception as e:
        splunk.log_message({'Status': f'failed to get items from {FileMonitor_tb} table for step {nextStepName}', "FileName": fileName, 'Message': str(e)}, context)
        raise e


def fetch_items(FileMonitor_tb, context):
    try:
        response = dynamodb.scan( 
            TableName=FileMonitor_tb
        )
        if 'Items' in response and not response['Items']:
            return None
        else:
            return response['Items']
    except Exception as e:
        splunk.log_message({'Status': f'failed to fetch items from {FileMonitor_tb} table', 'Message': str(e)}, context)
        raise e


def raise_Notification(fileName, nextStepName, context):
    splunk.log_message({'Status': 'failed', 'FileName': fileName, 'Message': f"input is not received to trigger {nextStepName} within the timelimit"}, context)

def file_exists(bucket_name, key):
    try:
        s3 = boto3.client('s3')
        response=s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except Exception as e:
        if e.response['Error']['Code'] == '404':
           return False
        raise e
        

def filemonitor_lambda_handler(event, context):

    nextStepName = event["nextstepname"]

    timeStamp = create_timestamp(event["timestampinminutes"])
    fileExpiryDurationInMinutes=create_timestamp((event["timestampinminutes"]+event["fileExpiryDurationInMinutes"]))


    json_data = {
            "nextStepName": f"{nextStepName}",
            "timeStamp": f"{timeStamp}",
            "fileExpireTimeStamp": f"{fileExpiryDurationInMinutes}",
            "reported": False
            
        }
    
    t_data = fetch_item_by_id(transaction_tb, event['fileName'], nextStepName, context)
   

    insert_data(FileMonitor_tb, t_data[0]["transactionId"]['S'], event['fileName'], json_data, nextStepName, context)
    
# +++++++++++++++++++++++++++++ main Functions +=============================================

def lambda_handler(event, context):
    try:
    
        if 'detail-type' in event: 
            items = fetch_items(FileMonitor_tb, context)
            if items is not None:
                for item in items:
                    json_Data = json.loads(item['JsonData']['S'])
                    transaction_data = fetch_item_by_id(transaction_tb, item['FileName']['S'], json_Data["nextStepName"], context)
                    fileName=transaction_data[0]["fileName"]['S']
                    if transaction_data is not None:
                        sfs = transaction_data[0]["stepFunctions"]["L"]
                        bucket_name = sfs[0]["M"]["sfInputFile"]["S"].split('/')[0]
                        source_key = sfs[0]["M"]["sfInputFile"]["S"].replace(bucket_name + "/", "", 1).replace(fileName,"")
                        print(source_key) 
                        for sf in sfs:
                            if sf["M"]["arnStepName"]["S"] == json_Data["nextStepName"]:

                                # bucket_name = sf["M"]["sfInputFile"]["S"].split('/')[0]
                                

                                nextStepName = json_Data["nextStepName"]
                                if not is_timestamp_expired(json_Data["fileExpireTimeStamp"]):
                                    if is_timestamp_expired(json_Data["timeStamp"]) and json_Data["reported"]!= True :
                                        Table_FileMonitor = dynamo_db_resource.Table(FileMonitor_tb)
                                        transactionId= item['TransactionID']['S']
                                        print("============")
                                        print(transactionId)
                                        print(fileName)
                                        Table_FileMonitor.delete_item( Key={'TransactionID': transactionId, "FileName": fileName})
                                        json_Data["reported"] =True
                                        insert_data(FileMonitor_tb, item['TransactionID']['S'], fileName, json_Data, nextStepName, context)

                                        splunk.log_message({'Status': 'failed', "FileName": transaction_data[0]["fileName"]['S'],
                                                        'Message': f"input is not received to trigger {nextStepName} within the timelimit"},
                                                           context)
                                    elif "sfStartTime" in sf["M"]:
                                        delete_item(FileMonitor_tb, transaction_data[0]["transactionId"]['S'],
                                                    transaction_data[0]["fileName"]['S'], nextStepName, context)
                                        splunk.log_message({'Status': 'success', "FileName": transaction_data[0]["fileName"]['S'],
                                                            'Message': f"{nextStepName} transaction is successful within given time"},
                                                           context)
                                else:
                                    if file_exists(bucket_name, source_key+fileName):
                                        move_file_to_failed(bucket_name, source_key, source_key.replace('input', 'failed'), transaction_data[0]["fileName"]['S'], context)
                                        splunk.log_message(
                                            {
                                                'Status': 'success',
                                                'FileName': transaction_data[0]['fileName']['S'],
                                                'Message': f"Input not received to trigger {nextStepName} within the time limit. Moved to failed."
                                            },
                                            context
                                        )
                                    delete_item(FileMonitor_tb, transaction_data[0]["transactionId"]['S'],
                                                transaction_data[0]["fileName"]['S'], nextStepName, context)

                                    splunk.log_message(
                                        {
                                            'Status': 'success',
                                            'Message': f"Input not received to trigger {nextStepName} within the time limit. deleted the entry at filemonitor."
                                        },
                                        context
                                    )
        else :
            
            if "createFileMonitorFlag" in event and event["createFileMonitorFlag"]:
                
                filemonitor_lambda_handler(event, context)
            
            transaction_lambda_handler(event, context)
            
    except Exception as e:
        raise e

