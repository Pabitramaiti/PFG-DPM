import json
import boto3
import os
from decimal import Decimal
from dpm_splunk_logger_py import splunk
# from boto3.dynamodb.conditions import Key
from datetime import datetime
# import uuid

#accessing environment variables
Destination_Table = os.environ.get('Table')
pkey = os.environ.get('PartitionKey')
skey = os.environ.get('SortKey')

#creating instance of DynamoDB resource
dynamoDBResource = boto3.resource('dynamodb')

#Creating instance of target table in DynamoDB
track_table = dynamoDBResource.Table(Destination_Table)

def lambda_handler(event, context):
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
        splunk.log_message(response, context)
        raise ValueError("body parameter is empty")
        # return response
    #checking if partition and sort key are present in passed parameters
    if pkey in body and skey in body:
        req_keys = {'GroupStep', 'StepFunctions', 's3Buckets', 'StartDateTime', 'ClientID', 'ClientName', 'EndDateTime', 'Status', 'msg'}
        # if all(key in body for key in req_keys):
        snd_keys = []
        timeformatchk = []
        dttimeformat = "%Y-%m-%d %H:%M:%S"
        for key in req_keys:
            if key not in body:
                snd_keys.append(key)
            if key in {'EndDateTime', 'StartDateTime'}:
                #validating the datatime formats
                try:
                    datetime.strptime(body[key], dttimeformat)
                except ValueError:
                    timeformatchk.append(key)
        if len(snd_keys) != 0:
            raise ValueError("please provide required keys"+str(snd_keys))
        elif timeformatchk:
            raise ValueError('please send correct datetime formats for '+str(timeformatchk))
            
        StepFunctions = []
        sfjson = {
            "Arn": body['StepFunctions'],
            "StartTime": body['StartDateTime']
            }
        StepFunctions.append(sfjson)
        s3Buckets = []
        s3Buckets.append(body['s3Buckets'])
        ModifiedDateTime = get_transaction_start_time()
        body.update({"ModifiedDateTime" : get_transaction_start_time(), "StepFunctions": StepFunctions, "s3Buckets": s3Buckets})    
        
        #Additional code for failure
        #---------------------------------------------------------
        #Extract actual error message from log incase of failure
        if body['Status'] == "Failed":
            temp_msg = body['msg']
            error_dict = json.loads(temp_msg)
            error_dict_capitalized = {key.upper():value for key,value in error_dict.items()}
            error_msg = error_dict_capitalized["ERRORMESSAGE"]
            body.update({"msg" : error_msg})    
        #---------------------------------------------------------
        
        #checking if item is present in dynamodb with same primary key
        response = getUMBitem(body[pkey], body[skey], context)
        if 'Item' in response:
            #modifying the parameters
            response = modifyUMBItem(body, context)
            return BuildResponse(200, response)
        #   query_res = track_table.query(
        #       KeyConditionExpression=Key(pkey).eq(body[pkey]) & Key(skey).eq(body[skey])
        #       )
        #   if query_res['Items']:
        #       response = modifyUMBItem(body, context)
        #       return response
            #saving passed parameters directly
        return saveItem(body, context)
        # else:
        #     return 'please provide required parameters'
            
    else:
        return BuildResponse(500, {'error': 'Api call parameters has no part key and sort key'})

#function to save the passed parameters in the DynamoDB table
def saveItem(requestBody, context):
    try:
        response = track_table.put_item(Item = requestBody)
        splunk.log_message({'event': response, 'message': f"successfully saved Item with partition key: {requestBody[pkey]} and sort key: {requestBody[skey]} in dynamodb table"}, context)
        return BuildResponse(200, response)
        
    except Exception as e:
        splunk.log_message({"status code": 500,"message":'OOps!!! Error saving Table Items, Following exception occurred -> {}'.format(e)}, context)
        raise e

#function to get the item from the DynamoDB table
def getUMBitem(part_key, range_key, context):
    try:
        response = {}
        response = track_table.get_item(
            Key = {
                pkey : part_key,
                skey : range_key
            }
        )
        if 'Item' in response:
            splunk.log_message({'event': json.dumps(response, cls=CustomEncoder), 'message': f'successfully retrieved Item with partition key: {part_key} and sort key: {range_key} from dynamodb table'}, context)
            return response
        else:
            splunk.log_message({'event': json.dumps(response, cls=CustomEncoder), 'message': 'umb_key : %s or sort_key : %s not found'%(part_key, range_key)}, context)
            return BuildResponse(404, {'message': 'part_key : %s or sort_key : %s not found'%(part_key, range_key)})
    except Exception as e:
        splunk.log_message({'event': json.dumps(response, cls=CustomEncoder), 'message': f'Failed to fetch record with partition key: {part_key} and sort key: {range_key} from dynamodb table due to:'+str(e)}, context)
        raise e

#function to update the passed parameters in the DynamoDB table                
def modifyUMBItem(updatedict, context):
    try:
        
        response = {}
        Exp_Attribute_Values = {
            ':val' : updatedict['StepFunctions'],
            ':objs': updatedict['s3Buckets'],
            ':status': updatedict['Status'],
            ':strt_time': updatedict['StartDateTime'],
            ':end_time': updatedict['EndDateTime'],
            ':Md_Strt_Time': updatedict['ModifiedDateTime'],
            ':msg': updatedict['msg'],
            ':grp_step': updatedict['GroupStep']
        }
        Exp_Attribute_Names = {
            '#sfn': 'StepFunctions',
            '#s3': 's3Buckets',
            '#status': 'Status',
            '#strt_time': 'StartDateTime',
            '#end_time': 'EndDateTime',
            '#Md_Strt_Time': 'ModifiedDateTime',
            '#msg': 'msg',
            '#grp_step': 'GroupStep'
        }
        Update_Expression = "SET #sfn = list_append(#sfn, :val), #s3 = list_append(#s3, :objs), #status = :status, #strt_time = :strt_time, #end_time = :end_time, #Md_Strt_Time = :Md_Strt_Time, #msg = :msg, #grp_step = :grp_step"
        
        primarykey = {
            pkey : updatedict[pkey],
            skey : updatedict[skey]
        }
        # del updatedict[pkey], updatedict[skey]
        # i = 1
        # for key,value in updatedict.items():
        #     Exp_Attribute_Values[":val"+str(i)] = value,
        #     Exp_Attribute_Names["#name"+str(i)] = key,
        #     Update_Expression = Update_Expression + str("set {} = {},".format(Exp_Attribute_Names["#name"+str(i)], Exp_Attribute_Values[":val"+str(i)]))
        #     i+=1
        
        response = track_table.update_item(
            Key = primarykey,
            UpdateExpression = Update_Expression,
            ExpressionAttributeValues = Exp_Attribute_Values,
            ExpressionAttributeNames = Exp_Attribute_Names,
            ReturnValues = 'UPDATED_NEW'
            )
        if 'Attributes' in response:
            splunk.log_message({'event': json.dumps(response, cls=CustomEncoder), 'message': f"successfully updated Item with partition key: {updatedict[pkey]} and sort key: {updatedict[skey]} from dynamodb table"}, context)
            return response
        else:
            splunk.log_message({'event': json.dumps(response, cls=CustomEncoder), 'message': 'part_key : %s or sort_key : %s not found'%(updatedict[pkey], updatedict[skey])}, context)
            raise KeyError("part_key : %s or sort_key : %s not found"%(updatedict[pkey], updatedict[skey]))
            
    except Exception as e:
        splunk.log_message({'event': json.dumps(response, cls=CustomEncoder), 'message': f"Failed to update record with partition key: {updatedict[pkey]} and sort key: {updatedict[skey]} from dynamodb table due to:"+str(e)}, context)
        raise e

#get transaction time
def get_transaction_start_time():
    try:
        startDateTime = datetime.now()
        startDateTime = startDateTime.strftime("%Y-%m-%d %H:%M:%S")
        return startDateTime
    except Exception as e:
        raise e
                
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