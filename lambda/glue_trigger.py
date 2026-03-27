# import json
# import boto3
# import os
# import logging
# import datetime
# import urllib.parse
# from dpm_splunk_logger_py import splunk

# stepfunctions = boto3.client("stepfunctions")
# dynamodb = boto3.client("dynamodb")
# s3_res = boto3.resource("s3")

# # Get the table name from environment variable
# table_name = os.environ.get("DDBCONFIGTBL")
# region = os.environ['REGION']
# statemachine = os.environ['STATEMACHINE']
# dbkey = os.environ['DBKEY']
# trackrecords = os.environ['TRACKRECORDS']
# archive_bucket = os.environ['ARCHIVE_BUCKET']

# def lambda_handler(event, context):
#     # Get the S3 event record
#     print(f"s3 event print: {event}")
#     #  splunk.log_message({'message': 'br_icsdev_dpmtest_pp_config_ddbloader Lambda function invoked'}, context)

#     bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
#     object_key = event["Records"][0]["s3"]["object"]["key"]

#     # Split the file name and its current extension
#     base_name, old_extension = os.path.splitext(object_key)

#     if old_extension == ".zip":
#         # Create the new file name with the desired extension
#         copybook_json_key_name = base_name + ".txt"
#         # Retrieve config data from DynamoDB

#         object_key_txt = base_name + ".TXT"
#         cetera_configdata_json = retrieve_config_data(table_name, context)

#         if cetera_configdata_json is not None:
#             validate_config_res = validate_config_data(cetera_configdata_json, object_key, context)

#             if validate_config_res["compression_flag"]:
#                 validate_config_res["bucket"] = bucket_name
#                 validate_config_res["key"] = object_key
#                 validate_config_res["dbkey"] = dbkey
#                 validate_config_res["trackrecords"] = trackrecords
#                 validate_config_res["region"] = region
#                 validate_config_res["archive_bucket"] = archive_bucket

#                 # return sf_trigger(validate_config_res, context)

#             if validate_config_res["supplemental_flag"]:
#                 validate_config_res["dbkey"] = dbkey
#                 validate_config_res["trackrecords"] = trackrecords
#                 validate_config_res["region"] = region
#                 print(f"validate_config_res is {validate_config_res}")
#                 sf_output = get_aux_sf_output(cetera_configdata_json, object_key_txt, bucket_name, context)
#                 print(f"sf output is {sf_output}")
#                 update_dict(validate_config_res, sf_output)
#                 print("from supplemental_flag")
#                 print(validate_config_res)
#                 #  splunk.log_message({'message': 'br_icsdev_dpmtest_pp_config_ddbloader Lambda function processing supplemental file'}, context)
#                 return sf_trigger(validate_config_res, context)

#             if validate_config_res["copybook_flag"]:
#                 validate_config_res["copybook_bucket"] = bucket_name
#                 validate_config_res["copybook_key"] = copybook_json_key_name
#                 validate_config_res["json_bucket"] = bucket_name
#                 validate_config_res["dbkey"] = dbkey
#                 validate_config_res["trackrecords"] = trackrecords
#                 validate_config_res["region"] = region
#                 print("from copybook_flag")
#                 print(validate_config_res)
#                 #  splunk.log_message({'message': 'br_icsdev_dpmtest_pp_config_ddbloader Lambda function processing copybook file'}, context)
#                 sf_resp = sf_trigger(validate_config_res, context)
#                 print(sf_resp)
#             if validate_config_res["driver_flag"]:
#                 # perform validation on required params in client config details
#                 validate_config_res["client_config"] = json.dumps(cetera_configdata_json)
#                 validate_config_res["s3_bucket_input"] = bucket_name
#                 validate_config_res["s3_key_input"] = base_name+'.D02022023'
#                 validate_config_res["dbkey"] = dbkey
#                 validate_config_res["trackrecords"] = trackrecords
#                 validate_config_res["region"] = region
#                 #  splunk.log_message({'message': 'br_icsdev_dpmtest_pp_config_ddbloader Lambda function processing driver file'}, context)
#                 sf_resp = sf_trigger(validate_config_res, context)
#     else:
#         # Create the return response
#         resp = {
#             'statusCode': 200,
#             'body': {
#                 'message': "processing only zip files",
#                 'statusCode': 200
#             }
#         }
#         #  splunk.log_message({'message': 'br_icsdev_dpmtest_pp_config_ddbloader Lambda function processing non-zip file'}, context)
#         return resp

# def update_dict(dict_a, dict_b):
#     for key, value in dict_b.items(): 
#         dict_a[key] = value

# def retrieve_config_data(table_name, context):
#     try:
#         dynamodb_response = dynamodb.get_item(TableName=table_name, Key={"clientName": {"S": "Cetera"}})
#         configdata = dynamodb_response["Item"]["config_data"]["S"]
#         # print(f"this is the config data: {configdata}")
#         #  splunk.log_message({'message': 'br_icsdev_dpmtest_pp_config_ddbloader Lambda function successfully retrieved config data from DynamoDB'}, context)

#         return json.loads(configdata)

#     except Exception as e:
#         print(f"Failed to retrieve config data from DynamoDB: {str(e)}")
#         #  splunk.log_message({'message': 'br_icsdev_dpmtest_pp_config_ddbloader Lambda function failed to retrieve config data from DynamoDB'}, context)

#         return None

# def validate_config_data(cetera_configdata_json, object_key, context):
#     try:
#         validate_res = {
#             "supplemental_flag": False,
#             "driver_flag": False,
#             "copybook_flag": False,
#             "compression_flag": False
#         }

#         _, ext = os.path.splitext(object_key)
#         ext = ext.lower()[1:]

#         if ext in ["tar", "gz", "zip", "7z"]:
#             validate_res["compression_flag"] = True

#         # Set the flags based on the file name
#         for file_pre in cetera_configdata_json["fileParams"]:
#             if (object_key[:10] == file_pre[:10]) and (cetera_configdata_json["fileParams"][file_pre].get("fileType") == "aux") and ("copybook" not in object_key.lower()):
#                 validate_res["supplemental_flag"] = True
#             elif object_key[:10] == file_pre[:10] and cetera_configdata_json["fileParams"][file_pre].get("fileType") == "driver":
#                 validate_res["driver_flag"] = True

#         # Check if object_key contains "copybook"
#         if "copybook" in object_key.lower():
#             validate_res["copybook_flag"] = True

#         # Print validate_res for debugging
#         print(f"validate_res: {validate_res}")
#         #  splunk.log_message({'message': 'br_icsdev_dpmtest_pp_config_ddbloader Lambda function response after validating the file'}, context)

#         return validate_res
#     except Exception as e:
#         print("An error occurred:", e)
#         #  splunk.log_message({'message': 'br_icsdev_dpmtest_pp_config_ddbloader Lambda function encountered an error while validating the file'}, context)

# def get_aux_sf_output(cetera_configdata_json, object_key, bucket_name, context):
#     json_data = cetera_configdata_json
#     # assuming copybook input is also stored in the same bucket as aux and supp file
#     # copybook_bucket = json_data["s3Bucket"]
#     copybook_bucket = bucket_name
#     # copybook output json is stored in a different bucket for easy reuse
#     s3_bucket_json = s3_res.Bucket(copybook_bucket)
#     config_json = {}
#     file_params_json = {}
#     arguments = {}
#     copybook_key = None
#     # get all prefix of record supplemental file
#     keys_list = json_data["fileParams"].keys()
#     for i in keys_list:
#         if i in object_key and "copybookFilePrefix" in json_data["fileParams"][i].keys():
#             # fetching json copybook prefix from config
#             json_pattern = json_data["fileParams"][i]["copybookFilePrefix"]
#             for json_name in s3_bucket_json.objects.all():
#                 # pick the one that matches with key in s3 bucket
#                 if json_pattern in json_name.key and json_name.key.endswith("json"):
#                     copybook_key = json_name.key 
#                     break
#             file_params_json = json_data["fileParams"][i]
#             break

#     tablename = extract_file_name_prefix(object_key)
#     # getting all config in dbparams key[CETERA UMB] using file_params_json [CETERA -UMB] that is retrieved and stored in file_params_
#     config_json = json_data["dbParams"].get(file_params_json["dbTableName"])
#     if config_json:
#         columns = config_json['columns']
#         # if redefine-col is present in copybook send the attribute to check 
#         if 'redefineCol' in file_params_json:
#             arguments['redefine_col'] = file_params_json['redefineCol']
#         else:
#             arguments['redefine_col'] = "None"
#         try:
#             arguments.update({
#                 'copybook_bucket': copybook_bucket,
#                 'copybook_key': copybook_key,
#                 'record_key': object_key,
#                 'record_bucket': bucket_name,
#                 'table_name': tablename,
#                 'columns': json.dumps(columns)
#             })
#             print(arguments)
#             #  splunk.log_message({'message': 'br_icsdev_dpmtest_pp_config_ddbloader Lambda function AUX file arguments'}, context)

#         except Exception as e:
#             print(e)
#             #  splunk.log_message({'message': 'br_icsdev_dpmtest_pp_config_ddbloader Lambda function failed while loading AUX file arguments from config'}, context)

#     return arguments

# def sf_trigger(validate_config_res, context):
#     try:
#         # Convert the datetime objects to string representation
#         print(f"Triggering step fn {validate_config_res}")
#         # validate_config_res["createdDateTime"] = str(validate_config_res["createdDateTime"])
#         # validate_config_res["updatedDateTime"] = str(validate_config_res["updatedDateTime"])

#         sf_response = stepfunctions.start_execution(
#             stateMachineArn=statemachine,
#             input=json.dumps(validate_config_res)
#         )

#         # Extract the necessary fields
#         execution_arn = sf_response.get('executionArn')
#         status_code = sf_response.get('ResponseMetadata', {}).get('HTTPStatusCode')

#         # Create the return response
#         response = {
#             'statusCode': status_code,
#             'body': {
#                 'executionArn': execution_arn,
#                 'statusCode': status_code
#             }
#         }

#         #  splunk.log_message({'message': 'br_icsdev_dpmtest_pp_config_ddbloader Lambda function triggered step functions successfully'}, context, response)

#         return response
#     except Exception as e:
#         #  splunk.log_message({'message': 'br_icsdev_dpmtest_pp_config_ddbloader Lambda function failed to trigger step functions'}, context)
#         return str(e)

# def extract_file_name_prefix(file_name):
#     base_name = os.path.basename(file_name)  # Get the base name of the file
#     file_name_prefix = os.path.splitext(base_name)[0]  # Extract the prefix without the extension
#     return file_name_prefix


import json
import boto3
import os
import logging
import re
import datetime
import urllib.parse
from dpm_splunk_logger_py import splunk

def lambda_handler(event, context):
    
    # Get the S3 event record
    print(f"s3 event print: {event}")
    print(f"printing context {context}")

    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    object_key = event["Records"][0]["s3"]["object"]["key"]

    splunk.log_message({'Status': 'success', 'InputFileName': object_key, 'InputBucketName': bucket_name, 'Message': 'Lambda function is invoked'}, context)
    print(f"bucket name is :{bucket_name}")
    print(f"Input file name is {object_key}")

    file_name = object_key.split('/')[-1].rsplit(".", 1)[0]
    xsd_key = "pp_ref_files/" + file_name + ".xsd"
    glue = boto3.client("glue")
    
    #Name of the glue job
    # job_name = 'br_icsdev_dpmtest_xml_validation_against_xsd'
    job_name = os.environ['GLUEJOB']
    
    # Define the parameters to pass to the Glue job
    parameters = {
        "--s3_bucket_input": bucket_name,
        "--s3_key_input_xml": object_key,
        "--s3_key_input_xsd": xsd_key
    }

    print(f"Glue job parameters are: {parameters}")

    try:
        response = glue.start_job_run(JobName=job_name, Arguments=parameters)
        
        return {
            'statusCode': 200,
            'body': 'glue job started successfully!!!'
        }

    except Exception as e:
        print(f'Error starting glue job: {str(e)}')

        return {
            'statusCode': 500,
            'body': 'Error starting glue job'
        }

