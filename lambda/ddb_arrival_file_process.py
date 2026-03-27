import boto3
import json
import re
import os
import datetime
from dpm_splunk_logger_py import splunk
from boto3.dynamodb.conditions import Key, Attr
from dpmdev_di_layer_common_functions import client_config
from dpmdev_di_layer_common_functions import functions
from ddb_handler import dbhandler
import sys
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
import time

file_monitoring_table_dynamo = os.getenv("FILE_MONITORING_TABLE_DYNAMO")

stepfunctions = boto3.client("stepfunctions")
dynamodb_client = boto3.client('dynamodb')
s3_client = boto3.client('s3')
Lambda = boto3.client('lambda')

scheduler = boto3.client("scheduler")
dynamodb = boto3.resource("dynamodb")


objFM = dbhandler.DynamoDBHandler(file_monitoring_table_dynamo) # object of file monitoring

def weekdays_mapping():
    return {
        0: "Monday",
        1: "Tuesday",
        2: "Wednesday",
        3: "Thursday",
        4: "Friday",
        5: "Saturday",
        6: "Sunday"
    }

def lambd_payload(grouped_items,context):
    try:
        for group_name, group_items in grouped_items.items():
            grp_complete_action = group_items[0].get("grp_complete_action")
            grp_initial_status = group_items[0].get("grp_initial_status")
            grp_completion_status = group_items[0].get("grp_completion_status")
            action_type = grp_complete_action.get("action_type")
            output_prefix = group_items[0].get("output_prefix")
            schedule = group_items[0].get("schedule")
            filetype = group_items[0].get("filetype")
            schedule = group_items[0].get("schedule")
            splunk.log_message({'Message': "File Group Completed", "Status": "logging"}, context)
            if "file_merger" in action_type and group_items[0].get("grp_merge_status") != grp_completion_status and group_items[0].get("grp_merge_status") != "failed":
                action_type = "file_merger"
                return {
                        "LmbdaPayload":{
                            "grp_initial_status":grp_initial_status,
                            "grp_completion_status":grp_completion_status,
                            "file_monitoring_table":file_monitoring_table_dynamo,
                            "action_type":action_type,
                            "output_prefix":output_prefix,
                            "group_name":group_name,
                            "group_items":group_items
                            }
                        }
            elif "content_merger" in action_type and group_items[0].get("content_merge_status") != grp_completion_status:
                action_type = "content_merger"
                print(f"schedule: {schedule} | filetype: {filetype}")
                if schedule and filetype == "DLY":
                    start_time = schedule.get("start_time") 
                    end_time = schedule.get("end_time") 
                    processing_days = schedule.get("processing_day") 
                    current_time = datetime.datetime.now().time() 
                    current_weekday = datetime.datetime.now().weekday() 
                    start_time = datetime.datetime.strptime(start_time, "%H:%M").time() 
                    end_time = datetime.datetime.strptime(end_time, "%H:%M").time() 
                    target_tz = schedule.get("timezone")
                    current_tz = time.tzname
                    print(current_time, start_time, end_time, "current_time, start_time, end_time")
                    if is_in_time_window(target_tz,current_tz,processing_days, current_weekday,start_time,current_time, end_time):
                        print("in time window")
                        return {
                            "LmbdaPayload":{
                                "grp_initial_status":grp_initial_status,
                                "grp_completion_status":grp_completion_status,
                                "file_monitoring_table":file_monitoring_table_dynamo,
                                "action_type":action_type,
                                "output_prefix":output_prefix,
                                "group_name":group_name,
                                "group_items":group_items
                                }
                            }
                    else:
                        continue
                return {
                            "LmbdaPayload":{
                                "grp_initial_status":grp_initial_status,
                                "grp_completion_status":grp_completion_status,
                                "file_monitoring_table":file_monitoring_table_dynamo,
                                "action_type":action_type,
                                "output_prefix":output_prefix,
                                "group_name":group_name,
                                "group_items":group_items
                                }
                        }
        return None
    except Exception as e:
        splunk.log_message({'Message': str(e), "Status": "failed"}, context)
        raise e

def is_in_time_window(target_tz,current_tz,target_weekdays, current_weekday, start, current, end) -> bool:
    """
    Check if the current datetime is within the given weekday and time range.

    :param target_weekday: Weekday number (Monday=0, Sunday=6)
    :param start: datetime.time for start of range
    :param end: datetime.time for end of range
    :return: True if current weekday/time is in range, else False
    """

    print(f"target_tz: {target_tz} | current_tz: {current_tz} | target_weekdays: {target_weekdays} | current_weekday: {current_weekday}\
          | start: {start} | current: {current} | end: {end}")
    if (target_tz in current_tz) and (current_weekday in target_weekdays) and (start < current < end):
        return True
    return False

# Assuming `items` is your list of dicts from DynamoDB
def group_by_run_id_completed(items):
    try:
        print(items,"itemsitemsitemsitemsitemsitems")
        grouped = {}
        for item in items:
            grp_completion_status = item.get("grp_completion_status")
            schedule = item.get("schedule")
            # grp_merge_status 
            if item.get("grp_status") == grp_completion_status:  # filter completed only
                if item.get("grp_run_id") is None:
                    continue
                run_id = item.get("grp_run_id")
                if run_id not in grouped:
                    grouped[run_id] = [item]
                else:
                    grouped[run_id].append(item)
        return grouped
    except Exception as e:
        print("in exception")
        splunk.log_message({'Message': str(e), "Status": "failed"}, context)
        raise e

def lambda_handler(event, context):
    try:
        fetch_all_items = objFM.fetch_all_items()
        grouped_items = group_by_run_id_completed(fetch_all_items)
        # group items for daily files

        print(grouped_items,"grouped_items")
        payload = lambd_payload(grouped_items,context)
        if payload:
            return payload
            
        return {
            "LmbdaPayload":{
                "action_type":None,
                "comment": "NoGroup"
                }
            }
        
    except Exception as e:
        print("in exception")
        splunk.log_message({'Message': str(e), "Status": "failed"}, context)
        raise e