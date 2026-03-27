import json
import boto3
from botocore.exceptions import ClientError
import os
import re
from decimal import Decimal
from dpm_splunk_logger_py import splunk
from datetime import datetime, timedelta, date

# Create a DynamoDB resource
dynamodb = boto3.resource('dynamodb')
all_client_timings_table = os.getenv("ALL_CLIENT_TIMINGS_TABLE")


def lambda_handler(event, context):
    extract_event_setenviron(event, context)
    try:
        file_name = os.environ.get("file_name")
        multifileConfig = os.environ.get("multifileConfig")
        multifileTableName = os.environ.get("multifileTableName")
        periodDateRange = os.environ.get("periodDateRange")

    except Exception as e:
        raise ValueError("Failed to extract event info: " + str(e))

    today = date.today()
    formatted_date = today.strftime("%m/%d/%Y")

    splunk.log_message({'Message': "file_name", "file_name": file_name, "Status": "checking"}, context)

    # Parse the JSON string from the event
    records = json.loads(multifileConfig)

    table_timings = dynamodb.Table(all_client_timings_table)

    ##Name of the client table that will be created/tracked for file statuses
    table_name = json.loads(multifileTableName)

    period_date_range_str = json.loads(periodDateRange)

    selected_name = calc_periood_date(period_date_range_str, formatted_date, context)

    try:
        table = dynamodb.Table(table_name)
        table_description = table.table_status
        print(f"Table '{table_name}' exists. Status: {table_description}")
        splunk.log_message({'Message': "table exists", "table_name": table_name, "Status": "checking"}, context)
        add_items(table, records, selected_name, table_timings, file_name, formatted_date, context)

    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        print(f"Table '{table_name}' does not exist.")
        splunk.log_message({'Message': "table does not exist", "table_name": table_name, "Status": "checking"}, context)
        raise Exception(f"Table '{table_name}' does not exist and cannot be processed.")

    # The following line checks if all items in the table have status "COMPLETED" and deletes the table if true
    empty_table_if_completed(table_name, context, os.environ.get("input_bucket"), os.environ.get("inputPath"))


def calc_periood_date(period_date_range_str, formatted_date, context):
    try:
        for i, period_date_range in enumerate(period_date_range_str):

            date_range = period_date_range['daterange']
            period_name = period_date_range['name']
            current_day_month_array = formatted_date.split("/")
            current_day = int(current_day_month_array[1])

            comparison_expression = f"{current_day} {date_range}"

            result = eval(comparison_expression)

            if result:
                return period_name

    except Exception as e:
        message = f"{str(e)}"
        splunk.log_message({'Message': "Error in calc_periood_date", "Message": message, "Status": "FAILED"},
                           context)
        raise Exception("Error in calc_periood_date: " + message)


def add_items(table, records, selected_name, table_timings, file_name, formatted_date, context):
    try:
        # Check if the table has any rows
        response = table.scan(Limit=1)  # Limit to 1 item for efficiency
        has_rows = 'Items' in response and len(response['Items']) > 0

        for i, record in enumerate(records):
            if not has_rows:
                # Perform the else portion first if the table has no rows
                response = table.get_item(Key={'sequence': i})
                existing_item = response.get('Item', {})

                base_filename = existing_item.get('base_filename', '').strip()
                status = existing_item.get('status', '').strip()

                if not base_filename or not status:
                    matching_period = next((p for p in record['period'] if p['name'] == selected_name), None)
                    if matching_period:
                        code_value = matching_period['timing']
                        calc_expected_date = get_calculation(table_timings, code_value, context)

                        table.put_item(
                            Item={
                                'sequence': i,
                                'base_filename': record['filename'],
                                'filename': ' ',
                                'period': matching_period['name'],
                                'expected_date': calc_expected_date,
                                'arrival_date': '',
                                'status': 'NOT STARTED'
                            }
                        )

            # Perform the if portion
            ##            if record['filename'] in file_name:
            ## if file_name.startswith(f"{record['filename']}_"):

            # Only run this block if file_name does NOT start with "bios_wfs_out_recon"
            print("file_name", file_name)
            print("record['filename']", record['filename'])

            if "out_recon" not in file_name and file_name.startswith(f"{record['filename']}"):
                ##if not file_name.startswith("bios_wfs_out_recon") and file_name.startswith(f"{record['filename']}"):
                # block 1
                matching_period = next((p for p in record['period'] if p['name'] == selected_name), None)
                if matching_period:
                    code_value = matching_period['timing']
                    calc_expected_date = get_calculation(table_timings, code_value, context)
                    table.put_item(
                        Item={
                            'sequence': i,
                            'base_filename': record['filename'],
                            'filename': file_name,
                            'period': matching_period['name'],
                            'expected_date': calc_expected_date,
                            'arrival_date': formatted_date,
                            'status': 'COMPLETED'
                        }
                    )
            ##elif re.match(rf"^{re.escape(record['filename'])}_\d{{2}}\d{{2}}\d{{5}}\.csv$", file_name):
            elif re.match(rf"^{re.escape(record['filename'])}_\d{{14}}\.csv$", file_name):
                # block 2
                matching_period = next((p for p in record['period'] if p['name'] == selected_name), None)
                if matching_period:
                    code_value = matching_period['timing']
                    calc_expected_date = get_calculation(table_timings, code_value, context)
                    table.put_item(
                        Item={
                            'sequence': i,
                            'base_filename': record['filename'],
                            'filename': file_name,
                            'period': matching_period['name'],
                            'expected_date': calc_expected_date,
                            'arrival_date': formatted_date,
                            'status': 'COMPLETED'
                        }
                    )

    except Exception as e:
        print(f"An error occurred: {str(e)}")


def extract_event_setenviron(event_data, context):
    try:
        for key, value in event_data.items():
            os.environ[key] = value
    except Exception as e:
        raise ValueError("Failed to extract event info: " + str(e))


def get_calculation(table_timings, code_value, context):
    try:
        response = table_timings.get_item(
            Key={'Code': code_value}
        )

        # print(f"this is table_times ----> {table_timings}")
        # print(f"This is code value ---> {code_value}")
        item = response.get('Item')

        # print(f"This is item ---> {item}")

        if item:
            today = datetime.now()
            ##Override for Last busness day of the Month
            if code_value == "L1":
                if today.month == 12:
                    next_month = today.replace(year=today.year + 1, month=1, day=1)
                else:
                    next_month = today.replace(month=today.month + 1, day=1)
                # Last day of current month
                last_day = next_month - timedelta(days=1)
                # Adjust if weekend
                if last_day.weekday() == 5:  # Saturday
                    last_day -= timedelta(days=1)
                elif last_day.weekday() == 6:  # Sunday
                    last_day -= timedelta(days=2)

                results = last_day

            ## Calendar Day following the last Business day of the month
            elif code_value == "L2":
                if today.month == 12:
                    next_month = today.replace(year=today.year + 1, month=1, day=1)
                else:
                    next_month = today.replace(month=today.month + 1, day=1)
                # Last day of current month
                last_day = next_month - timedelta(days=1)
                # Adjust if weekend
                if last_day.weekday() == 5:  # Saturday
                    last_business_day = last_day - timedelta(days=1)
                elif last_day.weekday() == 6:  # Sunday
                    last_business_day = last_day - timedelta(days=2)
                else:
                    last_business_day = last_day

                # Day after last business day
                results = last_business_day + timedelta(days=1)

            else:
                calculation = item.get('Calculation')
                results = eval(calculation)  # Evaluate the calculation expression

            # Assuming the result is a datetime object, extract the day of the month
            day_of_month = results

            date_formatted = day_of_month.strftime("%m/%d/%Y")

            return date_formatted
        else:
            return "no date found"
    except Exception as e:
        return f"An error occurred: {str(e)}"


def empty_table_if_completed(table_name, context, bucket_name, input_path):
    try:
        # Get the table
        table = dynamodb.Table(table_name)

        # Scan the table to retrieve all items
        response = table.scan()
        items = response.get('Items', [])

        # Check if all rows have the status "COMPLETED"
        all_completed = all(item.get('status') == 'COMPLETED' for item in items)

        if all_completed:
            # Use batch_writer to delete all items in the table
            with table.batch_writer() as batch:
                for item in items:
                    batch.delete_item(Key={'sequence': item['sequence']})
            print(f"All rows in table '{table_name}' have been deleted as all rows had status 'COMPLETED'.")
            splunk.log_message({'Message': f"All rows in table '{table_name}' deleted", "Status": "SUCCESS"}, context)
            write_empty_file_to_s3(bucket_name, input_path, context)
        else:
            print(f"Table '{table_name}' rows cannot be deleted as not all rows have status 'COMPLETED'.")
            splunk.log_message({'Message': f"Table '{table_name}' rows not deleted", "Status": "FAILED"}, context)
    except ClientError as e:
        message = f"{str(e)}"
        splunk.log_message({'Message': "Error in empty_table_if_completed", "Message": message, "Status": "FAILED"},
                           context)
        raise Exception("Error in empty_table_if_completed: " + message)


def write_empty_file_to_s3(bucket_name, input_path, context):
    try:
        # Generate the file name with the required naming convention
        timestamp = datetime.now().strftime("%Y-%m-%dT%H%M%S")
        ##        file_name = f"multifile-validation-trigger_{timestamp}.trg"
        file_name = f"WFSSTATEMENT_{timestamp}.starttask"

        # Combine the folder path and file name
        s3_key = f"{input_path.rstrip('/')}/{file_name}"

        # Create an S3 client
        s3_client = boto3.client('s3')

        # Upload an empty file to the S3 bucket
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body="")

        print(f"Empty file '{s3_key}' has been written to bucket '{bucket_name}'.")
        splunk.log_message({'Message': f"Empty file '{s3_key}' written to S3", "Status": "SUCCESS"}, context)
    except Exception as e:
        message = f"{str(e)}"
        splunk.log_message({'Message': "Error in write_empty_file_to_s3", "Message": message, "Status": "FAILED"},
                           context)
        raise Exception("Error in write_empty_file_to_s3: " + message)
