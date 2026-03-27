import json
import boto3
from botocore.exceptions import ClientError
import os
from decimal import Decimal
from dpm_splunk_logger_py import splunk
from datetime import datetime, timedelta, date

# Create a DynamoDB resource
dynamodb = boto3.resource('dynamodb')
all_client_config = os.getenv("ALL_CLIENT_CONFIG_DYNAMO")
all_client_timings_table = os.getenv("ALL_CLIENT_TIMINGS_TABLE")


def lambda_handler(event, context):
    splunk.log_message({'Message': "Multi File Evaluation Lambda triggered successfully"}, context)

    try:
        table = dynamodb.Table(all_client_config)
        table_description = table.table_status

    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        message = "ERROR The all_client_config table does not exist ->" + all_client_config
        splunk.log_message({'Message': message, "all_client_config": all_client_config, "Status": "failed"}, context)
        raise ValueError(message)

    # Use the scan operation to retrieve all items in the table
    response = table.scan()

    # Extract the items from the response
    items = response.get('Items', [])

    # Loop through each item
    for item in items:
        multifile_table_name = ""
        ##        records = ""
        splunk.log_message({'Message': "item", "item": item, "Status": "checking"}, context)
        client_name_key = item.get('clientName', None)
        client_config = item.get('clientConfig', None)
        print(f"client_name_key name: {client_name_key}")
        print(f"client_config table name: {client_config}")
        client_table_name = client_config
        table = dynamodb.Table(client_table_name)
        response = table.get_item(
            Key={
                'clientName': client_name_key,
            }
        )
        # Extract the multifileTableName value
        client_config_json = response['Item']['clientConfig']

        # Parse the JSON data
        try:
            client_config = json.loads(client_config_json)
            multifile_table_name = client_config['multifileTableName']
            print(f"multifile_table_name name: {multifile_table_name}")
            multifile_data = client_config['multifileData']
            print(f"multifile_data data: {multifile_data}")
            period_date_range = client_config['periodDateRange']
            print(f"period_date_range data: {period_date_range}")


        except json.JSONDecodeError:
            print("Error: Unable to parse JSON data")

        records = json.loads(json.dumps(multifile_data))
        period_date_range_str = json.loads(json.dumps(period_date_range))
        splunk.log_message(
            {'Message': "period_date_range_str", "period_date_range_str": period_date_range_str, "Status": "checking"},
            context)

        # All client timings table
        table_timings = dynamodb.Table(all_client_timings_table)

        today = date.today()
        formatted_date = today.strftime("%m/%d/%Y")

        selected_name = calc_periood_date(period_date_range_str, formatted_date, context)
        splunk.log_message(
            {'Message': "selected_name", "selected_name": selected_name,
             "Status": "checking"},
            context)

        table_name = multifile_table_name
        try:
            table = dynamodb.Table(table_name)
            table_description = table.table_status
            print(f"Table '{table_name}' exists. Status: {table_description}")
            splunk.log_message({'Message': "table exists", "table_name": table_name, "Status": "checking"}, context)
            check_date(table, formatted_date, context)

        except dynamodb.meta.client.exceptions.ResourceNotFoundException:
            print(f"Table '{table_name}' does not exist.")
            splunk.log_message({'Message': "table does NOT exist", "table_name": table_name, "Status": "checking"},
                               context)
            # Call the function to create the table
            create_table(table_name, context)
            add_items(table, records, selected_name, table_timings, formatted_date, context)


def create_table(table_name, context):
    try:
        # Create the DynamoDB table.
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'sequence',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'sequence',
                    'AttributeType': 'N'
                },
            ],
            BillingMode='PAY_PER_REQUEST'  # Set to on-demand capacity
#            ProvisionedThroughput={
#                'ReadCapacityUnits': 5,
#                'WriteCapacityUnits': 5
#            }
        )

        # Wait for the table to be created
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

        print("Table created successfully.")
    except ClientError as e:
        message = f"{str(e)}"
        splunk.log_message({'Message': "Error creating table:", "Message": message, "Status": "FAILED"},
                           context)
        raise Exception("Error creating table: " + message)


def calc_periood_date(period_date_range_str, formatted_date, context):
    try:
        for i, period_date_range in enumerate(period_date_range_str):
            splunk.log_message(
                {'Message': "period name INSIDE", "period name": period_date_range['name'], "Status": "checking"},
                context)

            splunk.log_message(
                {'Message': "period date range INSIDE", "period date range": period_date_range['daterange'],
                 "Status": "checking"},
                context)

            date_range = period_date_range['daterange']
            period_name = period_date_range['name']
            current_day_month_array = formatted_date.split("/")
            current_day = int(current_day_month_array[1])

            splunk.log_message(
                {'Message': "date_range", "date_range": date_range,
                 "Status": "checking"},
                context)

            splunk.log_message(
                {'Message': "current_day", "current_day": current_day,
                 "Status": "checking"},
                context)

            comparison_expression = f"{current_day} {date_range}"

            result = eval(comparison_expression)

            splunk.log_message(
                {'Message': "result INSIDE", "result": result,
                 "Status": "checking"},
                context)

            if result:
                return period_name

    except Exception as e:
        message = f"{str(e)}"
        splunk.log_message({'Message': "Error in calc_periood_date", "Message": message, "Status": "FAILED"},
                           context)
        raise Exception("Error in calc_periood_date: " + message)


def add_items(table, records, selected_name, table_timings, formatted_date, context):
    try:
        for i, record in enumerate(records):
            splunk.log_message({'Message': "filename INSIDE", "filename": record['filename'], "Status": "checking"},
                               context)
            # Find the matching period based on the selected name
            matching_period = next((p for p in record['period'] if p['name'] == selected_name), None)
            if matching_period:
                code_value = matching_period['timing']
                splunk.log_message({'Message': "timing", "timing": code_value, "Status": "checking"}, context)

                calc_expected_date = get_calculation(table_timings, code_value, context)

                table.put_item(
                    Item={
                        'sequence': i,  # Replace with your sequence logic
                        'base_filename': record['filename'],
                        'filename': ' ',
                        'period': matching_period['name'],
                        'expected_date': calc_expected_date,
                        'arrival_date': '',
                        'status': 'NOT STARTED'
                    }
                )
    except Exception as e:
        message = f"{str(e)}"
        splunk.log_message({'Message': "Error in add_items", "Message": message, "Status": "FAILED"},
                           context)
        raise Exception("Error in add_items: " + message)


def check_date(table, formatted_date, context):
    try:
        # Query for items with expected date greater than today and status not "COMPLETED"
        late_list = []
        response = table.scan()

        for item in response['Items']:
            '''
            expected_date = datetime.strptime(item['expected_date'], "%m/%d/%Y")
            today = datetime.now()
            '''
            ### 2044
            expected_date = datetime.strptime(item['expected_date'], "%m/%d/%Y").date()
            today = datetime.now().date()

            ###            if expected_date < today and item['status'] != 'COMPLETED':
            if expected_date < today and item['status'] != 'COMPLETED' and item['status'] != 'LATE':
                table.update_item(
                    Key={'sequence': item['sequence']},
                    UpdateExpression="set #status = :status",
                    ExpressionAttributeNames={'#status': 'status'},
                    ExpressionAttributeValues={':status': 'LATE'}
                )
                late_list.append(item['base_filename'])
                #####ADD CODE HERE
        #late_length = len(late_list)
        if late_list:
            late_files_str = ", ".join(late_list)
            splunk.log_message({'Message': f"The following files are late: {late_files_str}", "Status": "FAILED"}, context)
            raise Exception("The following files are late: " + late_files_str)



    except ClientError as e:
        message = f"{str(e)}"
        splunk.log_message({'Message': "Error in check_date", "Message": message, "Status": "FAILED"},
                           context)
        raise Exception("Error in check_date: " + message)


def get_calculation(table_timings, code_value, context):
    try:
        response = table_timings.get_item(
            Key={'Code': code_value}
        )
        item = response.get('Item')
        if item:
            calculation = item.get('Calculation')
            splunk.log_message({'Message': "calculation", "calculation": calculation, "Status": "checking"}, context)

            results = eval(calculation)  # Evaluate the calculation expression

            # Assuming the result is a datetime object, extract the day of the month
            day_of_month = results

            date_formatted = day_of_month.strftime("%m/%d/%Y")
            splunk.log_message({'Message': "date_formatted", "date_formatted": date_formatted, "Status": "checking"},
                               context)

            return date_formatted
        else:
            ##        return f"No item found for Code: {code_value}"
            return "no date found"

    except Exception as e:
        message = f"{str(e)}"
        splunk.log_message({'Message': "Error in get_calculation:", "Message": message, "Status": "FAILED"},
                           context)
        raise Exception("Error in get_calculation: " + message)
