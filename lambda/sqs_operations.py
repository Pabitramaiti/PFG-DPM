import json
import boto3
import time
import re
from dpm_splunk_logger_py import splunk

# Initialize the SQS client
sqs = boto3.client('sqs')

def send_message(queue_url, bucket_name, message_body, message_attributes, deduplication_id, message_group_id):
    try:
        # Send message to SQS
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageGroupId=message_group_id,  # Ensures FIFO ordering by bucketName
            MessageBody=json.dumps(message_body),
            MessageAttributes=message_attributes,
            MessageDeduplicationId=deduplication_id  # Unique deduplication ID
        )

        # Return success response with message details
        return response

    except Exception as e:
        error_message = str(e)
        splunk.log_message({"Message": error_message, "Status": "Failed"}, context)
        return {
            'statusCode': 500,
            'body': json.dumps({"error": error_message})
        }
        
def receive_message(queue_url, max_messages, wait_time,filter_prefix):
    try:
        all_messages = []
        total_received = 0
        batch_size = 10  # SQS max limit per call

        # Calculate how many batches we need
        batches = (max_messages + batch_size - 1) // batch_size

        for _ in range(batches):
            remaining = max_messages - total_received
            current_batch_size = min(batch_size, remaining)
            
            # Receive messages from SQS
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=current_batch_size,
                # WaitTimeSeconds=wait_time,
                VisibilityTimeout=10,
                AttributeNames=['All'],  # Get all attributes for troubleshooting
                #MessageAttributeNames=['All']  # Get message attributes if needed
                MessageAttributeNames=['filter_prefix']
            )
                
            if not response or 'Messages' not in response:
                break  # No more messages

            last_request_id = response.get('ResponseMetadata', {}).get('RequestId')

            for message in response['Messages']:
                body = message['Body']

                # Parse the body from JSON string to Python dictionary
                body_json = json.loads(body)

                # Check if the sourceKeys starts with the filter prefix (if it exists in the parsed body)
                source_keys = body_json.get('fileName', '')  # Safely get sourceKeys, default to empty string if not found

                file_name = source_keys.split('/')[-1]  # Get the last part of the path (e.g., 'advisors_list_1.json')

                if file_name.startswith(filter_prefix):  # Filter messages starting with filter_prefix
                    all_messages.append(message)
                
            total_received = len(all_messages)

            # Stop if we've already gathered the requested number
            if total_received >= max_messages:
                break
            
        # Return the messages
        return {
            'Messages': all_messages,
            'MessageCount': len(all_messages),
            'RequestId': response['ResponseMetadata']['RequestId']
        }
        
    except Exception as e:
        error_message = str(e)
        splunk.log_message({"Message": error_message, "Status": "Failed"}, context)
        return {
            'statusCode': 500,
            'body': json.dumps({"error": error_message})
        }

def get_queue_attributes(queue_url):
    try:
        # Get queue attributes (e.g., ApproximateNumberOfMessages)
        queue_attributes_response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        
        #print(f"Queue attributes response: {json.dumps(queue_attributes_response, indent=4)}")
        
        # Extract the approximate number of messages from the response
        approximate_messages = queue_attributes_response.get('Attributes', {}).get('ApproximateNumberOfMessages', 'Unknown')
        
        return {
            'QueueAttributes': {
                'ApproximateNumberOfMessages': approximate_messages
            },
            'RequestId': queue_attributes_response['ResponseMetadata']['RequestId']
        }
    
    except Exception as e:
        error_message = str(e)
        splunk.log_message({"Message": error_message, "Status": "Failed"}, context)
        return {
            'statusCode': 500,
            'body': json.dumps({"error": error_message})
        }

def create_deduplication_id(bucket_name, file_name, execution_time):
    try:
        # Concatenate the values and sanitize the string
        combined_string = f"{bucket_name}-{file_name}-{execution_time}"
        #sanitized_string = re.sub(r'[^a-zA-Z0-9]', '', combined_string)
        #deduplication_id = sanitized_string[:32]
        deduplication_id = re.sub(r'[^a-zA-Z0-9]', '', combined_string)
        
        return deduplication_id
    
    except Exception as e:
        error_message = str(e)
        splunk.log_message({"Message": error_message, "Status": "Failed"}, context)
        return {
            'statusCode': 500,
            'body': json.dumps({"error": error_message})
        }

def delete_message(queue_url, receipt_handle):
    try:
        # Delete the message from the queue
        response = sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        
        #print(f"Deleted message with ReceiptHandle: {receipt_handle}")
        
        return {
            'MessageDeleted': True,
            'RequestId': response['ResponseMetadata']['RequestId']
        }
    
    except Exception as e:
        error_message = str(e)
        splunk.log_message({"Message": error_message, "Status": "Failed"}, context)
        return {
            'statusCode': 500,
            'body': json.dumps({"error": error_message})
        }            

def lambda_handler(event, context):
    try:
        action = event.get('Action')
        queue_url = event.get('QueueUrl')
    
        if not queue_url:
            raise ValueError("QueueUrl is required.")
    
        # Based on the action, call the corresponding function
        if action == 'SendMessage':
            bucket_name = event.get('bucketName')
            file_name = event.get('fileName')
            message_group_id = event.get('MessageGroupId')
            message_body = event.get('MessageBody')        
            message_attributes = event.get('MessageAttributes')
            execution_time = str(time.time()) 

            # Generate MessageDeduplicationId
            deduplication_id = create_deduplication_id(bucket_name, file_name, execution_time)

            send_messages = send_message(queue_url, bucket_name, message_body, message_attributes, deduplication_id, message_group_id)

            # Return success response with message details
            return {
                'statusCode': 200,
                'messageId': send_messages.get('MessageId'),
                'messageGroupId': message_group_id,
                'deduplicationId': deduplication_id
            }

        elif action == 'ReceiveMessage':
            max_messages = event.get('MaxNumberOfMessages', 1)
            wait_time = event.get('WaitTimeSeconds', 10)
            filter_prefix = event.get('filter_prefix')        

            if not filter_prefix:
                raise ValueError("The 'filter_prefix' parameter is required and cannot be null or empty.")
        
            received_messages = receive_message(queue_url, max_messages, wait_time, filter_prefix)
        
            # If there are no messages, return empty response
            if received_messages['MessageCount'] == 0:
                return {
                    'Messages': [],
                    'MessageCount': 0,
                    'RequestId': received_messages['RequestId']
                }
        
            # Process the received messages and delete them after processing
            for message in received_messages['Messages']:
                # Extract receipt handle from the message to delete it later
                receipt_handle = message['ReceiptHandle']
            
                # After processing the message, delete it from the queue
                delete_message(queue_url, receipt_handle)

            return {
                'Messages': received_messages['Messages'],
                'MessageCount': len(received_messages['Messages']),
                'RequestId': received_messages['RequestId']
            }
    
        elif action == 'GetQueueAttributes':
            return get_queue_attributes(queue_url)
    
        elif action == 'DeleteMessage':
            receipt_handle = event.get('ReceiptHandle')
            if not receipt_handle:
                raise ValueError("ReceiptHandle is required for DeleteMessage.")
            return delete_message(queue_url, receipt_handle)
    
        else:
            raise ValueError("Invalid Action specified.")

    except Exception as e:
        error_message = str(e)
        splunk.log_message({"Message": error_message, "Status": "Failed"}, context)
        return {
            'statusCode': 500,
            'body': json.dumps({"error": error_message})
        }            
