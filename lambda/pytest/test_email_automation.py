import pytest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import json
import io
from email_automation import lambda_handler,read_file_to_json,format_email_body,fetch_matching_keys_in_folder,download_file_from_s3,send_email_with_attachment


# Define a fixture for the context
@pytest.fixture
def context():
    return "arn:dpm:lambda:email_automation:187777304606"

@patch('email_automation.read_file_to_json')
def test_format_email_body(mock_read_file_to_json, context):
    # Define the input parameters
    input_format = 'json'
    input_bucket = 'test_bucket'
    email_input_file = 'test_file_key'
    output_format = 'columnar'
    output_delimiter = ','
    # Define the expected JSON content
    expected_json_content = {"header1": "value1", "key2": "value2"}

    # Define the expected email body
    expected_email_body = "value1\nkey2,value2\n"

    # Set the return value of the read_file_to_json function
    mock_read_file_to_json.return_value = expected_json_content

    # Call the format_email_body function
    result = format_email_body(input_format, input_bucket, email_input_file, output_format, output_delimiter, context)

    # Check the result
    assert result == expected_email_body

    # Check if the read_file_to_json function was called with the correct arguments
    mock_read_file_to_json.assert_called_once_with(input_bucket, email_input_file, context)

    #test condition with invalid value for input_format
    input_format = 'json1'
    with pytest.raises(ValueError):
        result = format_email_body(input_format, input_bucket, email_input_file, output_format, output_delimiter, context)

    # test condition with invalid value for output_format
    input_format = 'json'
    output_format = 'columnar1'
    with pytest.raises(ValueError):
        result = format_email_body(input_format, input_bucket, email_input_file, output_format, output_delimiter, context)

    # test condition where read file to json doesnt return a valid output
    output_format = 'columnar'
    mock_read_file_to_json.return_value = ""
    with pytest.raises(Exception):
        result = format_email_body(input_format, input_bucket, email_input_file, output_format, output_delimiter, context)


@patch('email_automation.s3_client')
def test_fetch_matching_keys_in_folder(mock_s3_client):
    # Define the bucket name, folder prefix and pattern
    bucket_name = 'test_bucket'
    folder_prefix = 'test_folder/'
    pattern = 'test_file.txt'

    # Define the response from the list_objects_v2 method
    response = {
        'Contents': [
            {'Key': 'test_folder/test_file.txt'},
            {'Key': 'test_folder/another_file.txt'}
        ]
    }

    mock_s3_client.list_objects_v2.return_value = response
    # Call the fetch_matching_keys_in_folder method
    result = fetch_matching_keys_in_folder(bucket_name, folder_prefix, pattern)
    # Check the result
    assert result == 'test_folder/test_file.txt'

    # test case where there is no match.
    pattern = 'test_file1.txt'
    result = fetch_matching_keys_in_folder(bucket_name, folder_prefix, pattern)
    # Check the result
    assert result == ''

@patch('email_automation.s3_client')
def test_download_file_from_s3_exception(mock_s3_client):
    # Arrange
    bucket_name = "test_bucket"
    object_key = "test_key"
    destination_path = "/tmp/test_file"
    expected_error_message = "Failed to download file from S3: An error occurred (404) when calling the test_operation operation: Not Found"

    error = {
        'Error': {
            'Code': '404',
            'Message': 'Not Found'
        }
    }
    operation_name = 'test_operation'
    mock_s3_client.download_file = MagicMock(side_effect=ClientError(error, operation_name))

    # Act
    with patch('builtins.print') as mock_print:
        download_file_from_s3(bucket_name, object_key, destination_path)

    # Assert
    mock_print.assert_called_once_with(expected_error_message)

@patch('email_automation.s3_client')
def test_download_file_from_s3(mock_s3_client):
    # Define the bucket name, object key and destination path
    bucket_name = 'test_bucket'
    object_key = 'test_object_key'
    destination_path = 'test_destination_path'

    # Create a mock s3 client
    # mock_s3_client = MagicMock()
    # mock_boto3_client.return_value = mock_s3_client

    #Call the download_file_from_s3 method
    try:
        download_file_from_s3(bucket_name, object_key, destination_path)
    except ClientError as e:
        assert False, f"Unexpected ClientError: {e}"

    # Check if the download_file method was called with the correct arguments
    mock_s3_client.download_file.assert_called_once_with(bucket_name, object_key, destination_path)

@patch('boto3.client')
def test_send_email_with_attachment(mock_boto3_client, context):
    # Define the input parameters
    sender_email = 'test_sender_email'
    recipient_emails = ['test_recipient_email']
    cc_emails = ['test_cc_email']
    subject = 'test_subject'
    body_text = 'test_body'
    attachment_file_path = 'test_attachment_file_path'
    bucket_name = 'test_bucket'
    object_key = 'test_object_key'
    attachment_flag = False

    # Define the expected response
    expected_response = 'Email sent! Message ID: test_message_id'

    # Create a mock SES client
    mock_ses_client = MagicMock()
    mock_ses_client.send_raw_email.return_value = {'MessageId': 'test_message_id'}

    # Assign the mock SES client to the return value of boto3.client
    mock_boto3_client.return_value = mock_ses_client

    # Call the send_email_with_attachment function
    result = send_email_with_attachment(sender_email, recipient_emails, cc_emails, subject, body_text, attachment_file_path, bucket_name, object_key, attachment_flag)

    # Check the result
    assert result == expected_response

    # Check if the send_raw_email method was called
    mock_ses_client.send_raw_email.assert_called()

@patch('boto3.client')
def test_send_email_with_attachment_no_credentials(mock_boto3_client):
    # Arrange
    sender_email = "test_sender@test.com"
    recipient_emails = ["test_recipient@test.com"]
    cc_emails = ["test_cc@test.com"]
    subject = "Test Subject"
    body_text = "Test Body Text"
    attachment_file_path = "/tmp/test_file"
    bucket_name = "test_bucket"
    object_key = "test_key"
    attachment_flag = False

    mock_ses_client = MagicMock()
    mock_ses_client.send_raw_email.side_effect = NoCredentialsError()
    mock_boto3_client.return_value = mock_ses_client

    # Act
    response = send_email_with_attachment(sender_email, recipient_emails, cc_emails, subject, body_text, attachment_file_path,
                               bucket_name, object_key, attachment_flag)

    # Assert
    assert response == "Credentials not available"

@patch('email_automation.download_file_from_s3')
@patch('builtins.open', new_callable=MagicMock)
@patch('boto3.client')
def test_send_email_with_attachment(mock_boto3_client, mock_open, mock_download):
    # Arrange
    sender_email = "test_sender@test.com"
    recipient_emails = ["test_recipient@test.com"]
    cc_emails = ["test_cc@test.com"]
    subject = "Test Subject"
    body_text = "Test Body Text"
    attachment_file_path = "/tmp/test_file"
    bucket_name = "test_bucket"
    object_key = "wells/input/HHNMADDR_20240110110010.txt"
    attachment_flag = True
    mock_open.return_value.__enter__.return_value.read.return_value = b"file content"

    # Act
    send_email_with_attachment(sender_email, recipient_emails, cc_emails, subject, body_text, attachment_file_path,
                               bucket_name, object_key, attachment_flag)

    # Assert
    mock_download.assert_called_once_with(bucket_name, object_key, attachment_file_path)
    mock_open.assert_called_once_with(attachment_file_path, 'rb')

@patch('email_automation.s3_client')
def test_read_file_to_json(mock_s3_client, context):  # Add the context fixture as an argument to your test
    # Define the bucket name and file key
    bucket_name = 'test_bucket'
    file_key = 'test_file_key'

    # Define the expected JSON content
    expected_json_content = {"key": "value"}
    # Convert the content to JSON and encode it to bytes
    content_bytes = json.dumps(expected_json_content).encode('utf-8')
    # Create a BytesIO object with the content
    streaming_body = io.BytesIO(content_bytes)

    # Define the response from the list_objects_v2 method
    response = {
        'Body': streaming_body
    }

    mock_s3_client.get_object.return_value = response
    # Call the read_file_to_json method with the context
    result = read_file_to_json(bucket_name, file_key, context)

    # Check the result
    assert result == expected_json_content    # Check if the get_object method was called with the correct arguments
    mock_s3_client.get_object.assert_called_once_with(Bucket=bucket_name, Key=file_key)

    #test negative scenario
    mock_s3_client.get_object.return_value = ""
    # Call the send_email_with_attachment function and check if it raises a NoCredentialsError
    with pytest.raises(Exception):
        result = read_file_to_json(bucket_name, file_key, context)

@patch('email_automation.format_email_body')
@patch('email_automation.fetch_matching_keys_in_folder')
@patch('email_automation.send_email_with_attachment')
def test_lambda_handler_valueerror(mock_send_email_with_attachment, mock_fetch_matching_keys_in_folder, mock_format_email_body, context):
    event = {
        'sender_email': 'test_sender_email',
        'attachment_file_path': 'test_attachment_file_path',
        'recipient_emails': ['test_recipient_email'],
        'cc_emails': ['test_cc_email'],
        'subject': 'test_subject',
        'body': 'test_body',
        'bucket_name': 'test_bucket_name',
        'object_key': 'test_object_key',
        'pattern': 'test_pattern',
        'attachment_flag': False,
        'email_input_file': 'test_email_input_file',
        'email_input_format': 'json',
        'email_output_delimiter': ','
    }
    with pytest.raises(ValueError):
        result = lambda_handler(event, context)

@patch('email_automation.format_email_body')
@patch('email_automation.fetch_matching_keys_in_folder')
@patch('email_automation.send_email_with_attachment')
def test_lambda_handler_withoutAttachment(mock_send_email_with_attachment, mock_fetch_matching_keys_in_folder, mock_format_email_body, context):
    # Define the mock event and context
    event = {
        'sender_email': 'test_sender_email',
        'attachment_file_path': 'test_attachment_file_path',
        'recipient_emails': ['test_recipient_email'],
        'cc_emails': ['test_cc_email'],
        'subject': 'test_subject',
        'body': 'test_body',
        'bucket_name': 'test_bucket_name',
        'object_key': 'test_object_key',
        'pattern': 'test_pattern',
        'attachment_flag': False,
        'email_input_file': 'test_email_input_file',
        'email_input_format': 'json',
        'email_output_format': 'columnar',
        'email_output_delimiter': ','
    }

    # Define the return values of the mocked functions
    mock_format_email_body.return_value = 'test_body'
    mock_fetch_matching_keys_in_folder.return_value = 'test_object_key'
    mock_send_email_with_attachment.return_value = 'Email sent!'

    # Call the lambda_handler function
    result = lambda_handler(event, context)

    # Check the result
    assert result == {
        'statusCode': 200,
        'body': 'Email sent!'
    }


@patch('email_automation.format_email_body')
@patch('email_automation.fetch_matching_keys_in_folder')
@patch('email_automation.send_email_with_attachment')
def test_lambda_handler_withAttachment(mock_send_email_with_attachment, mock_fetch_matching_keys_in_folder, mock_format_email_body, context):
    # Define the mock event and context
    event = {
        'sender_email': 'test_sender_email',
        'attachment_file_path': 'test_attachment_file_path',
        'recipient_emails': ['test_recipient_email'],
        'cc_emails': ['test_cc_email'],
        'subject': 'test_subject',
        'body': 'test_body',
        'bucket_name': 'test_bucket_name',
        'object_key': 'test_object_key',
        'pattern': 'test_pattern',
        'attachment_flag': True,
        'email_input_file': 'test_email_input_file',
        'email_input_format': 'json',
        'email_output_format': 'columnar',
        'email_output_delimiter': ','
    }

    # Define the return values of the mocked functions
    mock_format_email_body.return_value = 'test_body'
    mock_fetch_matching_keys_in_folder.return_value = 'test_object_key'
    mock_send_email_with_attachment.return_value = 'Email sent!'

    # Call the lambda_handler function
    result = lambda_handler(event, context)

    # Check the result
    assert result == {
        'statusCode': 200,
        'body': 'Email sent!'
    }