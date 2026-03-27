import pytest 
import json
import sys
import boto3
from moto import mock_aws
from unittest.mock import patch, MagicMock, mock_open, call, Mock
from botocore.exceptions import ClientError
from delimited_to_json import get_json, get_run_id, get_configs, parse_config, write_output_file, is_header, is_trailer, extract_duplicate_values, process_file, main

# @patch('delimited_to_json.get_run_id', return_value='mocked_run_id')
# @patch('delimited_to_json.splunk.log_message')	
# @patch('delimited_to_json.boto3.client')
# @patch('delimited_to_json.sfs.open')
# @patch('delimited_to_json.DataStorage')
# @patch('delimited_to_json.extract_duplicate_values')	
# @patch('delimited_to_json.get_configs')	
# @patch('delimited_to_json.write_output_file')
# @patch('delimited_to_json.parse_configs')
# @patch('delimited_to_json.process_file', return_value={'data': 'some_data'})	
# # def test_main(
# # 	mock_process_file, mock_parse_config, mock_write_output_file, mock_get_configs, mock_extract_duplicate_values, mock_DataStorage, mock_kitree, mock_s3fs_open, mock_boto3_client, mock_log_message, mock_logging
# # ):
# # 	main()

# Positive scenario: Mocked Boto3 client returns account ID
def test_get_run_id_positive_account_id_found(boto3_client_mock):
	mock_sts = boto3_client_mock.return_value
	mock_sts.get_caller_identity.return_value = {'Account': '556144470667'}

	run_id = get_run_id()

	assert run_id == 'arn:dpm:glue:delimited-to-json-conversion:556144470667'

# Scenario: Mocked Boto3 client raises KeyError for account ID
def test_get_run_id_key_error_handling(boto3_client_mock):
	mock_sts = boto3_client_mock.return_value
	mock_sts.get_caller_identity.side_effect = KeyError

	# Function should handle KeyError gracefully
	with pytest.raises(KeyError):
		run_id = get_run_id()


# Scenario: Mocked Boto3 client raises ClientError
def test_get_run_id_client_error_handling(boto3_client_mock):
	mock_sts = boto3_client_mock.return_value
	mock_sts.get_caller_identity.side_effect = ClientError({'Error': {}}, 'operation')

	# Function should handle ClientError gracefully
	with pytest.raises(ClientError):
		run_id = get_run_id()

# Scenario: Mocked Boto3 client returns empty account ID
def test_get_run_id_empty_account_id(boto3_client_mock):
	mock_sts = boto3_client_mock.return_value
	mock_sts.get_caller_identity.return_value = {'Account': ''}

	run_id = get_run_id()

	assert run_id == 'arn:dpm:glue:delimited-to-json-conversion:' # Handle empty account ID

# Mocking Boto3 client and testing parameters passed
def test_get_run_id_boto3_client_called_with_correct_parms(boto3_client_mock):
	get_run_id()

	boto3_client_mock.assert_called_once_with('sts') # Assert that boto3.client was called with 'sts'

def test_get_configs_verify_all_args(sys_argv_mock):
	# Call the function under test
	args = get_configs()

	# Verify that all required arguments are present in the returned dictionary
	assert 's3_bucket_input' in args
	assert 's3_input_file' in args 
	assert 's3_bucket_output' in args
	assert 's3_output_file' in args
	assert 'delimiter' in args
	assert 'config_str' in args

def test_get_configs_with_all_values(awsglue_mock):
	# Mock the return value of getResolvedOptions
	awsglue_mock['utils'].getResolvedOptions.return_value = {
		's3_bucket_input': 'input-bucket',
		's3_input_file': 'input-file',
		's3_bucket_output': 'output-bucket',
		's3_output_file': 'output-file',
		'delimiter': ',',
		'config_str': '("header_identifiers": ["HEADER"], "trailer_identifiers": ["TRAILER"], "unique_key": "id"}'

	}

	# Call the function under test
	args = get_configs()

	# Verify the return value matches the expected dictionary
	expected_args= {'s3_bucket_input': 'input-bucket', 's3_input_file': 'input-file', 's3_bucket_output': 'output-bucket', 's3_output_file': 'output-file', 'delimiter': ',', 'config_str': '{"header_identifiers": ["HEADER"], "trailer_identifiers": ["TRAILER"], "unique_key": "id"}'}


	# Compare the actual and expected dictionaries
	filetred_args = {key: args[key] for key in expected_args.keys()}

	assert filetred_args == expected_args, f' Expected{expected_args}, actual {filetred_args}'

def test_get_configs_invalid_argument_key():
	sys.argv = [
		'script_name.py',
		'--s3_bucket_input','input-bucket',
		'--s3_input_file', 'input-file',
		'--s3_bucket_output','output-bucket',
		'--s3_output_file', 'output-file',
		'--eelimiter', ',', 
		'--config_str', '("header_identifiers": ["HEADER"], "trailer_identifiers": ["TRAILER"], "unique_key": "id"}'
	]

	with pytest.raises(ValueError):
		args = get_configs()

def test_get_configs_missing_argument_key():
	sys.argv = [
		'script_name.py',
		'--s3_bucket_input','input-bucket',
		'--s3_input_file', 'input-file',
		'--s3_bucket_output','output-bucket',
		'--s3_output_file', 'output-file',
		'--config_str', '("header_identifiers": ["HEADER"], "trailer_identifiers": ["TRAILER"], "unique_key": "id"}'
	]

	with pytest.raises(ValueError):
		args = get_configs()

def test_parse_config_success(mock_splunk):
	"""
	Test parse_config with a valid configuration.
	"""
	config = {
		"header_identifiers": ["header1"],
		"trailer_identifiers": ["trailer1"],
		"unique_key": "unique_value",
		"duplicate_removal": {
			"duplicate_values_input_file": "dup_values.txt",
			"column_in_source_file": "source_col",
			"column_in_duplicates_file": "dup_col",
			"removal_condition": "non_first",
		},
	}

	actual = parse_config(config)
	assert actual == None
	assert config["headers"] == ["header1"]
	assert config["layout_header_identifier"] == "header1"
	assert config["trailers"] == ["trailer1"]
	assert config["unique_key"] == "unique_value"
	assert config["duplicate_values_input_file"] == "dup_values.txt"
	assert config["column_in_duplicates_file"] == "dup_col"
	assert config["column_in_source_file"] == "source_col"
	assert config["removal_condition"] == "non_first"

def test_parse_config_missing_headers(mock_splunk):
	"""
	Test parse_config with a missing header_identifiers.
	"""
	config = {
		"trailer_identifiers": ["trailer1"],
		"unique_key": "unique_value",
	}

	with pytest.raises(ValueError, match="There are no header identifiers specified in the config file"):
		parse_config(config)
		# mock_splunk.assert_any_call({'Status': 'failed', 'Message': 'There are no header identifiers specified in the config file'}, "arn:dpm:glue:delimited-to-json-conversion:2545743144112" )

def test_parse_config_missing_trailers(mock_splunk):
	"""
	Test parse_config with a missing trailer_identifiers.
	"""
	config = {
		"header_identifiers": ["header1"],
		"unique_key": "unique_value",
	}

	with pytest.raises(ValueError, match="There are no trailers specified in the config file"):
		parse_config(config)
	# mock_splunk.assert_any_call({'Status': 'failed', 'Message': 'There are no trailers specified in the config file'},"arn:dpm:glue:delimited-to-json-conversion:556144470667")
	
def test_parse_config_missing_unique_key(mock_splunk):
	"""
	Test parse_config with a missing unique_key.
	"""
	config = {
		"header_identifiers": ["header1"],
		"trailer_identifiers": ["trailer1"],
	}

	with pytest.raises(ValueError, match="unique_key is not specified in the config file"):
		parse_config(config)
	# mock_splunk.assert_any_call({'Status': 'failed', 'Message': 'unique_key is not specified in the config file'},"arn:dpm:glue:delimited-to-json-conversion:556144470667")

def test_parse_config_invalid_removal_condition(mock_splunk):
	"""
	Test parse_config with an invalid removal_condition.
	"""
	config = {
		"header_identifiers": ["header1"],
		"trailer_identifiers": ["trailer1"],
		"unique_key": "unique_value",
		"duplicate_removal": {
			"duplicate_values_input_file": "dup_values.txt",
			"column_in_source_file": "source_col",
			"column_in_duplicates_file": "dup_col",
			"removal_condition": "invalid_condition",
		},
	}

	with pytest.raises(ValueError, match="value of removal_condition configured under duplicate removal is invalid"):
		parse_config(config)
	# mock_splunk.assert_any_call({'Status': 'failed', 'Message': 'Value of removal condition is invalid'},"arn:dpm:glue:delimited-to-json-conversion:556144470667")

def test_parse_config_missing_duplicate_attributes(mock_splunk):
	"""
	Test parse_config with missing attributes for duplicate removal.
	"""
	config = {
		"header_identifiers": ["header1"],
		"trailer_identifiers": ["trailer1"],
		"unique_key": "unique_value",
		"duplicate_removal": {
			"duplicate_values_input_file": "dup_values.txt",
			"column_in_source_file": "source_col",
			"column_in_duplicates_file": None,
			"removal_condition": "non_first",
		},
	}

	with pytest.raises(ValueError, match="Attributes required for removing duplicates are missing in the config file"):
		parse_config(config)
	# mock_splunk.assert_any_call({'Status': 'failed', 'Message': 'Attributes required for removing duplicates are missing in the config file'},"arn:dpm:glue:delimited-to-json-conversion:556144470667")

def test_write_output_file_access_denied(mock_splunk, sfs_mock):
	# Mock data for the test case
	converted_data_contents = {"key": "value"}
	output_bucket = "test-bucket"
	output_file = "test-file.json"

	# Set up the S3FileSystem mock to raise PermissionError when open is called 
	sfs_mock.return_value.open.side_effect = PermissionError("Access Denied")

	# Call the function under test
	with pytest.raises(PermissionError):
		write_output_file(converted_data_contents, output_bucket, output_file)

def test_write_output_file_negative(mock_splunk, sfs_mock):
	# Mock data for negative test case
	converted_data_contents = None
	output_bucket = "test-bucket"
	output_file = "test-file.json"

	# Call the fuction under test and expect a ValueError
	with pytest.raises(ValueError) as e:
		write_output_file(converted_data_contents, output_bucket, output_file)

	# Assertions
	expected_error_msg= "Write operation to S3 bucket test-bucket failed with error: Contents to output file test-file.json is empty."
 
	assert str(e.value) == expected_error_msg

def test_is_header():
	# Define test data
	headers = {"HEADER1", "HEADER2"}

	# Test case 1: Line starts with a configured header
	line1 = "HEADER1,Value1,Value2"
	assert is_header(line1, headers) is True

	# Test case 2: Line starts with another configured header
	line2 = "HEADER2,Value1,Value2"
	assert is_header(line2, headers) is True

	# # Test case 3: Line starts with part of a configured header
	line3 = "HEADER1_PART,Value1,Value2"
	assert is_header(line3, headers) is True

def test_is_header_invalid_values():
	headers = {"HEADER1", "HEADER2"}

	# Test case 4:Line does not start with any configured header
	line4 = "Some random line"
	assert is_header(line4, headers) is False

	# Test case 5: Empty line
	line5 = ""
	assert is_header(line5, headers) is False

	# Test case 6: No headers configured
	empty_headers = set()
	line6 = "Header1,Value1,Value2"
	assert is_header(line6, empty_headers) is False

def test_is_trailer_positive():
	# Define test data
	trailers = {"TRAILER1", "TRAILER2"}

	# Positive test case 1: Line starts with a configured trailer
	line1 = "TRAILER1,Value1,Value2"
	assert is_trailer(line1, trailers) is True

	# Positive test case 2:Line starts with another configured trailer 
	line2 = "TRAILER2,Value1,Value2"
	assert is_trailer(line2, trailers) is True

	# Positive test case 1: Line starts with part of a configured trailer
	line1 = "TRAILER1_PART,Value1,Value2"
	assert is_trailer(line1, trailers) is True

def test_is_trailer_invalid_values():
	# Define test data
	trailers = {"TRAILER1", "TRAILER2"}

	# Negative test case 2: Line does not start with any configured trailer
	line2 = "Some random line"
	assert is_trailer(line2, trailers) is False

	# Negative test case 3: Empty line
	line3 = ""
	assert is_trailer(line3, trailers) is False

	# Negative test case 4: No trailers configured
	empty_trailers = set()
	line4 = "TRAILER1,Value1,Value2"
	assert is_trailer(line4, empty_trailers) is False

@patch('s3fs.S3FileSystem.open')
@patch('boto3.resource')
@patch('delimited_to_json.extract_duplicate_values')
def test_process_files(mock_extract_duplicate_values, mock_boto3_resource, mock_s3fs_open):
	# Mocking extract_duplicate_values to return a set of duplicate values
	mock_extract_duplicate_values.return_value = ["Value1", "Value2", "Value1"]

	# Define the mock file content
	file_content = json.dumps({
		"HEADER1": "Value1\nValue2\nValue1",
		"DATA1": "Data1_Value1\nData1_Value2",
		"DATA2": "Data2_Value1\nData2_Value2",
		"TRAILER1": "Trailer1_Value"
		})

	# Mock S3FileSystem open method to return the mock file content 
	mock_file = MagicMock()
	mock_file.read.return_value = file_content
	mock_s3fs_open.return_value.__enter__.return_value = mock_file

	# Mock S3 resource
	mock_bucket = MagicMock()
	mock_boto3_resource.return_value.Bucket.return_value = mock_bucket
	mock_bucket.create_bucket.return_value = None

	input_bucket = "validbucketname123"
	input_file = "test-file.csv"
	delimiter = ","

	config = {
		"header_identifiers": ["HEADER1"],
		"trailer_identifiers": ["TRAILER1"],
		"layout_header_identifiers": "HEADER1",
		"unique_key": "unique_key",
		"duplicate_removal": {
			"removal_condition": "non_first",
			"column_in_source_file": "source_col",
			"column_in_duplicates_file": "dup_col",
			"duplicate_values_input_file": "dup_values.txt",
		}, 
		"removal_condition": "non_first",
		"column_in_source_file": "unique_key",
		"duplicate_values_input_file": "dup_values.txt",
		"column_in_duplicates_file": "header1"
	}


	# Call the function under test with mocked dependencies
	result = process_file(input_bucket, input_file, delimiter, config)
	assert isinstance(result, list)

@patch('builtins.print')
def test_process_files_invalid(mock_open, mock_extract_duplicate_values, mock_s3fs_open, mock_create_bucket):
	# Mocking extract_duplicate_values function to return a set of duplicates 
	mock_extract_duplicate_values.return_value = {'Value1'}

	# Mocking S3FileSystem open method to return a context manager 
	mock_open_context = MagicMock()
	mock_open.return_value.__enter__.return_value = mock_open_context

	# Mocking S3 resource creation and bucket creation
	mock_bucket = mock_create_bucket
	mock_bucket.name = "test-bucket"

	input_bucket = "validbucketname123" # Use a valid bucket name
	input_file = "test-file.csv"
	delimiter = ","

	# Configure the mock_open context manager to simulate an empty file 
	mock_open_context.read.return_value = "" # Simulating an empty file

	config = {
		"header_identifiers": ["header1"],
		"trailer_identifiers": ["trailer1"],
		"layout_header_identifiers": "header1",
		"unique_keys": "unique_key",
		"removal_condition": "non_first",
		"column_in_source_file": "unique_key",
		"duplicate_values_input_file": "dup_values.txt",
		"column_in_duplicates_file": "header1",
	}

	# Call the function under test with mocked dependencies
	with pytest.raises(ValueError) as e:
		process_file(input_bucket, input_file, delimiter, config)

	assert str(e.value)

@mock_aws
@patch('s3fs.S3FileSystem.open')
def test_process_file_iteration(mock_s3fs_open):
	# Mock S3 resource and create a test bucket
	s3 = boto3.resource("s3", region_name="us-east-1")
	input_bucket = "test5-bucket"
	s3.create_bucket(Bucket=input_bucket)

	#Create a mock file with valid JSON content
	file_content = json.dumps({
		"HEADER1": "Value1\nValue2\nValue1",
		"DATA1": "Data1_Value1\nData1_Value2",
		"DATA2": "Data2_Value1\nData2_Value2",
		"TRAILER1": "Trailer1_Value"
	})
	s3.Object(input_bucket, "dup_values.txt") .put(Body=file_content)

	# Mock the S3FileSystem open method to return the mock file content
	mock_open_context = MagicMock()
	mock_open_context.__enter__.return_value.read.return_value = file_content
	mock_s3fs_open.return_value = mock_open_context

	# Mock config data
	config = {
		"header_identifiers": ["HEADER1"],
		"trailer_identifiers": ["TRAILER1"],
		"layout_header_identifiers": "HEADER1",
		"unique_key": "unique_key",
		"duplicate_removal": {
			"removal_condition": "non_first",
			"column_in_source_file": "source_col",
			"column_in_duplicates_file": "dup_col",
			"duplicate_values_input_file": "dup_values.txt",
		}, 
		"removal_condition": "non_first",
		"column_in_source_file": "unique_key",
		"duplicate_values_input_file": "dup_values.txt",
		"column_in_duplicates_file": "header1"
	}
	# Call the function under test with mocked dependencies
	result = process_file(input_bucket, "dup_values.txt", ",", config)

	# Validate the result
	assert isinstance(result, list)
	assert len(result) == 0 # Adjust this based on expected behavior

@pytest.fixture
def mock_file(mocker):
	# Create a mock file object
	mock_file = mocker.mock_open()
	# Set up the content of the file
	mock_file.return_value.__iter__.return_value = iter([
		"HEADER1,Value1\nValue2\n",
		"DATA1,Data1_Value1,Data1_Value2\n",
		"TRAILER1,Trailer1_Value\n"
	])
	return mock_file


@patch('delimited_to_json.sfs.open')
@patch('delimited_to_json.is_header')
@patch('delimited_to_json.is_trailer')
@patch('delimited_to_json.splunk.log_message')
@patch('delimited_to_json.get_run_id')
def test_process_file_trailer_handling(mock_get_run_id, mock_log_message, mock_is_trailer, mock_is_header, mock_s3fs_open):
	# Mock get_run_id and set its return value
	mock_get_run_id.return_value = "mocked_run_id"

	# Mock the necessary dependencies and set their return values
	line1 = "TRAILER1,Value1,Value2\n"
	mock_s3fs_open.return_value.__enter__.return_value = [line1]
	mock_is_header.side_effect = [False] # Asserting line1 is not a header

	# Set up the trailers in the configuration
	trailers = {"TRAILER1", "TRAILER2"}
	config = {
		"trailer_identifiers": list(trailers),
		"header_identifiers": ["HEADER1"],
		"unique_key": "Value1"
	}

	# Set up the mock_is_trailer side effect based on the trailer set 
	def is_trailer_effect(line, trailers):
		return any(trailer in line for trailer in trailers)
	mock_is_trailer.side_effect = lambda line, trailers=trailers: is_trailer_effect(line, trailers)

	# Set up the necessary parameters
	input_bucket = "test_bucket"
	input_file = "test_file.csv"
	delimiter = ","

	# Call the function under test
	process_file(input_bucket, input_file, delimiter, config)

	mock_log_message.assert_called()
	assert mock_log_message.call_count == 5

@patch('delimited_to_json.extract_duplicate_values')
def test_process_file_duplicate_handling(mock_extract_duplicate_values, mock_DataStorage):
	# Mock the necessary dependencies and set their return values
	mock_extract_duplicate_values.returns_value = {"duplicate_value1", "duplicate_value2"}
	mock_storage_instance = MagicMock()
	mock_storage_instance.get_data.return_value = "Mocked data"

	with patch('delimited_to_json.DataStorage', return_value=mock_storage_instance):
		# Set up the necessary parameters
		input_bucket = "test-bucket"
		input_file = "test_file.csv"
		delimiter = ","
		config = {
			"header_identifiers": ["HEADER1"],
			"trailer_identifiers": ["TRAILER1"],
			"layout_header_identifiers": "HEADER1",
			"unique_key": "Value1",
			"duplicate_removal": {
				"removal_condition": "non_first",
				"column_in_source_file": "Value2",
				"column_in_duplicates_file": "dup_col",
				"duplicate_values_input_file": "dup_values.txt",
		}, 
		"removal_condition": "non_first",
		"column_in_source_file": "unique_key",
		"duplicate_values_input_file": "dup_values.txt",
		"column_in_duplicates_file": "header1"
	}

	with patch("s3fs.S3FileSystem.open", mock_open(read_data="")) as mock_file:
		# Call the function under test
		result = process_file(input_bucket, input_file, delimiter, config)

	# Assert that the expected duplicate values were extracted
	mock_extract_duplicate_values.assert_called_once_with(
		input_bucket, "dup_values.txt", "dup_col"
	)

@patch('s3fs.S3FileSystem.open')
def test_extract_duplicate_values_with_duplicates(mock_open):
	# Mock S3FileSystem open method to return the mock file content
	import json 
	mock_open.return_value.__enter__.return_value.read.return_value = json.dumps({
		"HEADER1": "Value1",
		"Value1": "Value1"
	})

	# Call the function under test
	result_duplicates = extract_duplicate_values("test-bucket", "test-file.csv", "Value1")

	# Define the expected result based on the mock file content
	expected_duplicates = ['Value1']

	# Assert that the function returns the expected result
	assert result_duplicates == expected_duplicates 

@patch('s3fs.S3FileSystem.open')
def test_extract_duplicate_values_with_non_duplicates(mock_open):
	# Mock S3FileSystem open method to return the mock file content
	import json 
	mock_open.return_value.__enter__.return_value.read.return_value = json.dumps ({
		"HEADER1": "Value1",
		"HEADER2": "Value1"
	})

	# Call the function under test
	result_duplicates = extract_duplicate_values("test-bucket", "test-file.csv", "Value1")

	# Define the expected result based on the mock file content
	expected_duplicates = None

	# Assert that the function returns the expected result
	assert result_duplicates == expected_duplicates 
 
@patch('delimited_to_json.sfs.open')
@patch('delimited_to_json.is_header')	
@patch('delimited_to_json.is_trailer')
@patch('delimited_to_json.splunk.log_message')	
@patch('delimited_to_json.get_run_id')
def test_process_file_data_processing(mock_get_run_id, mock_log_message, mock_is_trailer, mock_is_header, mock_s3fs_open):
	# Mock the necessary dependencies and set their return values
	mock_s3fs_open.return_value.__enter__.return_value=[
		"HEADER1,Value1,Value2,unique_key\n", # Include 'unique_key' in the headers
		"DATA1,Value3,Value4,Value1\n", # Add a value for 'unique_key' in the data
		"TRAILER1,Value5,Value6,Value1\n"
	]
	mock_is_header.side_effect=[True, False, False]
	mock_is_trailer.side_effect=[True, False, False]

	# Mock DataStorage class and its methods
	mock_storage_instance = MagicMock()
	mock_storage_instance.get_data.return_value= "Mocked data"

	with patch('delimited_to_json.DataStorage', return_value=mock_storage_instance):
		# Set up the necessary parameters
		input_bucket = "test_bucket"
		input_file = "test_file.csv"
		delimiter = ","
		config = {
			"header_identifiers": ["HEADER1"],
			"trailer_identifiers": ["TRAILER1"],
			"layout_header_identifier": "HEADER1",
			"unique_key": "Value1",
			"removal_condition": "non_first",
			"column_in_source_file": "unique_key",
			"duplicate_values_input_file": "dup_values.txt",
			"column_in_duplicates_file": "header1"
		}

		process_file(input_bucket, input_file, delimiter, config)

		# Assert that the necessary mock functions were called with the expected arguments
		mock_storage_instance.add_record.assert_called()
		assert mock_storage_instance.add_record.call_count == 2

@patch('delimited_to_json.sfs.open')	
@patch('delimited_to_json.is_header')
@patch('delimited_to_json.is_trailer')
@patch('delimited_to_json.splunk.log_message')
@patch('delimited_to_json.get_run_id')
def test_process_unique_key_validation(mock_get_run_id, mock_log_message, mock_is_trailer, mock_is_header, mock_s3fs_open):
	# Mock the necessary dependencies and set their return values
	mock_s3fs_open.return_value.__enter__.return_value = [
		"HEADER1,Value1,Value2\n",
		"DATA1,Value3,Value4\n",
		"TRAILER1,Value5,Value6\n"
	]
	mock_is_header.side_effect = [True, False, False]
	mock_is_trailer.side_effect = [False,False, True]

	# Mock DataStorage class and its methods
	mock_storage_instance = MagicMock()
	mock_storage_instance.get_data.return_value = "Mocked data"

	with patch('delimited_to_json.DataStorage', return_value=mock_storage_instance):
		# Set up the necessary parameters
		input_bucket = "test-bucket"
		input_file = "test_file.csv"
		delimiter = ","
		config = {
			"header_identifiers": ["HEADER1"],
			"trailer_identifiers": ["TRAILER1"],
			"layout_header_identifier": "HEADER1",
			"unique_key": "Value6",
			"removal_condition": "non_first",
			"column_in_source_file": "unique_key",
			"column_in_values_input_file": "dup_values.txt",
			"column_in_duplicates_file": "header1"
		}
  
		with pytest.raises(KeyError):
			process_file(input_bucket, input_file, delimiter, config)


def test_get_json_success(mock_logging):
	mock_log_message, mock_get_run_id = mock_logging

	# Mock the input JSON string and input file name 
	json_str = '{"file_type1": {"key1": "Value1"}, "file_type2": {"key2": "value2"}}'
	input_file_name = "file_type1_config.json"

	# Call the function with mocked arguments
	result = get_json(json_str, input_file_name)

	expected_result = {"key1": "value1"}
	assert result == None

	# Assert that log.message and get_run_id were not called 
	mock_log_message.assert_not_called()
	mock_get_run_id.assert_not_called()

def test_get_json_failure(mock_logging):
	mock_log_message, mock_get_run_id = mock_logging

	# Mock the input JSON string and input file name to simulate an error
	json_str = ''
	input_file_name = "invalid_file_name.json"

	# Call the function with mocked arguments, which should raise an exception
	with pytest.raises(Exception) as context:
		get_json(json_str, input_file_name)

	# Assert that the correct exception message was logged
	mock_log_message.assert_called_once_with({'Status': 'failed', 'json_str': '', 'Message': 'Conversion of configuration string into json failed : Expecting value: line 1 column 1 (char 0)'} , mock_get_run_id())

@patch('delimited_to_json.get_run_id')
def test_main_success(mock_get_run_id, sys_argv_mock, mock_splunk, boto3_client_mock, mock_s3fs_open, mock_kitree, mock_DataStorage, mock_extract_duplicate_values, mock_get_configs, mock_write_output_file):
	"""Test the main function for successful execution."""
	
	mock_get_run_id.return_value = "mocked_run_id"
	mock_Kitree_instance = mock_kitree.return_value
	mock_dataStorage_instance = mock_DataStorage.return_value
	mock_dataStorage_instance.get_data.return_value = {'data': 'some_data'}
 
 
	with patch('delimited_to_json.parse_config'), patch('delimited_to_json.process_file', return_value={'data': 'some_data'}): 
		main() 
	mock_splunk.assert_any_call({'Status': 'success', 'InputFileName': 'input-file', 'Message': 'Converted JSON file write operation to S3 bucket is successful.'},'mocked_run_id')
 
	mock_write_output_file.assert_called_once_with({'data': 'some_data'}, 'output-bucket', 'output-file')

@patch('delimited_to_json.get_run_id')
def test_main_missing_attributes(mock_get_run_id, sys_argv_mock, mock_splunk, boto3_client_mock, mock_s3fs_open, mock_kitree, mock_DataStorage, mock_extract_duplicate_values, mock_get_configs, mock_write_output_file):
    """Test the main function for handling missing attributes error."""

    # Set one of the required attributes as missing (e.g., input_bucket)
    mock_get_configs.return_value = {
        's3_bucket_input': '', # Missing input_bucket 
		's3_input_file': 'input-file',
		's3_bucket_output': 'output-bucket',
		's3_output_file': 'output-file',
		'delimiter': ',',
		'config_str': '{"header_identifiers": ["HEADER"], "trailer_identifiers": ["TRAILER"], "unique_key": "id"}'
	}
    with patch('sys.exit') as mock_sys_exit, patch('delimited_to_json.parse_config'), patch('delimited_to_json.process_file', return_value={'data': 'some_data'}) :
        with pytest.raises(Exception) as e:
            main()
            mock_splunk.assert_any_call({'Status': 'failed', 'Message': f'Failed to retrieve input parameters from step function due to {str(e)}'}, "mocked_run_id")


@patch('delimited_to_json.get_run_id', return_value='mocked_run_id')
def test_main_value_error_handling(mock_get_run_id, sys_argv_mock, mock_splunk, boto3_client_mock, mock_s3fs_open, mock_kitree, mock_DataStorage, mock_extract_duplicate_values, mock_get_configs, mock_write_output_file):
    """Test the main function for handling ValueError."""
    # Set up mock configurations
    mock_get_configs.return_value = {
        's3_bucket_input': 'input-bucket', 
		's3_input_file': 'input-file',
		's3_bucket_output': 'output-bucket',
		's3_output_file': 'output-file',
		'delimiter': ',',
		'config_str': '{"header_identifiers": ["HEADER"], "trailer_identifiers": ["TRAILER"], "unique_key": "id"}'
	}
            
    # Simulate ValueError by setting an invalid config_str
    mock_get_json= MagicMock(side_effect=ValueError("Invalid JSON"))
    
    with patch('delimited_to_json.get_json', mock_get_json):
        with pytest.raises(Exception) as e:
            main()
            
    # Assert 
    assert str(e.value) == "Invalid JSON" # Check if the correct ValueError message is raised
    mock_splunk.assert_any_call({'Status': 'failed', 'InputFileName': 'input-file', 'Message': 'Invalid JSON'}, "mocked_run_id")
    
     
@patch('delimited_to_json.get_run_id', return_value='mocked_run_id')
def test_main_file_not_found_error_handling(mock_get_run_id, sys_argv_mock, mock_splunk, boto3_client_mock, mock_s3fs_open, mock_kitree, mock_DataStorage, mock_extract_duplicate_values, mock_get_configs, mock_write_output_file):
    """Test the main function for handling FileNotFoundError."""
    
    # Set up mock configurations
    mock_get_configs.return_value = {
        's3_bucket_input': 'input-bucket', 
		's3_input_file': 'non-existent-file', # Simulating a non-existent
		's3_bucket_output': 'output-bucket',
		's3_output_file': 'output-file',
		'delimiter': ',',
		'config_str': '{"header_identifiers": ["HEADER"], "trailer_identifiers": ["TRAILER"], "unique_key": "id"}'
	}
    
    # Simulate FileNotFoundError during file processing
    mock_process_file = patch('delimited_to_json.process_file', side_effect=FileNotFoundError("File not found error"))
    
    with mock_process_file, pytest.raises(Exception) as e:
        main()

    # Assert 
    assert str(e.value) == "File open from S3 bucket failed with error: Input file non-existent-file in S3 bucket input-bucket is not found."
    mock_splunk.assert_any_call({'Status': 'failed', 'InputFileName': 'non-existent-file', 'Message': 'File open from S3 bucket failed with error: Input file non-existent-file in S3 bucket input-bucket is not found.'}, "mocked_run_id")
    
@patch('delimited_to_json.get_run_id', return_value='mocked_run_id')
def test_main_file_os_error_handling(mock_get_run_id, sys_argv_mock, mock_splunk, boto3_client_mock, mock_s3fs_open, mock_kitree, mock_DataStorage, mock_extract_duplicate_values, mock_get_configs, mock_write_output_file):
    """Test the main function for handling OSError."""
    
    # Set up mock configurations
    mock_get_configs.return_value = {
        's3_bucket_input': 'input-bucket', 
		's3_input_file': 'input-file',
		's3_bucket_output': 'output-bucket',
		's3_output_file': 'output-file',
		'delimiter': ',',
		'config_str': '{"header_identifiers": ["HEADER"], "trailer_identifiers": ["TRAILER"], "unique_key": "id"}'
	}
    
    # Simulate OSError during file processing
    mock_process_file = patch('delimited_to_json.process_file', side_effect=OSError("File read error"))
     
    with mock_process_file, pytest.raises(Exception) as e:
        main()
            
    # Assert
    assert str(e.value) == "File open from S3 bucket failed with error: File read error"
    mock_splunk.assert_any_call({'Status': 'failed', 'InputFileName': 'input-file', 'Message': 'File open from S3 bucket failed with error: File read error'}, "mocked_run_id")

@patch('delimited_to_json.get_run_id', return_value='mocked_run_id')
@patch('delimited_to_json.splunk.log_message')	
@patch('delimited_to_json.get_configs', return_value= {
	's3_bucket_input': 'input-bucket',
	's3_input_file': 'input-file',
	's3_bucket_output': 'output-bucket',
	's3_output_file': 'output-file',
	'delimiter': ',',
	'config_str': 'config_str'
})

@patch('delimited_to_json.get_json')	
@patch('delimited_to_json.process_file')
@patch('delimited_to_json.write_output_file')
def test_file_not_found_error_during_writing(mock_write_output_file, mock_process_file, mock_get_json, mock_get_configs, mock_log_message, mock_get_run_id):
    mock_write_output_file.side_effect = FileNotFoundError("Output bucket not found")
    
    with pytest.raises(Exception):
        main() 
        
    mock_log_message.assert_any_call(
		{'Status': 'failed', 'InputFileName': 'input-file', 'Message': 'Write operation for the file output-file failed with S3 bucket output-bucket not found error.'} , 'mocked_run_id'
	)
        
@patch('delimited_to_json.get_run_id', return_value='mocked_run_id')
@patch('delimited_to_json.splunk.log_message')	
@patch('delimited_to_json.get_configs', return_value= {
	's3_bucket_input': 'input-bucket',
	's3_input_file': 'input-file',
	's3_bucket_output': 'output-bucket',
	's3_output_file': 'output-file',
	'delimiter': ',',
	'config_str': 'config_str'
})
   
@patch('delimited_to_json.get_json')	
@patch('delimited_to_json.process_file')
@patch('delimited_to_json.write_output_file')
def test_file_not_oserror_during_writing(mock_write_output_file, mock_process_file, mock_get_json, mock_get_configs, mock_log_message, mock_get_run_id):
    mock_write_output_file.side_effect = OSError("Disk quota exceeded")
    
    with pytest.raises(Exception):
        main() 
        
        mock_log_message.assert_any_call(
			{'Status': 'failed', 'InputFileName': 'input_file', 'Message': 'Write operation for the file output_file in S3 bucket output_bucket failed with error:}, Disk quota exceeded'}, 'mocked_run_id'
		)     
    

@patch('delimited_to_json.sfs.open')
@patch('delimited_to_json.is_header')	
@patch('delimited_to_json.is_trailer')
@patch('delimited_to_json.splunk.log_message')	
@patch('delimited_to_json.get_run_id')
@patch('delimited_to_json.extract_duplicate_values')
def test_process_file_data_processing(mock_extract_duplicate_values, mock_get_run_id, mock_log_message, mock_is_trailer, mock_is_header, mock_s3fs_open):
	mock_extract_duplicate_values.return_value= ["Value1", "Value2", "Value1"]
	# Mock the necessary dependencies and set their return values
	mock_s3fs_open.return_value.__enter__.return_value=[
		"HEADER1,Value1,Value2,unique_key\n", # Include 'unique_key' in the headers
		"DATA1,Value3,Value4,Value1\n", # Add a value for 'unique_key' in the data
		"TRAILER1,Value5,Value6,Value1\n"
	]
	mock_is_header.side_effect=[True, False, False]
	mock_is_trailer.side_effect=[True, False, False]

	# Mock DataStorage class and its methods
	mock_storage_instance = MagicMock()
	mock_storage_instance.get_data.return_value = "Mocked data"

	with patch('delimited_to_json.DataStorage', return_value=mock_storage_instance):
		# Set up the necessary parameters
		input_bucket = "test_bucket"
		input_file = "test_file.csv"
		delimiter = ","

		# Add the required keys and values to the config dictionary
		config = {
			"header_identifiers": ["HEADER1"],
			"trailer_identifiers": ["TRAILER1"],
			"layout_header_identifier": "HEADER1",
			"unique_key": "Value1",
			"duplicate_removal": {
				"removal_condition": "non_first",
				"column_in_source_file": "unique_key",
				"column_in_duplicates_file": "header1",
				"duplicate_values_input_file": "dup_values.txt"
			}
		}
		column_value = "Value1"
		duplicates_encountered_mock = MagicMock()
		duplicates_encountered_mock.add = MagicMock()
		# process_file(input_bucket, input_file, delimiter, config)
		with patch ('delimited_to_json.set', return_value=duplicates_encountered_mock):
			# Call the function under test
			process_file(input_bucket, input_file, delimiter, config)

			# Assert that add was called once with the expected value
			duplicates_encountered_mock.add.assert_called_once_with(column_value)
	
		# Assert that the necessary mock functions were called with the expected arguments
		mock_storage_instance.add_record.assert_called()
		mock_storage_instance.add_record.assert_called_with({'HEADER1': ['TRAILER1'], 'Value1': ['Value5'], 'Value2': ['Value6'], 'unique_key': ['Value1']})
  
@patch('delimited_to_json.sfs.open')
@patch('delimited_to_json.is_header')	
@patch('delimited_to_json.is_trailer')
@patch('delimited_to_json.splunk.log_message')	
@patch('delimited_to_json.get_run_id')
@patch('delimited_to_json.extract_duplicate_values')	
def test_process_file_no_duplicates_encountered(mock_extract_duplicate_values, mock_get_run_id, mock_log_message, mock_is_trailer, mock_is_header, mock_s3fs_open):
	mock_extract_duplicate_values.return_value=["Value1", "Value2", "Value1"]
	# Mock the necessary dependencies and set their return values
	mock_s3fs_open.return_value.__enter__.return_value=[
		"HEADER1,Value1,Value2,unique_key\n", # Include 'unique_key' in the headers
		"DATA1,Value3,Value4,Value1\n", # Add a value for 'unique_key' in the data
		"TRAILER1,Value5,Value6,Value1\n"
	]
	mock_is_header.side_effect=[True, False, False]
	mock_is_trailer.side_effect=[True, False, False]

	# Mock DataStorage class and its methods
	mock_storage_instance = MagicMock()
	mock_storage_instance.get_data.return_value= "Mocked data"

	with patch('delimited_to_json.DataStorage', return_value=mock_storage_instance):
		# Set up the necessary parameters
		input_bucket = "test_bucket"
		input_file = "test_file.csv"
		delimiter = ","


		# Add the required keys and values to the config dictionary 
		config = {
			"header_identifiers": ["HEADER1"],
			"trailer_identifiers": ["TRAILER1"],
			"layout_header_identifier": "HEADER1",
			"unique_key": "Value1",
			"duplicate_removal": {
				"removal_condition": "non_first",
				"column_in_source_file": "unique_key",
				"column_in_duplicates_file": "header1",
				"duplicate_values_input_file": "dup_values.txt",
			}
		}
		column_value = "Value1"
		duplicates_encountered_mock = MagicMock()
		duplicates_encountered_mock.add = MagicMock()
		# process_file(input_bucket, input_file, delimiter, config)
		with patch('delimited_to_json.set', return_value=duplicates_encountered_mock):
			# Call the function under test
			process_file(input_bucket, input_file, delimiter, config)

			# Assert that add was called once with the expected value
			duplicates_encountered_mock.add.assert_called_once_with(column_value)

		# Assert that the necessary mock functions were called with the expected arguments
		mock_storage_instance.add_record.assert_called()
		mock_storage_instance.add_record.assert_called_with({'HEADER1': ['TRAILER1'], 'Value1': ['Value5'], 'Value2': ['Value6'], 'unique_key': ['Value1']})


@patch('delimited_to_json.sfs.open')
@patch('delimited_to_json.is_header')	
@patch('delimited_to_json.is_trailer')
@patch('delimited_to_json.splunk.log_message')	
@patch('delimited_to_json.get_run_id')
@patch('delimited_to_json.extract_duplicate_values')	
def test_process_file_duplicates_encountered(mock_extract_duplicate_values, mock_get_run_id, mock_log_message, mock_is_trailer, mock_is_header, mock_s3fs_open):
	mock_extract_duplicate_values.return_value=["Value1", "Value2", "Value1"]
	# Mock the necessary dependencies and set their return values
	mock_s3fs_open.return_value.__enter__.return_value=[
		"HEADER1,Value1,Value2,unique_key\n", # Include 'unique_key' in the headers
		"DATA1,Value3,Value4,Value1\n", # Add a value for 'unique_key' in the data
		"TRAILER1,Value5,Value6,Value1\n"
	]
	mock_is_header.side_effect=[True, False, False]
	mock_is_trailer.side_effect=[True, False, False]

	# Mock DataStorage class and its methods
	mock_storage_instance = MagicMock()
	mock_storage_instance.get_data.return_value= "Mocked data"

	# Set up the initial state of duplicates_encountered to certain column_value
	column_value= "Value1"
	duplicates_encountered={column_value}

	with patch('delimited_to_json.DataStorage', return_value=mock_storage_instance):
		with patch('delimited_to_json.set', return_value=duplicates_encountered):
			# Set up the necessary parameters
			input_bucket = "test_bucket"
			input_file = "test_file.csv"
			delimiter = ","

			# Add the required keys and values to the config dictionary 
			config = {
				"header_identifiers": ["HEADER1"],
				"trailer_identifiers": ["TRAILER1"],
				"layout_header_identifier": "HEADER1",
				"unique_key": "Value1",
				"duplicate_removal": {
					"removal_condition": "non_first",
					"column_in_source_file": "unique_key",
					"column_in_duplicates_file": "header1",
					"duplicate_values_input_file": "dup_values.txt",
				}
			}

			# Call the function under test
			process_file(input_bucket, input_file, delimiter, config)

			assert len(duplicates_encountered) == 1
   
@patch('delimited_to_json.get_run_id', return_value='mocked_run_id')
def test_main_value_error_empty(mock_get_run_id, sys_argv_mock, mock_splunk, boto3_client_mock, mock_s3fs_open, mock_kitree, mock_DataStorage, mock_extract_duplicate_values, mock_get_configs, mock_write_output_file):
    """Test the main function for handling ValueError."""
    # Set up mock configurations
    mock_get_configs.return_value = {
        's3_bucket_input': "", 
		's3_input_file': "",
		's3_bucket_output': "",
		's3_output_file': "",
		'delimiter': ',',
		'config_str': ""
	}
            
    with pytest.raises(Exception) as e:
        main()
            
    # Assert 
    assert str(e.value) == "input parameters retrieval failed: some or all of the attributes required for the module are missing" # Check if the correct ValueError message is raised
    mock_splunk.assert_any_call({'status': 'failed', 'message': 'failed to retrieve input parameters from step function due to some or all of the attributes required for the module are missing'}, "mocked_run_id")