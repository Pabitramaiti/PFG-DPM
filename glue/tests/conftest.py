import sys
import pytest
from unittest.mock import patch, Mock, MagicMock
sys.modules['splunk'] = Mock()
sys.modules['sfs'] = Mock()
import moto
import boto3

@pytest.fixture
def mock_s3_client():
    """ Mock S3client using moto."""
    with moto.moto_s3():
        yield boto3.client('s3')
        
@pytest.fixture
def sfs_mock():
    # Mock the S3FileSystem class
    with patch('s3fs.S3FileSystem') as mock_s3fs:
        # Mock the open method of S3FileSystem to return a MagicMock
        mock_s3fs.return_value.open = MagicMock()
        yield mock_s3fs
        
# Mocking boto3 client
@pytest.fixture
def boto3_client_mock():
    with patch('boto3.client') as mock_boto3_client:
        yield mock_boto3_client
        
@pytest.fixture
def mock_splunk(mocker):
    """ Mock the splunk module and its log_message function. """
    mock_splunk= mocker.patch("splunk.log_message")
    return mock_splunk
        
# Mocking awsglue module and its compnents
@pytest.fixture
def awsglue_mock():
    mock_glue = Mock()
    mock_utils = Mock(getResolvedOptions=Mock())
    
    # Mocking other potential submodules and functions
    with patch.dict('sys.modules', {
        'awsglue': mock_glue,
        'awsglue.utils': mock_utils,
        'awsglue.dynamicframe': Mock(),
        'awsglue.context': Mock(),
        'awsglue.transforms': Mock(),
    }):
        yield {
            'glue': mock_glue,
            'utils': mock_utils,
        }
        
# Mocking the input arguments
@pytest.fixture
def sys_argv_mock():
    sys.argv = [
        'delimited_to_json.py',
        '--s3_bucket_input', 'input-bucket',
        '--s3_input_file', 'input-file',
        '--s3_bucket_output', 'output-bucket',
        '--s3_output_file', 'output-file',
        '--delimiter', ',',
        '--config_str', '{"header_identifiers": ["HEADER"], "trailer_identifiers": ["TRAILER"], "unique_key": "id"}'
    ]
    yield
    
@pytest.fixture
def mock_kitree():
    """Mock the KITree class."""
    with patch('delimited_to_json.KITree') as mock_kitree:
        yield mock_kitree

@pytest.fixture
def mock_DataStorage():
    """Mock the DataStorage class. """
    mock_DataStorage = Mock()
    return mock_DataStorage

@pytest.fixture
def mock_extract_duplicate_values():
    with patch('delimited_to_json.extract_duplicate_values') as mock_extract_duplicates:
        yield mock_extract_duplicates
        
@pytest.fixture
def mock_create_bucket(mock_s3_resource):
    mock_bucket = mock_s3_resource() .Bucke
    mock_bucket.create = MagicMock()
    yield mock_bucket
    
@pytest.fixture
def mock_s3fs_open():
    with patch('s3fs.S3FileSystem.open') as mock_open:
        yield mock_open
        
@pytest.fixture
def mock_s3_resource():
    with patch('boto3.resource') as mock_s3:
        yield mock_s3
        
@pytest.fixture
def mock_logging():
    with patch('delimited_to_json.splunk.log_message') as mock_log_message, \
         patch('delimited_to_json.get_run_id') as mock_get_run_id:
             yield mock_log_message, mock_get_run_id
             
@pytest.fixture
def mock_write_output_file():
    """Mock write_output_file function."""
    with patch('delimited_to_json.write_output_file') as mock_write_output_file:
        yield mock_write_output_file
        
@pytest.fixture
def mock_get_configs():
    """Mock get_configs function."""
    with patch('delimited_to_json.get_configs') as mock_get_configs:
        mock_get_configs.return_value = {
            's3_bucket_input': 'input-bucket',
            's3_input_file': 'input-file',
            's3_bucket_output': 'output-bucket',
            's3_output_file': 'output-file',
            'delimiter': ',',
            'config_str': '{"header_identifiers": ["HEADER"], "trailer_identifiers": ["TRAILER"], "unique_key": "id"}'
        }
        yield mock_get_configs
                     
@pytest.fixture
def mock_get_json():
    with patch('delimited_to_json.get_json') as mock:
        yield mock
        
@pytest.fixture
def mock_process_file():
    with patch('delimited_to_json.get_json') as mock:
        yield mock