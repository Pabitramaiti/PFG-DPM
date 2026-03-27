import os
import sys
import pytest
from unittest.mock import patch, MagicMock, call

import importlib

# This will patch boto3.client for all tests in this session
@pytest.fixture(scope="session", autouse=True)
def patch_boto3_client():
    with patch("boto3.client", return_value=MagicMock()):
        yield

@pytest.fixture(autouse=True)
def setup_env(monkeypatch):
    monkeypatch.setenv("maxRetries", "2")
    monkeypatch.setenv("maxFailuresPerBatch", "2")
    monkeypatch.setenv("sleepTimeout", "1")
    monkeypatch.setenv("region", "us-east-1")
    monkeypatch.setenv("ssmdbKey", "ssmkey")
    monkeypatch.setenv("dbName", "db")
    monkeypatch.setenv("schema", "s")
    monkeypatch.setenv("transactionId", "txid")
    monkeypatch.setenv("outputDir", "dir")
    monkeypatch.setenv("outputBucketName", "bucket")
    yield

@pytest.fixture(autouse=True)
def reset_sys_argv():
    sys.argv = [sys.argv[0]]
    yield

@pytest.fixture
def module():
    # Import fresh to ensure sys.argv/envvars take effect
    import copy_files
    importlib.reload(copy_files)
    return copy_files

@patch("copy_files.DataPostgres")
@patch("copy_files.log_message")
@patch("copy_files.processOrders")
def test_main_success(mock_process, mock_log_message, mock_dp, module):
    # Mock DataPostgres().getDocuments to return groupable objects with .orderid/.documentid/.s3location
    doc = MagicMock()
    doc.orderid = "order1"
    doc.documentid = "doc1"
    doc.s3location = "bucket/object"
    mock_dp.return_value.getDocuments.return_value = [doc]
    mock_process.side_effect = [[], []]  # No failed orderIds after 1st call
    module.main()
    assert mock_process.called
    assert not mock_log_message.call_args_list[-1][0][0] == "failed" or \
           "ValueError: Failed to process documents" not in \
           str(mock_log_message.call_args_list[-1][0][1])

@patch("copy_files.DataPostgres")
@patch("copy_files.log_message")
@patch("copy_files.processOrders")
def test_main_failure(mock_process, mock_log_message, mock_dp, module):
    doc = MagicMock()
    doc.orderid = "order1"
    doc.documentid = "doc1"
    doc.s3location = "bucket/object"
    mock_dp.return_value.getDocuments.return_value = [doc]
    mock_process.side_effect = [["order1"], ["order1"]]
    with pytest.raises(ValueError, match=r"Failed to process documents for order IDs: \['order1'\] after 2 retries"):
        module.main()
    assert any(
        "Failed to process documents" in str(c[0][1])
        for c in mock_log_message.call_args_list
    )

@patch("copy_files.copyToDestination")
@patch("copy_files.log_message")
@patch("copy_files.s3")
def test_customCopyLogic_pdf_exists(mock_s3, mock_log_message, mock_copy, module):
    # path ending .pdf, should not call s3.get_object
    module.customCopyLogic("bucket/object.pdf", "destpath")
    assert not mock_s3.get_object.called
    assert mock_copy.called

@patch("copy_files.copyToDestination")
@patch("copy_files.log_message")
@patch("copy_files.s3")
def test_customCopyLogic_pdf_not_exists_success(mock_s3, mock_log_message, mock_copy, module):
    # Path not ending .pdf, s3.get_object doesn't raise
    mock_s3.get_object.return_value = True
    module.customCopyLogic("bucket/object", "destpath")
    assert mock_s3.get_object.called
    assert mock_copy.called

@patch("copy_files.copyToDestination")
@patch("copy_files.log_message")
@patch("copy_files.s3")
def test_customCopyLogic_pdf_not_exists_fail(mock_s3, mock_log_message, mock_copy, module):
    # Path not ending .pdf, s3.get_object raises, should log info and use original path
    mock_s3.get_object.side_effect = Exception("missing")
    module.customCopyLogic("bucket/object", "destpath")
    assert mock_s3.get_object.called
    assert mock_copy.called
    assert mock_log_message.called
    assert mock_log_message.call_args_list[0][0][0] == "info"

@patch("copy_files.copyToDestination")
@patch("copy_files.log_message")
def test_processOrders_handles_failure_and_raises(mock_log_message, mock_copy, module):
    # Grouped documents with a doc list that triggers an exception in copy
    doc = MagicMock()
    doc.orderid = "o1"
    doc.documentid = "d1"
    doc.s3location = "bucket/object"
    grouped = {"o1": [doc]}
    # monkeypatch outputDir env to ensure .pdf path
    os.environ["outputDir"] = "odir"
    os.environ["outputBucketName"] = "obus"
    # Custom copy logic will always raise
    with patch.object(module, "customCopyLogic", side_effect=Exception("f")):
        with pytest.raises(ValueError, match="Number of failed documents exceeded"):
            module.processOrders(["o1"], grouped, 0)
    assert mock_log_message.called

@patch("copy_files.copyToDestination")
@patch("copy_files.log_message")
def test_processOrders_success_under_failure_limit(mock_log_message, mock_copy, module):
    # Will not raise; customCopyLogic passes
    doc = MagicMock()
    doc.orderid = "o1"
    doc.documentid = "d1"
    doc.s3location = "bucket/object"
    grouped = {"o1": [doc]}
    os.environ["outputDir"] = "odir"
    os.environ["outputBucketName"] = "obus"
    with patch.object(module, "customCopyLogic") as mockcc:
        out = module.processOrders(["o1"], grouped, 5)
    # No ValueError, empty returned (everything succeeded)
    assert out == []
    assert mockcc.called

def test_set_job_params_as_env_vars(monkeypatch):
    import copy_files
    monkeypatch.setenv("NOTCHANGED", "v1")
    sys.argv = ["prog", "--foo", "bar", "--baz", "quux"]
    copy_files.set_job_params_as_env_vars()
    assert os.environ["foo"] == "bar"
    assert os.environ["baz"] == "quux"

