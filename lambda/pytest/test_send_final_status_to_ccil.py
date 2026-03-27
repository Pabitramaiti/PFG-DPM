import os
import json
import pytest
import psycopg2
import requests
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

os.environ["DB_NAME"] = "testdb"
os.environ["KAFKA_API_URL"] = "http://kafka/api"

from send_final_status_to_ccil import (
    get_db_credentials, initialize_db_pool, get_db_connection, release_db_connection,
    get_client_orderid_from_transaction, get_processing_orders_bulk, send_to_kafka_api, get_kafka_message, lambda_handler
)

@pytest.fixture
def context():
    return MagicMock()

# --- DB Connection Utility Tests ---

@patch("send_final_status_to_ccil.db_pool")
def test_get_db_connection_returns_conn(mock_db_pool):
    conn = MagicMock()
    mock_db_pool.getconn.return_value = conn
    assert get_db_connection() is conn

@patch("send_final_status_to_ccil.db_pool")
def test_release_db_connection_calls_putconn(mock_db_pool):
    conn = MagicMock()
    release_db_connection(conn)
    mock_db_pool.putconn.assert_called_once_with(conn)

def test_release_db_connection_none():
    release_db_connection(None)

@patch("send_final_status_to_ccil.psycopg2.pool.SimpleConnectionPool")
def test_initialize_db_pool_success(mock_pool):
    creds = dict(host='h', port=1, username='u', password='pw')
    initialize_db_pool(creds)
    mock_pool.assert_called_once()

def test_initialize_db_pool_failure():
    with pytest.raises(ValueError):
        initialize_db_pool(None)

@patch("send_final_status_to_ccil.boto3.client")
@patch("send_final_status_to_ccil.splunk")
def test_get_db_credentials_success(mock_splunk, mock_boto):
    mock_ssm = MagicMock()
    mock_secrets = MagicMock()
    mock_boto.side_effect = [mock_ssm, mock_secrets]
    mock_ssm.get_parameter.return_value = {'Parameter': {'Value': 'foo'}}
    creds = dict(host='x', port=1, username='u', password='pw')
    mock_secrets.get_secret_value.return_value = {'SecretString': json.dumps(creds)}
    out = get_db_credentials('ssmk')
    assert out['host'] == 'x' and out['username'] == 'u'

@patch("send_final_status_to_ccil.boto3.client")
@patch("send_final_status_to_ccil.splunk")
def test_get_db_credentials_failure(mock_splunk, mock_boto):
    mock_boto.side_effect = Exception("fail")
    assert get_db_credentials('ssmkey') is None
    mock_splunk.log_message.assert_called()


# --- get_client_orderid_from_transaction ---

@patch("send_final_status_to_ccil.splunk")
def test_get_client_orderid_from_transaction_success(mock_splunk):
    import send_final_status_to_ccil
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchall.return_value = [("o1","c1")]
    conn.cursor.return_value = cursor
    pool = MagicMock()
    pool.getconn.return_value = conn
    send_final_status_to_ccil.db_pool = pool
    out = get_client_orderid_from_transaction("co", "c1", "t1")
    assert out == [("o1","c1")]

@patch('send_final_status_to_ccil.splunk')
def test_get_client_orderid_from_transaction_psycopg2error(mock_splunk):
    import send_final_status_to_ccil
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = psycopg2.Error()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_pool = MagicMock()
    mock_pool.getconn.return_value = mock_conn
    send_final_status_to_ccil.db_pool = mock_pool
    with pytest.raises(Exception):
        get_client_orderid_from_transaction('cx', 'a', 'b')
    mock_splunk.log_message.assert_called()


# --- get_processing_orders_bulk (success, error, branch/finally) ---

@patch('send_final_status_to_ccil.splunk')
def test_get_processing_orders_bulk_success(mock_splunk):
    import send_final_status_to_ccil
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("order1", "client-a", '{"foo":"bar"}', "corr1", "trace1")]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_pool = MagicMock()
    mock_pool.getconn.return_value = mock_conn
    send_final_status_to_ccil.db_pool = mock_pool
    result = send_final_status_to_ccil.get_processing_orders_bulk('ctx', ["o1"], ["c1"])
    assert isinstance(result, list)
    assert result[0][0] == "order1"
    # check cleanups
    mock_cursor.close.assert_called()
    mock_pool.putconn.assert_called()

@patch("send_final_status_to_ccil.get_db_connection")
@patch("send_final_status_to_ccil.splunk")
def test_get_processing_orders_bulk_cursor_assignment_fails(mock_splunk, mock_get_conn):
    import send_final_status_to_ccil
    mock_conn = MagicMock()
    mock_conn.cursor.side_effect = psycopg2.Error()
    mock_get_conn.return_value = mock_conn
    with pytest.raises(Exception):
        send_final_status_to_ccil.get_processing_orders_bulk("ct", ["a"], ["b"])
    mock_conn.rollback.assert_called_once()


# --- send_to_kafka_api ---
from send_final_status_to_ccil import get_kafka_message
import send_final_status_to_ccil
from unittest.mock import patch

@patch("send_final_status_to_ccil.splunk")
def test_get_kafka_message_all_else_branches(mock_splunk):
    send_final_status_to_ccil.destination = "BRCC"  # make sure it's set!
    doc = {
        "communicationDocumentSequenceNumber": 99
        # No communicationActionRegistry, communicationIndices, communicationDetail
    }
    record_json = {
        "communicationCommon": {"orderId": "YES"},
        "communicationDocuments": [doc]
    }
    result = get_kafka_message("ctx", record_json, "XCORR", "XTRACE")
    doc_part = result[0]["payload"]["communicationDocuments"][0]
    print(doc_part)
    assert doc_part["documentId"] == 99
    assert doc_part["communicationActionRegistry"] is None
    assert doc_part["communicationIndices"] is None
    assert doc_part["communicationDetail"] is None

@patch("send_final_status_to_ccil.requests.post")
@patch("send_final_status_to_ccil.splunk")
def test_send_to_kafka_api_requests_exception_full_path(mock_splunk, mock_post):
    mock_post.side_effect = requests.RequestException("fail!")
    assert not send_to_kafka_api("ctx", {"detail":{"correlationId": "x"}})
    mock_splunk.log_message.assert_called()

@patch("send_final_status_to_ccil.requests.post")
@patch("send_final_status_to_ccil.splunk")
def test_send_to_kafka_api_success(mock_splunk, mock_post):
    mock_post.return_value.status_code = 200
    msg = {"detail":{"correlationId":"a", "traceId":"t"}}
    assert send_to_kafka_api("ctx", msg) is True


# --- get_kafka_message ---

@pytest.fixture
def base_record():
    return {
        "communicationCommon": {
            "orderId": "12345",
            "communicationNarrativeType": "INFO",
            "communicationNarrativeValue": "Test narrative",
            "communicationIdentifierType": "requestId",
            "communicationIdentifierValue": "req-001",
            "nonStandardItems": {
                "businessUnitType": "BU1",
                "departmentId": "D001",
                "notificationProperties": {"key": "value"}
            }
        },
        "communicationDocuments": [
            {
                "documentId": "doc1",
                "communicationDocumentSequenceNumber": 1,
                "communicationActionRegistry": {"action": "print"},
                "communicationIndices": {"index": "001"},
                "communicationDetail": {"detail": "details"}
            },
            {
                "communicationDocumentSequenceNumber": 2  # No documentId
            }
        ]
    }


@patch("send_final_status_to_ccil.splunk")
@patch("send_final_status_to_ccil.datetime")
def test_get_kafka_message_full_coverage(mock_splunk, mock_datetime, base_record):
    # Create a fixed datetime object
    fixed_datetime = datetime(2025, 7, 18, tzinfo=timezone.utc)
    
    # Mock datetime.now() to return the fixed datetime
    mock_datetime.now.return_value = fixed_datetime
    mock_datetime.timezone = timezone  # Ensure timezone.utc is available

    context = {}
    correlation_id = "corr-123"
    traceid = "trace-456"

    result = get_kafka_message(context, base_record, correlation_id, traceid)

    assert isinstance(result, list)
    assert result[0]["detail"]["orderId"] == "12345"
    assert result[0]["payload"]["orderEchoback"]["requestid"] == "req-001"
    assert len(result[0]["payload"]["communicationDocuments"]) == 2

@patch("send_final_status_to_ccil.splunk")
def test_get_kafka_message_success(mock_splunk):
    rec = {
        "communicationCommon": {"orderId": "oid", "communicationNarrativeType":"t","communicationNarrativeValue":"v"},
        "communicationDocuments": [{"documentId": "docid", "communicationDocumentSequenceNumber": 5}]
    }
    res = get_kafka_message("ct", rec, "c","t")
    assert isinstance(res, list)
    assert res[0]["detail"]["orderId"] == "oid"


@patch("send_final_status_to_ccil.splunk")
def test_get_kafka_message_type_error(mock_splunk):
    with pytest.raises(Exception):
        get_kafka_message("ctx", ["not_a_dict"], "cid", "tid")
    mock_splunk.log_message.assert_called()

@patch("send_final_status_to_ccil.splunk")
def test_get_kafka_message_documentId_fallback(mock_splunk):
    rec = {
        "communicationCommon": {"orderId": "oid"},
        "communicationDocuments": [{"communicationDocumentSequenceNumber": 42}]
    }
    out = get_kafka_message("ctx", rec, "cid", "tid")
    assert out[0]["payload"]["communicationDocuments"][0]["documentId"] == 42

# --- lambda_handler ---

@patch("send_final_status_to_ccil.splunk")
def test_lambda_handler_missing_param(mock_splunk, context):
    event = {}
    with pytest.raises(Exception):
        lambda_handler(event, context)
    mock_splunk.log_message.assert_called()

@patch("send_final_status_to_ccil.splunk")
def test_lambda_handler_missing_db_credentials(mock_splunk, context):
    import send_final_status_to_ccil
    send_final_status_to_ccil.db_pool = None
    from send_final_status_to_ccil import lambda_handler
    with patch("send_final_status_to_ccil.get_db_credentials", return_value=None):
        event = {
            "client": "client",
            "transaction_id": "tid",
            "ssmdbkey": "ssmkey"
        }
        context = MagicMock()
        with pytest.raises(Exception, match="Failed to retrieve DB credentials"):
            lambda_handler(event, context)


@patch("send_final_status_to_ccil.get_db_credentials")
@patch("send_final_status_to_ccil.initialize_db_pool")
@patch("send_final_status_to_ccil.get_client_orderid_from_transaction")
@patch("send_final_status_to_ccil.splunk")
def test_lambda_handler_no_orders_branch(
    mock_splunk, mock_get_orders, mock_init, mock_get_db_creds
):
    mock_get_db_creds.return_value = {"host": "1", "port": 1, "username": "a", "password": "b"}
    mock_get_orders.return_value = [] # triggers no orders
    evt = {"client": "c", "transaction_id": "t", "ssmdbkey": "k"}
    with pytest.raises(Exception):
        lambda_handler(evt, "ctx")
    # verify log_message for no orders
    mock_splunk.log_message.assert_any_call(
        {"Status": "success", "Message": "No orders found for client_id: c, transaction_id: t"}, "ctx"
    )

@patch("send_final_status_to_ccil.get_db_credentials")
@patch("send_final_status_to_ccil.initialize_db_pool")
@patch("send_final_status_to_ccil.get_client_orderid_from_transaction")
@patch("send_final_status_to_ccil.get_processing_orders_bulk")
@patch("send_final_status_to_ccil.get_kafka_message")
@patch("send_final_status_to_ccil.send_to_kafka_api")
@patch("send_final_status_to_ccil.splunk")
def test_lambda_handler_kafka_retries(mock_splunk, m_send, m_kmsg, m_bulk, m_orderid, m_initdb, m_creds):
    m_creds.return_value = {"host":"h", "port":1, "username":"u", "password":"pw"}
    m_orderid.return_value = [("o","c")]
    m_bulk.return_value = [("o","c", '{"communicationCommon":{"orderId":"o"}, "communicationDocuments":[]}', "corr", "trc")]
    m_kmsg.return_value = [{"detail": {"orderId":"o"}, "payload": {}}]
    m_send.return_value = False
    evt = dict(client="c", transaction_id="t", ssmdbkey="ssm")
    with pytest.raises(Exception, match="Kafka API call failed"):
        lambda_handler(evt, "ctx")  # hits ALL retry lines
    retry_msgs = [c[0][0] for c in mock_splunk.log_message.call_args_list if "Retrying to send message" in c[0][0].get("Message","")]
    assert len(retry_msgs) == 3

@patch("send_final_status_to_ccil.splunk")
@patch("send_final_status_to_ccil.get_db_credentials", return_value={"user": "u", "pass": "p"})
@patch("send_final_status_to_ccil.initialize_db_pool")
@patch("send_final_status_to_ccil.get_client_orderid_from_transaction", return_value=[("order1", "client123")])
@patch("send_final_status_to_ccil.get_processing_orders_bulk", return_value=[("order1", "client123", json.dumps({"communicationCommon": {"orderId": "order1"}, "communicationDocuments": [{"documentId": "doc1", "communicationDocumentSequenceNumber": 1}]}), "corr-id", "trace-id")])
@patch("send_final_status_to_ccil.get_kafka_message", return_value=[[{"detail": {}, "payload": {}}]])
def test_lambda_handler_kafka_retry_success(mock_splunk, mock_kafka, mock_proc, mock_orders, mock_init, mock_creds):
    from send_final_status_to_ccil import lambda_handler

    # Simulate Kafka send: fail once, then succeed
    send_mock = MagicMock(side_effect=[False, True])
    
    with patch("send_final_status_to_ccil.send_to_kafka_api", send_mock):
        event = {
            "client": "client123",
            "transaction_id": "txn456",
            "ssmdbkey": "dbkey",
            "destinationType": "onprem"
        }
        result = lambda_handler(event, {})
        assert result["statusCode"] == 200
        assert send_mock.call_count == 2  # One initial + one retry

@patch("send_final_status_to_ccil.get_db_credentials")
@patch("send_final_status_to_ccil.initialize_db_pool")
@patch("send_final_status_to_ccil.get_client_orderid_from_transaction")
@patch("send_final_status_to_ccil.get_processing_orders_bulk")
@patch("send_final_status_to_ccil.splunk")
def test_lambda_handler_no_processing_orders_branch(
    mock_splunk, mock_get_processing_orders, mock_get_orders, mock_init, mock_get_db_creds):
    mock_get_db_creds.return_value = {"host": "localhost", "port": 1, "username": "a", "password": "p"}
    mock_get_orders.return_value = [("o","c")]
    mock_get_processing_orders.return_value = []   # <------
    evt = {"client": "c", "transaction_id": "t", "ssmdbkey": "key"}
    with pytest.raises(Exception):
        lambda_handler(evt, "ctx")
    assert any("No processing orders found" in str(call[0][0]) for call in mock_splunk.log_message.call_args_list)

@pytest.fixture
def base_event():
    return {
        "client": "client123",
        "transaction_id": "txn456",
        "ssmdbkey": "dbkey",
        "destinationType": "onprem"
    }

@patch("send_final_status_to_ccil.splunk")
@patch("send_final_status_to_ccil.get_db_credentials", return_value={"user": "u", "pass": "p"})
@patch("send_final_status_to_ccil.initialize_db_pool")
@patch("send_final_status_to_ccil.get_client_orderid_from_transaction", return_value=[("order1", "client123")])
@patch("send_final_status_to_ccil.get_processing_orders_bulk", return_value=[("order1", "client123", json.dumps({"communicationCommon": {"orderId": "order1"}, "communicationDocuments": [{"documentId": "doc1", "communicationDocumentSequenceNumber": 1}]}), "corr-id", "trace-id")])
@patch("send_final_status_to_ccil.get_kafka_message", return_value=[[{"detail": {}, "payload": {}}]])
@patch("send_final_status_to_ccil.send_to_kafka_api", return_value=True)
def test_lambda_handler_success(mock_splunk, mock_send, mock_kafka, mock_proc, mock_orders, mock_init, mock_creds, base_event):
    from send_final_status_to_ccil import lambda_handler
    result = lambda_handler(base_event, {})
    assert result["statusCode"] == 200


@patch("send_final_status_to_ccil.splunk")
@patch("send_final_status_to_ccil.get_db_credentials", return_value={"user": "u", "pass": "p"})
@patch("send_final_status_to_ccil.initialize_db_pool")
@patch("send_final_status_to_ccil.get_client_orderid_from_transaction", return_value=[("order1", "client123")])
@patch("send_final_status_to_ccil.get_processing_orders_bulk", return_value=[("order1", "client123", json.dumps({"communicationCommon": {"orderId": "order1"}, "communicationDocuments": [{"documentId": "doc1", "communicationDocumentSequenceNumber": 1}]}), "corr-id", "trace-id")])
@patch("send_final_status_to_ccil.get_kafka_message", return_value=[[{"detail": {}, "payload": {}}]])
@patch("send_final_status_to_ccil.send_to_kafka_api", return_value=True)
def test_lambda_handler_marcomm_destination(mock_splunk, mock_send, mock_kafka, mock_proc, mock_orders, mock_init, mock_creds):
    from send_final_status_to_ccil import lambda_handler
    event = {
        "client": "client123",
        "transaction_id": "txn456",
        "ssmdbkey": "dbkey",
        "destinationType": "marcomm"
    }
    result = lambda_handler(event, {})
    assert result["statusCode"] == 200


@patch("send_final_status_to_ccil.splunk")
@patch("send_final_status_to_ccil.get_db_credentials", return_value={"user": "u"})
@patch("send_final_status_to_ccil.initialize_db_pool")
@patch("send_final_status_to_ccil.get_client_orderid_from_transaction", side_effect=Exception("DB error"))
@patch("send_final_status_to_ccil.traceback.format_exc", return_value="Traceback error")
def test_lambda_handler_order_fetch_fail(mock_splunk, mock_trace, mock_orders, mock_init, mock_creds):
    from send_final_status_to_ccil import lambda_handler
    event = {"client": "c", "transaction_id": "t", "ssmdbkey": "k"}
    with pytest.raises(Exception, match="Error fetching data from order_transactions table"):
        lambda_handler(event, {})


@patch("send_final_status_to_ccil.splunk")
@patch("send_final_status_to_ccil.get_db_credentials", return_value={"user": "u", "pass": "p"})
@patch("send_final_status_to_ccil.initialize_db_pool")
@patch("send_final_status_to_ccil.get_client_orderid_from_transaction", return_value=[("order1", "client123")])
@patch("send_final_status_to_ccil.get_processing_orders_bulk", side_effect=Exception("Order table error"))
@patch("send_final_status_to_ccil.traceback.format_exc", return_value="Traceback error")
def test_lambda_handler_processing_orders_exception(mock_slunk, mock_trace, mock_proc, mock_orders, mock_init, mock_creds):
    from send_final_status_to_ccil import lambda_handler
    event = {
        "client": "client123",
        "transaction_id": "txn456",
        "ssmdbkey": "dbkey",
        "destinationType": "onprem"
    }

    with pytest.raises(Exception) as excinfo:
        lambda_handler(event, {})

    assert "Error fetching data from order table" in str(excinfo.value)
    mock_proc.assert_called_once()
    mock_trace.assert_called_once()
