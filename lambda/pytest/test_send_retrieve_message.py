import os
import json
import pytest
import psycopg2
import requests
from unittest.mock import patch, MagicMock

os.environ["DB_NAME"] = "testdb"
os.environ["KAFKA_API_URL"] = "http://kafka/api"

from send_retrieve_message import (
    get_db_connection, release_db_connection, get_db_credentials, initialize_db_pool,
    get_orders, get_s3locations, get_kafka_message, send_to_kafka_api, update_order_status, #get_job_ids_from_s3,
    insert_transaction, is_s3_location_present, lambda_handler
)

@pytest.fixture
def context():
    return MagicMock()

@pytest.fixture
def sample_db_credentials():
    return {
        "host": "test-host",
        "port": 5432,
        "username": "test-user",
        "password": "test-pass"
    }

"""
# --- get_job_ids_from_s3 ---

@patch("send_retrieve_message.s3")
@patch("send_retrieve_message.splunk")
# def test_get_job_ids_from_s3_success(mock_splunk, mock_s3, context):
#     mock_s3.get_object.return_value = {'Body': MagicMock(read=MagicMock(return_value=b'job1,job2'))}
#     result = get_job_ids_from_s3(context, "bucket", "key")
#     assert result == ["job1", "job2"]
# 
@patch("send_retrieve_message.s3")
@patch("send_retrieve_message.splunk")
# def test_get_job_ids_from_s3_exception(mock_splunk, mock_s3, context):
#     mock_s3.get_object.side_effect = Exception("fail")
#     result = get_job_ids_from_s3(context, "bucket", "key")
#     assert result == []
#     mock_splunk.log_message.assert_called()
# """
# # --- DB Connection Utility Tests ---
# 
@patch("send_retrieve_message.db_pool")
def test_get_db_connection_returns_conn(mock_db_pool):
    conn = MagicMock()
    mock_db_pool.getconn.return_value = conn
    result = get_db_connection()
    assert result is conn
    mock_db_pool.getconn.assert_called_once()

@patch("send_retrieve_message.db_pool")
def test_get_db_connection_raises(mock_db_pool):
    mock_db_pool.getconn.side_effect = Exception("fail")
    with pytest.raises(Exception):
        get_db_connection()

@patch("send_retrieve_message.db_pool")
def test_release_db_connection_calls_putconn(mock_db_pool):
    conn = MagicMock()
    release_db_connection(conn)
    mock_db_pool.putconn.assert_called_once_with(conn)

def test_release_db_connection_none():
    # Should not raise if conn is None
    release_db_connection(None)

@patch("send_retrieve_message.db_pool")
def test_release_db_connection_putconn_raises(mock_db_pool):
    conn = MagicMock()
    mock_db_pool.putconn.side_effect = Exception("fail")
    with pytest.raises(Exception):
        release_db_connection(conn)

# --- DB Credentials & Pool ---

@patch("send_retrieve_message.boto3.client")
@patch("send_retrieve_message.splunk")
def test_get_db_credentials_success(mock_splunk, mock_boto, context):
    mock_ssm = MagicMock()
    mock_secretsmanager = MagicMock()
    mock_boto.side_effect = [mock_ssm, mock_secretsmanager]
    mock_ssm.get_parameter.return_value = {'Parameter': {'Value': 'test-secret-name'}}
    mock_secretsmanager.get_secret_value.return_value = {
        'SecretString': json.dumps({
            "host": "test-host",
            "port": 5432,
            "username": "test-user",
            "password": "test-pass"
        })
    }
    result = get_db_credentials(context, "test-key")
    assert result["host"] == "test-host"
    assert result["username"] == "test-user"

@patch("send_retrieve_message.boto3.client")
@patch("send_retrieve_message.splunk")
def test_get_db_credentials_failure(mock_splunk, mock_boto, context):
    mock_boto.side_effect = Exception("Connection error")
    result = get_db_credentials(context, "test-key")
    assert result is None
    mock_splunk.log_message.assert_called()

@patch("send_retrieve_message.psycopg2.pool.SimpleConnectionPool")
def test_initialize_db_pool_success(mock_pool, sample_db_credentials):
    initialize_db_pool(sample_db_credentials)
    mock_pool.assert_called_once()

def test_initialize_db_pool_failure():
    with pytest.raises(ValueError):
        initialize_db_pool(None)

# --- get_orders ---

@patch("send_retrieve_message.s3")
@patch("send_retrieve_message.splunk")
@patch("send_retrieve_message.db_pool")
def test_get_orders_success(mock_db_pool, mock_splunk, mock_s3):
    mock_s3.get_object.return_value = {
        'Body': MagicMock(read=MagicMock(return_value=b"123|456|{\"some\":\"data\"}|job1|client|COMPLETED\n"))
    }
    context = {}
    orders = get_orders(context, "bucket-name", "path/to/file.trig")

    # Now filter to match your old expectations (client, status)
    filtered = [o for o in orders if o['client'] == "client" and o['status'] == "COMPLETED"]
    assert filtered == [{
        'order_id': "123",
        'correlation_id': "456",
        'orderdata': "{\"some\":\"data\"}",
        'job_name': "job1",
        'client': "client",
        'status': "COMPLETED"
    }]

def test_get_orders_missing_s3_params():
    context = {}
    # New code does NOT take client_id/status as arguments!
    # *You will get an S3 error for missing params, not your function's raise!*
    with pytest.raises(Exception):
        get_orders(context, None, "some/path/file.trig")
    with pytest.raises(Exception):
        get_orders(context, "my-bucket", None)
    with pytest.raises(Exception):
        get_orders(context, None, None)

@patch("send_retrieve_message.s3")
@patch("send_retrieve_message.splunk")
@patch("send_retrieve_message.db_pool")
def test_get_orders_no_matching_status(mock_db_pool, mock_splunk, mock_s3):
    mock_s3.get_object.return_value = {
        'Body': MagicMock(read=MagicMock(return_value=b"123|456|{\"some\":\"data\"}|job1|client|COMPLETED\n"))
    }

    context = {}
    orders = get_orders(context, "bucket-name", "path/to/file.trig")
    # Filter for a status not present - should get empty
    filtered = [o for o in orders if o['client'] == "client" and o['status'] == "NON_EXISTENT_STATUS"]
    assert filtered == []

    # You can check that the original parsed order is still present
    assert orders[0]["status"] == "COMPLETED"

# --- get_s3locations ---

@patch("send_retrieve_message.db_pool")
@patch("send_retrieve_message.splunk")
def test_get_s3locations_success(mock_splunk, mock_db_pool, context):
    conn = MagicMock()
    cursor = MagicMock()
    mock_db_pool.getconn.return_value = conn
    conn.cursor.return_value = cursor
    cursor.fetchall.return_value = [("docid", "s3loc")]
    locs = get_s3locations(context, "oid")
    assert locs == [("docid", "s3loc")]

@patch("send_retrieve_message.db_pool")
@patch("send_retrieve_message.splunk")
def test_get_s3locations_error(mock_splunk, mock_db_pool, context):
    conn = MagicMock()
    cursor = MagicMock()
    mock_db_pool.getconn.return_value = conn
    conn.cursor.return_value = cursor
    cursor.execute.side_effect = Exception("fail")
    with pytest.raises(Exception):
        get_s3locations(context, "oid")
    mock_splunk.log_message.assert_called()

# --- get_kafka_message ---

@patch("send_retrieve_message.splunk")
def test_get_kafka_message_success(mock_splunk, context):
    record = {
        'order_id': "oid",
        'correlation_id': "cid",
        'orderdata': json.dumps({
            "communicationCommon": {"orderId": "oid"},
            "communicationDocuments": [{
                "documentId": "docid",
                "communicationDocumentSequenceNumber": 1,
                "communicationActions": [{
                    "communicationActionRegistry": [
                        {"communicationCustomKey": "RegistryItemDetailItemKey", "communicationCustomValue": "customVal"},
                        {"communicationCustomKey": "RegistryItemDetailItemKeyVersion", "communicationCustomValue": "v1"}
                    ]
                }],
                "nonStandardItems": {
                    "documentDetails": {
                        "staticDocument": {"businessIdentifier": "bid"},
                        "documentDeliveryDetail": {
                            "documentIndexes": {
                                "documentIndex": [
                                    {
                                        "uniqueDocumentInstanceKeyName": "someKey",
                                        "uniqueDocumentInstanceKeyValue": "someValue"
                                    }
                                ]
                            }
                        }
                    }
                }
            }]
        }),
        'job': "job"
    }

    msgs = get_kafka_message(context, record, "client", "docid", "s3loc/composedFile.pdf")
    assert isinstance(msgs, list)
    assert msgs[0]["documentRegistryIdentifier"] in ["client", "bid"]
    assert msgs[0]["communicationIndices"] == [
        {"communicationCustomKey": "someKey", "communicationCustomValue": "someValue"}
    ]


@patch("send_retrieve_message.splunk")
@patch("send_retrieve_message.uuid.uuid4", return_value="mocked-uuid")
def test_get_kafka_message_empty_corelation_id(mock_uuid, mock_splunk):
    from send_retrieve_message import get_kafka_message

    context = {}
    client_id = "client123"
    document_id = "doc1"
    s3_location = "s3://bucket/path/composedFile.pdf/composedFile"

    record = {
        'order_id': "order123",  # order_id
        'correlation_id': "",    # correlation_id (empty)
        'orderdata': json.dumps({
            "communicationCommon": {
                "orderId": "order123"
            },
            "communicationDocuments": [
                {
                    "documentId": "doc1",
                    "communicationDocumentSequenceNumber": 1
                }
            ]
        }),
        'job': "job-name"
    }

    result = get_kafka_message(context, record, client_id, document_id, s3_location)

    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["documentId"] == "doc1"
    assert result[0]["orderId"] == "order123"
    assert result[0]["s3locator"] == "s3://bucket/path/composedFile.pdf"



@patch("send_retrieve_message.splunk")
@patch("send_retrieve_message.uuid.uuid4", return_value="mocked-uuid")
def test_get_kafka_message_missing_document_id(mock_uuid, mock_splunk):
    from send_retrieve_message import get_kafka_message

    context = {}
    client_id = "client123"
    document_id = "doc999"  # This will be used as fallback
    s3_location = "s3://bucket/path/composedFile.pdf/composedFile"

    record = {
        'order_id': "order123",  # order_id
        'correlation_id': "",    # correlation_id (empty)
        'orderdata': json.dumps({
            "communicationCommon": {
                "orderId": "order123"
            },
            "communicationDocuments": [
                {
                    # No "documentId" key
                    "communicationDocumentSequenceNumber": 1
                }
            ]
        }),
        'job': "job-name"
    }

    result = get_kafka_message(context, record, client_id, document_id, s3_location)

    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["documentId"] == "doc999"  # ✅ This confirms the fallback path was used
    assert result[0]["orderId"] == "order123"



@patch("send_retrieve_message.splunk")
def test_get_kafka_message_document_id_mismatch(mock_splunk, context):
    record = {
        'order_id': "oid",
        'correlation_id': "cid",
        'orderdata': json.dumps({
            "communicationCommon": {"orderId": "oid"},
            "communicationDocuments": [{
                "documentId": "notdocid",
                "communicationDocumentSequenceNumber": 1,
                "communicationActions": [{"communicationActionRegistry": []}],
                "nonStandardItems": {
                    "documentDetails": {
                        "staticDocument": {"businessIdentifier": "bid"},
                        "documentDeliveryDetail": {"documentIndexes": {"documentIndex": "idx"}}
                    }
                }
            }]
        }),
        'job': "job"
    }
    msgs = get_kafka_message(context, record, "client", "docid", "s3loc/composedFile.pdf")
    assert msgs == []

@patch("send_retrieve_message.splunk")
def test_get_kafka_message_missing_fields(mock_splunk, context):
    record = ["oid", "cid", json.dumps({}), "job"]
    msgs = get_kafka_message(context, record, "client", "docid", "s3loc/composedFile.pdf")
    assert msgs is None or isinstance(msgs, list)
    mock_splunk.log_message.assert_called()

# --- send_to_kafka_api ---

@patch("send_retrieve_message.requests.post")
@patch("send_retrieve_message.splunk")
@pytest.mark.parametrize("status_code,expected", [
    (200, True),
    (500, False)
])
def test_send_to_kafka_api_variants(mock_splunk, mock_post, context, status_code, expected):
    mock_post.return_value.status_code = status_code
    result = send_to_kafka_api(context, [{"msg": "test"}])
    assert result is expected
    mock_splunk.log_message.assert_called()

@patch("send_retrieve_message.requests.post")
@patch("send_retrieve_message.splunk")
def test_send_to_kafka_api_exception(mock_splunk, mock_post, context):
    mock_post.side_effect = requests.RequestException("fail")
    result = send_to_kafka_api(context, [{"msg": "test"}])
    assert result is False
    mock_splunk.log_message.assert_called()

@patch("send_retrieve_message.requests.post")
@patch("send_retrieve_message.splunk")
def test_send_to_kafka_api_success_log(mock_splunk, mock_post, context):
    mock_post.return_value.status_code = 200
    result = send_to_kafka_api(context, [{"msg": "test"}])
    assert result is True
    assert any("Success" in str(args[0]["Status"]) for args, kwargs in mock_splunk.log_message.call_args_list)

# --- update_order_status ---

@patch("send_retrieve_message.db_pool")
@patch("send_retrieve_message.splunk")
def test_update_order_status_success(mock_splunk, mock_db_pool, context):
    conn = MagicMock()
    cursor = MagicMock()
    mock_db_pool.getconn.return_value = conn
    conn.cursor.return_value = cursor
    update_order_status(context, "client", "job", "PROCESSING", "oid")
    cursor.execute.assert_called()
    conn.commit.assert_called()

@patch("send_retrieve_message.db_pool")
@patch("send_retrieve_message.splunk")
def test_update_order_status_error(mock_splunk, mock_db_pool, context):
    conn = MagicMock()
    cursor = MagicMock()
    mock_db_pool.getconn.return_value = conn
    conn.cursor.return_value = cursor
    cursor.execute.side_effect = psycopg2.Error("fail")
    with pytest.raises(Exception):
        update_order_status(context, "client", "job", "PROCESSING", "oid")
    mock_splunk.log_message.assert_called()

@patch("send_retrieve_message.s3")
def test_get_orders_raises_if_no_records(mock_s3):
    import send_retrieve_message

    # All lines invalid (either wrong format or missing JSON)
    file_content = "abc|def\n" \
                   "nojsonhere\n"

    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=file_content.encode()))
    }

    context = {}
    with pytest.raises(Exception, match="No valid orders found in the file."):
        send_retrieve_message.get_orders(context, "bucket", "file.trig")

@patch("send_retrieve_message.s3")
def test_get_orders_skip_bad_buffer_parse_good_record(mock_s3):
    import send_retrieve_message

    file_content = (
        "BADHEADER\n"
        "{\"foo\": 1}|jobX|client|COMPLETED\n"
        "123|456|{\"data\":1}|job2|client|COMPLETED\n"
    )
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=file_content.encode()))
    }
    context = {}
    orders = send_retrieve_message.get_orders(context, "bucket", "file.trig")
    assert len(orders) == 1
    assert orders[0]["order_id"] == "123"

# --- insert_transaction ---

@patch("send_retrieve_message.db_pool")
@patch("send_retrieve_message.splunk")
def test_insert_transaction_success(mock_splunk, mock_db_pool, context):
    conn = MagicMock()
    cursor = MagicMock()
    mock_db_pool.getconn.return_value = conn
    conn.cursor.return_value = cursor
    insert_transaction(context, "oid", "tid", "client")
    cursor.execute.assert_called()
    conn.commit.assert_called()

@patch("send_retrieve_message.db_pool")
@patch("send_retrieve_message.splunk")
def test_insert_transaction_error(mock_splunk, mock_db_pool, context):
    conn = MagicMock()
    cursor = MagicMock()
    mock_db_pool.getconn.return_value = conn
    conn.cursor.return_value = cursor
    cursor.execute.side_effect = psycopg2.Error("fail")
    with pytest.raises(Exception):
        insert_transaction(context, "oid", "tid", "client")
    mock_splunk.log_message.assert_called()

# --- is_s3_location_present ---

@patch("send_retrieve_message.s3")
@patch("send_retrieve_message.splunk")
@pytest.mark.parametrize("side_effect,expected", [
    (None, True),
    ([Exception("fail"), Exception("fail")], False)
])
def test_is_s3_location_present_variants(mock_splunk, mock_s3, context, side_effect, expected):
    if side_effect:
        mock_s3.get_object.side_effect = side_effect
    else:
        mock_s3.get_object.return_value = {}
    result = is_s3_location_present(context, "s3://bucket/key.pdf")
    assert result is expected
    mock_splunk.log_message.assert_called()

@patch("send_retrieve_message.s3")
@patch("send_retrieve_message.splunk")
def test_is_s3_location_present_invalid_format(mock_splunk, mock_s3, context):
    result = is_s3_location_present(context, "invalid_format")
    assert result in [True, False]  # Should not raise
    mock_splunk.log_message.assert_called()

@patch("send_retrieve_message.s3")
@patch("send_retrieve_message.splunk")
def test_is_s3_location_present_appends_pdf(mock_splunk, mock_s3, context):
    # First call fails, second call (with .pdf) succeeds
    mock_s3.get_object.side_effect = [
        Exception("Not Found"),  # First attempt
        {}  # Second attempt with .pdf
    ]

    result = is_s3_location_present(context, "s3://bucket/key_without_pdf")

    assert result is True
    assert mock_s3.get_object.call_count == 2
    mock_splunk.log_message.assert_any_call(
        {"Status": "Info", "Message": "Checking for file with : key_without_pdf.pdf"},
        context
    )


# --- lambda_handler (all branches) ---

@pytest.fixture
def valid_event():
    return {
        "clientName": "client_123",
        "transactionId": "trans_456",
        "ssmDBKey": "db_key",
        "bucket": "test-bucket",
        "objectKey": "input/file.csv"
    }


@patch("send_retrieve_message.splunk")
def test_lambda_handler_missing_param(mock_splunk, context):
    event = {}
    with pytest.raises(Exception):
        lambda_handler(event, context)
    mock_splunk.log_message.assert_called()

@patch("send_retrieve_message.splunk")
def test_lambda_handler_missing_db_credentials(mock_splunk, context):
    import send_retrieve_message
    send_retrieve_message.db_pool = None
    from send_retrieve_message import lambda_handler
    with patch("send_retrieve_message.get_db_credentials", return_value=None):
        event = {
            "clientName": "client",
            "transactionId": "tid",
            "ssmDBKey": "ssmkey",
            "bucket": "bucket",
            "objectKey": "key"
        }
        with pytest.raises(Exception, match="Failed to retrieve DB credentials"):
            lambda_handler(event, context)

@patch("send_retrieve_message.insert_transaction")
@patch("send_retrieve_message.update_order_status")
@patch("send_retrieve_message.send_to_kafka_api")
@patch("send_retrieve_message.get_kafka_message")
@patch("send_retrieve_message.is_s3_location_present")
@patch("send_retrieve_message.get_s3locations")
@patch("send_retrieve_message.get_orders")
@patch("send_retrieve_message.get_db_credentials")
@patch("send_retrieve_message.initialize_db_pool")
@patch("send_retrieve_message.splunk")
def test_lambda_handler_initializes_db_pool(
    mock_splunk, mock_init_db, mock_get_db_creds, mock_get_orders,
    mock_get_s3locations, mock_is_s3_present, mock_get_kafka_msg,
    mock_send_kafka, mock_update_status, mock_insert_txn,
    valid_event, context
):
    import send_retrieve_message
    send_retrieve_message.db_pool = None

    mock_get_db_creds.return_value = {"user": "dbuser", "password": "dbpass"}
    mock_get_orders.return_value = [{
        "order_id": "order1",
        "correlation_id": None,
        "orderdata": None,
        "job_name": "job1",
        "client": valid_event["clientName"],
        "status": "COMPLETED"
    }]
    mock_get_s3locations.return_value = [["loc1", "s3://bucket/file.pdf"]]
    mock_is_s3_present.return_value = True

    response = send_retrieve_message.lambda_handler(valid_event, context)

    assert response["statusCode"] == 200
    mock_init_db.assert_called_once_with({"user": "dbuser", "password": "dbpass"})

@patch("send_retrieve_message.get_db_credentials")
@patch("send_retrieve_message.initialize_db_pool")
@patch("send_retrieve_message.db_pool")
@patch("send_retrieve_message.get_orders")
@patch("send_retrieve_message.splunk")
def test_lambda_handler_get_orders_fail(mock_splunk, mock_get_orders, mock_initialize_db_pool, mock_get_db_credentials, mock_get_job_ids_from_s3, context):
    #import send_retrieve_message
    #send_retrieve_message.db_pool = None
    event = {
        "clientName": "client",
        "transactionId": "tid",
        "ssmDBKey": "ssmkey",
        "bucket": "bucket",
        "objectKey": "key"
    }
    mock_get_job_ids_from_s3.return_value = ["job1"]
    mock_get_db_credentials.return_value = {"host": "h", "port": 1, "username": "u", "password": "p"}
    mock_get_orders.side_effect = Exception("fail")
    with pytest.raises(Exception):
        lambda_handler(event, context)
    mock_splunk.log_message.assert_called()


@patch("send_retrieve_message.get_db_credentials")
@patch("send_retrieve_message.initialize_db_pool")
@patch("send_retrieve_message.get_orders")
@patch("send_retrieve_message.splunk")
def test_lambda_handler_no_orders_found(
    mock_splunk, mock_get_orders, mock_init_db, mock_get_db_creds,
    valid_event, context
):
    import send_retrieve_message
    send_retrieve_message.db_pool = None

    mock_get_db_creds.return_value = {"user": "dbuser", "password": "dbpass"}
    mock_get_orders.return_value = []

    with pytest.raises(Exception, match="No orders found for client_id: client_123, status: 'COMPLETED'"):
        send_retrieve_message.lambda_handler(valid_event, context)

    mock_splunk.log_message.assert_any_call(
        {"Status": "Success", "Message": "No orders found for client_id: client_123, status: 'COMPLETED'"},
        context
    )

@patch("send_retrieve_message.insert_transaction")
@patch("send_retrieve_message.update_order_status")
@patch("send_retrieve_message.send_to_kafka_api")
@patch("send_retrieve_message.get_kafka_message")
@patch("send_retrieve_message.is_s3_location_present")
@patch("send_retrieve_message.get_s3locations")
@patch("send_retrieve_message.get_orders")
@patch("send_retrieve_message.get_db_credentials")
@patch("send_retrieve_message.initialize_db_pool")
@patch("send_retrieve_message.splunk")
def test_lambda_handler_file_missing_kafka_sent(
    mock_splunk, mock_init_db, mock_get_db_creds, mock_get_orders,
    mock_get_s3locations, mock_is_s3_present, mock_get_kafka_msg,
    mock_send_kafka, mock_update_status, mock_insert_txn,
    valid_event, context
):
    import send_retrieve_message
    send_retrieve_message.db_pool = None

    mock_get_db_creds.return_value = {"user": "dbuser", "password": "dbpass"}
    mock_get_orders.return_value = [{
        "order_id": "order1",
        "correlation_id": None,
        "orderdata": None,
        "job_name": "job1",
        "client": valid_event["clientName"],
        "status": "COMPLETED"
    }]
    mock_get_s3locations.return_value = [["loc1", "s3://bucket/file.pdf"]]
    mock_is_s3_present.return_value = False
    mock_get_kafka_msg.return_value = {"msg": "retrieve"}
    mock_send_kafka.return_value = True

    response = send_retrieve_message.lambda_handler(valid_event, context)

    assert response["statusCode"] == 200
    mock_get_kafka_msg.assert_called_once()
    mock_send_kafka.assert_called_once()
    mock_update_status.assert_called_once()
    mock_insert_txn.assert_called_once()

@patch("send_retrieve_message.insert_transaction")
@patch("send_retrieve_message.update_order_status")
@patch("send_retrieve_message.send_to_kafka_api")
@patch("send_retrieve_message.get_kafka_message")
@patch("send_retrieve_message.is_s3_location_present")
@patch("send_retrieve_message.get_s3locations")
@patch("send_retrieve_message.get_orders")
@patch("send_retrieve_message.get_db_credentials")
@patch("send_retrieve_message.initialize_db_pool")
@patch("send_retrieve_message.splunk")
def test_lambda_handler_file_missing_kafka_message_none(
    mock_splunk, mock_init_db, mock_get_db_creds, mock_get_orders,
    mock_get_s3locations, mock_is_s3_present, mock_get_kafka_msg,
    mock_send_kafka, mock_update_status, mock_insert_txn,
    valid_event, context
):
    import send_retrieve_message
    send_retrieve_message.db_pool = None

    mock_get_db_creds.return_value = {"user": "dbuser", "password": "dbpass"}
    mock_get_orders.return_value = [{
        "order_id": "order1",
        "correlation_id": None,
        "orderdata": None,
        "job_name": "job1",
        "client": valid_event["clientName"],
        "status": "COMPLETED"
    }]
    mock_get_s3locations.return_value = [["loc1", "s3://bucket/file.pdf"]]
    mock_is_s3_present.return_value = False
    mock_get_kafka_msg.return_value = None  # Simulate BAD DATA

    response = send_retrieve_message.lambda_handler(valid_event, context)

    assert response["statusCode"] == 200
    mock_get_kafka_msg.assert_called_once()
    mock_send_kafka.assert_not_called()
    mock_update_status.assert_called_once()
    mock_insert_txn.assert_not_called()
    mock_splunk.log_message.assert_any_call(
        {"Status": "Warning", "Message": "Order was not considered for Aggregation", "Reason": "Not able to create retrieve message, could be missing required fields", "order_id": "order1"},
        context
    )

@patch("send_retrieve_message.insert_transaction")
@patch("send_retrieve_message.update_order_status")
@patch("send_retrieve_message.send_to_kafka_api")
@patch("send_retrieve_message.get_kafka_message")
@patch("send_retrieve_message.is_s3_location_present")
@patch("send_retrieve_message.get_s3locations")
@patch("send_retrieve_message.get_orders")
@patch("send_retrieve_message.get_db_credentials")
@patch("send_retrieve_message.initialize_db_pool")
@patch("send_retrieve_message.splunk")
def test_lambda_handler_no_s3_locations(
    mock_splunk, mock_init_db, mock_get_db_creds, mock_get_orders,
    mock_get_s3locations, mock_is_s3_present, mock_get_kafka_msg,
    mock_send_kafka, mock_update_status, mock_insert_txn,
    valid_event, context
):
    import send_retrieve_message
    send_retrieve_message.db_pool = None

    mock_get_db_creds.return_value = {"user": "dbuser", "password": "dbpass"}
    mock_get_orders.return_value = [{
        "order_id": "order1",
        "correlation_id": None,
        "orderdata": None,
        "job_name": "job1",
        "client": valid_event["clientName"],
        "status": "COMPLETED"
    }]
    mock_get_s3locations.return_value = []  # No S3 locations

    response = send_retrieve_message.lambda_handler(valid_event, context)

    assert response["statusCode"] == 200
    mock_send_kafka.assert_not_called()
    mock_insert_txn.assert_not_called()
    mock_update_status.assert_called_once()
    mock_splunk.log_message.assert_any_call(
        {
            "Status": "Warning",
            "Message": "Order was not considered for Aggregation",
            "Reason": "No S3 locations were found for this order in retrieve_table",
            "order_id": "order1"
        },
        context
    )

@patch("send_retrieve_message.insert_transaction")
@patch("send_retrieve_message.update_order_status")
@patch("send_retrieve_message.send_to_kafka_api")
@patch("send_retrieve_message.get_kafka_message")
@patch("send_retrieve_message.is_s3_location_present")
@patch("send_retrieve_message.get_s3locations")
@patch("send_retrieve_message.get_orders")
@patch("send_retrieve_message.get_db_credentials")
@patch("send_retrieve_message.initialize_db_pool")
@patch("send_retrieve_message.splunk")
def test_lambda_handler_insert_transaction_failure(
    mock_splunk, mock_init_db, mock_get_db_creds, mock_get_orders,
    mock_get_s3locations, mock_is_s3_present, mock_get_kafka_msg,
    mock_send_kafka, mock_update_status, mock_insert_txn,
    valid_event, context
):
    import send_retrieve_message
    send_retrieve_message.db_pool = None

    mock_get_db_creds.return_value = {"user": "dbuser", "password": "dbpass"}
    mock_get_orders.return_value = [{
        "order_id": "order1",
        "correlation_id": None,
        "orderdata": None,
        "job_name": "job1",
        "client": valid_event["clientName"],
        "status": "COMPLETED"
    }]
    mock_get_s3locations.return_value = [["loc1", "s3://bucket/file.pdf"]]
    mock_is_s3_present.return_value = True
    mock_send_kafka.return_value = True
    mock_update_status.return_value = None
    mock_insert_txn.side_effect = Exception("DB insert failed")

    with pytest.raises(Exception, match=r"Failed to inserting transaction_id, client_id into order_transactions table: .*"):
        send_retrieve_message.lambda_handler(valid_event, context)

    mock_insert_txn.assert_called_once()
    mock_splunk.log_message.assert_called()

@patch("send_retrieve_message.insert_transaction")
@patch("send_retrieve_message.update_order_status")
@patch("send_retrieve_message.send_to_kafka_api")
@patch("send_retrieve_message.get_kafka_message")
@patch("send_retrieve_message.is_s3_location_present")
@patch("send_retrieve_message.get_s3locations")
@patch("send_retrieve_message.get_orders")
@patch("send_retrieve_message.get_db_credentials")
@patch("send_retrieve_message.initialize_db_pool")
@patch("send_retrieve_message.splunk")
@patch("send_retrieve_message.time.sleep", return_value=None)
def test_lambda_handler_kafka_send_fails_skips_update_insert(
    mock_sleep, mock_splunk, mock_init_db, mock_get_db_creds, mock_get_orders,
    mock_get_s3locations, mock_is_s3_present, mock_get_kafka_msg,
    mock_send_kafka, mock_update_status, mock_insert_txn,
    valid_event, context
):
    import send_retrieve_message
    send_retrieve_message.db_pool = None

    mock_get_db_creds.return_value = {"user": "dbuser", "password": "dbpass"}
    mock_get_orders.return_value = [{
        "order_id": "order1",
        "correlation_id": None,
        "orderdata": None,
        "job_name": "job1",
        "client": valid_event["clientName"],
        "status": "COMPLETED"
    }]
    mock_get_s3locations.return_value = [["loc1", "s3://bucket/file.pdf"]]
    mock_is_s3_present.return_value = False
    mock_get_kafka_msg.return_value = {"msg": "retrieve"}
    mock_send_kafka.return_value = False  # Simulate Kafka failure

    with pytest.raises(Exception, match=r"Failed to process orders: Skipping order order1 update .* Kafka failure"):
        send_retrieve_message.lambda_handler(valid_event, context)

    mock_update_status.assert_not_called()
    mock_insert_txn.assert_not_called()
    mock_splunk.log_message.assert_any_call(
        {"Status": "Failed", "Message": "Skipping order order1 update & transaction due to Kafka failure"},
        context
    )


@patch("send_retrieve_message.insert_transaction")
@patch("send_retrieve_message.update_order_status")
@patch("send_retrieve_message.send_to_kafka_api")
@patch("send_retrieve_message.get_kafka_message")
@patch("send_retrieve_message.is_s3_location_present")
@patch("send_retrieve_message.get_s3locations")
@patch("send_retrieve_message.get_orders")
@patch("send_retrieve_message.get_db_credentials")
@patch("send_retrieve_message.initialize_db_pool")
@patch("send_retrieve_message.splunk")
def test_lambda_handler_update_order_status_failure(
    mock_splunk, mock_init_db, mock_get_db_creds, mock_get_orders,
    mock_get_s3locations, mock_is_s3_present, mock_get_kafka_msg,
    mock_send_kafka, mock_update_status, mock_insert_txn,
    valid_event, context
):
    import send_retrieve_message
    send_retrieve_message.db_pool = None

    mock_get_db_creds.return_value = {"user": "dbuser", "password": "dbpass"}
    mock_get_orders.return_value = [{
        "order_id": "order1",
        "correlation_id": None,
        "orderdata": None,
        "job_name": "job1",
        "client": valid_event["clientName"],
        "status": "COMPLETED"
    }]
    mock_get_s3locations.return_value = [["loc1", "s3://bucket/file.pdf"]]
    mock_is_s3_present.return_value = True
    mock_send_kafka.return_value = True
    mock_get_kafka_msg.return_value = {"msg": "retrieve"}
    mock_update_status.side_effect = Exception("Update failed")

    with pytest.raises(Exception, match=r"Failed to update order status: .*"):
        send_retrieve_message.lambda_handler(valid_event, context)

    mock_update_status.assert_called_once()
    mock_insert_txn.assert_not_called()
    mock_splunk.log_message.assert_any_call(
        {"Status": "Info", "Message": "Retrying to send KAFKA message "},
        context
    )
    mock_splunk.log_message.assert_called()
