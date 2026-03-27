import json
import boto3
from sqlalchemy import create_engine, Table, MetaData, select, Enum # type: ignore
from sqlalchemy import text
from sqlalchemy import delete,update
import traceback
import splunk # type: ignore
from collections import namedtuple, defaultdict

import re
secretsManager = boto3.client("secretsmanager")

class DataPostgres(object):
    def __init__(self, region, ssmdbkey, dbname, schema, run_id):
        self.conn = None
        self.engine = None
        self.region=region
        self.dbkey=ssmdbkey
        self.dbname=dbname
        self.schema=schema
        self.run_id = run_id
        self.document = namedtuple('Document', ['orderid', 's3location', 'documentid'])
        self.create_connection()

    
    def get_secret_values(self, secret_name):
        response = secretsManager.get_secret_value(SecretId = secret_name)
        secret_value = response['SecretString']
        json_secret_value = json.loads(secret_value)
        return json_secret_value

    def create_connection(self):
        try:
            
            ssm = boto3.client('ssm', self.region)
            secret_name = ssm.get_parameter(Name=self.dbkey, WithDecryption=True)
            secret_name=secret_name['Parameter']['Value']
            host = self.get_secret_values(secret_name)
            self.engine = create_engine(f"postgresql://{host['username']}:{host['password']}@{host['host']}:{host['port']}/{self.dbname}", future=True)
            self.conn = self.engine.connect()
            splunk.log_message({'Status': 'success', 'Message': 'Connection with DBCluster is established'}, self.run_id)
        except Exception as e:
            splunk.log_message({'Status':'failed', 'Message': traceback.format_exc()}, self.run_id)
            raise ValueError(f'Error creating connection: {traceback.format_exc()}')


    
    def getDocuments(self, transactionid):
        try:
            meta = MetaData(schema=self.schema)

            transactions, retrievals = Table('order_transactions', meta, autoload=True, autoload_with=self.engine), Table('retrieve_table', meta, autoload=True, autoload_with=self.engine)
    
            with self.engine.connect() as connection:
                results = (connection.execute( select(retrievals).select_from(retrievals.join(transactions, (transactions.c.orderid == retrievals.c.orderid) & (transactions.c.transactionid == transactionid))))).fetchall()
                documents = self.getDocument(results, retrievals)
                if not documents or len(documents) == 0: raise ValueError(f"No documents found for transactionid: {transactionid}")

                return documents
        except Exception as e:
            message = f"Document path retrieval process failed due to SQLAlchemy error. {traceback.format_exc()}"
            splunk.log_message({'Status': 'failed', 'Message': message}, self.run_id)
            raise ValueError(e)
        
    def getDocument(self, results, retrievals, fileNamePattern=".*composedFile$|.*composedFile.pdf$"):
        documents = []


        getValue = lambda result, colName: result._mapping[colName]
        for result in results:
            orderid, s3location, documentid = getValue(result, retrievals.c.orderid), getValue(result, retrievals.c.s3location), getValue(result, retrievals.c.document_id)
            s3location = s3location.split('://')[1] if '://' in s3location else s3location
            documents.append(self.document(orderid=orderid, s3location=s3location, documentid=documentid))
        return documents
    
    def removeOrder(self, order_id):
        try:
            meta = MetaData(schema=self.schema)

            transactionsData, ordersData = Table('order_transactions', meta, autoload=True, autoload_with=self.engine), Table('order_table', meta, autoload=True, autoload_with=self.engine)
            failed_message = "Failed to pull document from S3 location for one of the documents, reffer Splunk for more log info"
            orders = ','.join(['%s'] * len(order_id))
            splunk.log_message({'Status': 'Info', 'Message': 'Removing order from Transaction table', 'order':order_id}, self.run_id)
            with self.engine.connect() as connection:
                delete_query = delete(transactionsData).where(transactionsData.c.orderid.in_(order_id))
                splunk.log_message({'Status': 'Info', 'Message': 'Executing query : '+str(delete_query)}, self.run_id)
                connection.execute(delete_query)
                connection.commit()
            splunk.log_message({'Status': 'Info', 'Message': 'Updating order table with ERROR status', 'order':order_id, 'error_message':failed_message}, self.run_id)
            with self.engine.connect() as connection:
                update_query = update(ordersData).where(ordersData.c.orderid.in_(order_id)).values(status='ERROR',error_message=failed_message)
                splunk.log_message({'Status': 'Info', 'Message': 'Executing query : '+str(update_query)}, self.run_id)
                connection.execute(update_query)
                connection.commit()
        except Exception as e:
            message = f"Failed to remove order from table. {traceback.format_exc()}"
            splunk.log_message({'Status': 'Warning', 'Message': message, 'order':order_id}, self.run_id)