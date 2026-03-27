import json, sys, boto3, os
import splunk  # import the module from the s3 bucket
import time
import re
from awsglue.utils import getResolvedOptions
from sqlalchemy import create_engine, text
import requests
from datetime import datetime
import uuid

# Initialize Boto3 clients
s3 = boto3.client('s3')
ssm_client = boto3.client("secretsmanager")


def get_run_id():
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    return "arn:dpm:glue:extract_account_from_cdm:" + account_id


def get_secret_values(secret_name):
    response = ssm_client.get_secret_value(SecretId=secret_name)
    secret_value = response['SecretString']
    json_secret_value = json.loads(secret_value)

    return json_secret_value


class DataPostgres:
    def __init__(self):
        self.conn = None
        self.engine = None
        self.region = os.getenv("region")
        self.dbkey = os.getenv("dbkey")
        self.dbname = os.getenv("dbname")
        self.s3_bucket = os.getenv("input_bucket")
        self.input_file_name = os.getenv("input_filename")
        self.input_folder = os.getenv("input_folder")
        self.s3_wip = os.getenv("s3_wip")
        self.uuid_value = self.s3_wip.split("/")[-1]
        self.kafka_url_parameter = os.getenv("kafka_url_parameter")
        self.kafka_topic = os.getenv("kafka_topic")
        self.contact_type = ''
        self.schema = os.getenv("schema")
        self.summary_option = os.getenv("summary_option")
        self.schema_set_query = os.getenv("schema_set_query")
        self.function_call_query = os.getenv("function_call_query")
        self.filename = ""
        self.message = ""
        self.account_number = ""
        self.create_connection()
        self.execution_time = str(time.time())
        self.client_name = os.getenv("clientName")
        self.periodDateRange = os.getenv("periodDateRange")
        self.execution_time = datetime.now()
        self.runType = os.getenv("runType")
        self.contactTypeMap = os.getenv("contactTypeMap")
        self.filePattern = os.getenv("filePattern")
        self.client_id = ""
        #self.timestamp = ""
        self.timestamp = datetime.now().isoformat(timespec="milliseconds")
        self.origin = ""

    def create_connection(self):
        try:
            # creates engine connection
            ssm = boto3.client('ssm', self.region)
            ssmdbkey = self.dbkey
            secret_name = ssm.get_parameter(Name=ssmdbkey, WithDecryption=True)
            secret_name = secret_name['Parameter']['Value']
            host = get_secret_values(secret_name)
            self.engine = create_engine(
                f"postgresql://{host['username']}:{host['password']}@{host['host']}:{host['port']}/{self.dbname}",
                future=True)
            self.conn = self.engine.connect()
            result = splunk.log_message({'Status': 'success', 'InputFileName': 'test file',
                                         'Message': 'Connection with Aurora Postgres Cluster is established'},
                                        get_run_id())
        except Exception as e:
            self.message = "Connection failed due to error " + str(e)
            result = splunk.log_message({
                'InputFileName': 'test file',
                'Status': 'failed',
                'Message': self.message
            }, get_run_id())

            raise Exception(self.message)  # << To halt execution

    def extract_records(self):
        try:
            # this method will extract the data from trigger file name
            self.extract_data_from_trigger_file_name()

            # Execute query
            #schema_query = f"{self.schema_set_query} {self.schema}"
            #self.conn.execute(text(schema_query))

            #query = text(self.function_call_query)
            #result = self.conn.execute(query,
            #    {'identifier': self.account_number, 'clientId':self.client_id, 'origin': self.origin,
            #     'summary_option':self.summary_option})

            #records = result.fetchone()[0]
            
            with self.engine.connect() as connection:
                connection.execute(text(f"SET search_path TO {self.schema}"))
                result = connection.execute(
                    text(self.function_call_query),
                    {
                        'identifier': self.account_number,
                        'clientId': self.client_id,
                        'origin': self.origin,
                        'summary_option': self.summary_option
                    }
                )

                records = result.fetchone()[0]

                connection.commit()

            # Logging
            splunk.log_message({
                'Status': 'success',
                'InputFileName': 'test file',
                'Message': f"Fetched party records from database"
            }, get_run_id())

            return records

        except Exception as e:
            self.message = "Failed to extract records due to error: " + str(e)
            splunk.log_message({
                'Status': 'failed',
                'InputFileName': 'test file',
                'Message': self.message
            }, get_run_id())

            raise Exception(self.message)

    def extract_data_from_trigger_file_name(self):
        try:

            # Define the regular expression pattern to extract the part between the second underscore and the next underscore
            # pattern = r"(?:CIT_TEST_)?PATHFINDERSTATEMENT_([A-Z0-9]{8})_([0-9]{3})_([A-Z0-9._]+)_([0-9]{4})([0-1][0-9])([0-3][0-9])T([0-2][0-9])([0-5][0-9])([0-5][0-9])([0-9][0-9][0-9])\.trigger"  # (?:CIT_TEST_)? makes CIT_TEST_ optional
            pattern = self.filePattern
            # Search the filename using the regex pattern
            match = re.search(pattern, self.input_file_name)

            if match:
                # If a match is found, extract the desired part
                self.account_number = match.group(1)
                self.client_id = match.group(2)
                self.origin = match.group(3)
                #year = match.group(4)
                #month = match.group(5)
                #day = match.group(6)
                #hour = match.group(7)
                #minutes = match.group(8)
                #seconds = match.group(9)
                #milliseconds = match.group(10)
                #self.timestamp = year + '-' + month + '-' + day + 'T' + hour + ':' + minutes + ':' + seconds + '.' + milliseconds

            else:
                print("No match found.")

        except Exception as e:
            self.message = f"Failed to extract account number from filename: {str(e)}"
            print(self.message)
            raise Exception(self.message)

    def get_process_type(self, return_key):
        """
        To get the mid/mon values based on given periodDateRange

        Args:
            :

        Returns:
            string: this will return the name of the process type.

        """
        process_type = None
        # find current date.
        execution_time = self.execution_time
        current_day = execution_time.day

        # based on today find the name
        range_data = json.loads(self.periodDateRange)
        print(range_data)

        for data in range_data:
            separated = data["daterange"].split(" ")
            symbol = separated[0]
            number = int(separated[1])

            if symbol == ">" and current_day > number:
                if return_key in data:
                    process_type = data[return_key]
                    break
            elif symbol == "<" and current_day < number:
                if return_key in data:
                    process_type = data[return_key]
                    break

        if process_type is None:
            raise ValueError("No matching process found for the current day.")

        return process_type

    def s3_bucket_json_file_name_generator(self):
        """
        To generate the file name based on below format without extension(.json)
        File will be saved in the S3 bucket that will be further used by the wif-core team.

        attributes:
            self: class method and class attributes

        Response:
            string: name of the file that will be used to on the S3 bucket.
        """

        # To create the file name based on the below name without extension(.json)
        # ClientName_Acounttype_AccountNumber_processType_uniqueID.json

        # To get the Client Name:
        client_name = self.client_name

        # Program to get the Account Type:
        account_type = self.contact_type

        # To get the Account Number: self.account_number
        account_number = self.account_number

        # Program to get the processType:
        process_type = self.get_process_type(return_key="fileName")

        # returning the name of the file that need to save in the S3 bucket
        return f"{client_name}_{account_type}_{account_number}_{process_type}_{self.uuid_value}.json"

    def upload_to_s3(self, data):
        try:
            if not data:
                return

            # Old name
            # self.filename = f"{self.s3_wip}/{self.contact_type}_{self.account_number}.json"

            # new name
            full_file_name = self.s3_bucket_json_file_name_generator()

            # To check if the filename is existed and not blank
            if not full_file_name and full_file_name.strip() == "":
                raise ValueError("Filename cannot be blank or missing.")

            self.filename = f"{self.s3_wip}/{full_file_name}"

            # creating the content of the file
            json_data = json.dumps(data)

            s3.put_object(
                Bucket=self.s3_bucket,
                Key=self.filename,
                Body=json_data,
                ContentType="application/json"
            )

            splunk.log_message({
                'Status': 'success',
                'InputFileName': self.filename,
                'Message': f"Data successfully uploaded to S3"
            }, get_run_id())

        except Exception as e:
            self.message = f"Failed to upload to S3: {str(e)}"
            splunk.log_message({
                'Status': 'failed',
                'InputFileName': 'upload_to_s3',
                'Message': self.message
            }, get_run_id())

            raise Exception(self.message)

    def get_party_contact_type(self, data):
        try:
            contact_type_map = json.loads(self.contactTypeMap)

            if not data:
                return
            parties = data.get("Customer", {}).get("Party", [])

            # Loop through each array of party and its array of contacts
            for party in parties:
                contacts = party.get("PartyContact", [])
                for contact in contacts:
                    contact_type = contact.get("PartyContactType")

                    # Return the contacttype value if found
                    if contact_type in contact_type_map:
                        self.contact_type = contact_type_map[contact_type]
                        return

            self.message = f"Failed to get PartyContactType."
            raise Exception(self.message)

        except Exception as e:
            # log error message in splunk
            message = "Failed to extract records due to error: " + str(e)

            self.message = f"Failed to get PartyContactType: {str(e)}"
            splunk.log_message({
                'Status': 'failed',
                'InputFileName': 'get_party_contact_type',
                'Message': self.message
            }, get_run_id())

            raise Exception(self.message)

    def send_to_kafka_api(self):

        ssm = boto3.client('ssm', self.region)
        kafka_url = ssm.get_parameter(Name=self.kafka_url_parameter, WithDecryption=True)
        kafka_url = kafka_url['Parameter']['Value']
        kafka_url_with_topic = f"{kafka_url}{self.kafka_topic}"

        # Added the information regarding account its type and file on S3.
        fileLocations = [
            {
                "accountNumber": self.account_number,
                "type": self.contact_type,
                "fileName": self.filename,
            }
        ]

        message = {
            "clientName": self.client_name,
            "accountType": self.contact_type,
            "accountNumber": self.account_number,
            "uuid": self.uuid_value,
            "bucketName": self.s3_bucket,
            "runType": self.get_process_type(return_key="topicName"),
            "createdTimestamp": self.timestamp,
            "env": self.runType,
            "fileLocation": fileLocations
        }

        try:
            headers = {"Content-Type": "application/json", 'source': 'CCIL', 'Accept': 'application/json'}
            response = requests.post(kafka_url_with_topic, json=message, headers=headers)
            if response.status_code == 200:
                print('Message successfully sent to Kafka')
                self.message = f"Message is published successfully to KAFKA"
                splunk.log_message({
                    'Status': 'Success',
                    'InputFileName': 'send_to_kafka_api',
                    'Message': self.message
                }, get_run_id())
            else:
                print('Message failed sent to Kafka, status code:', response.status_code)
                self.message = f"Failed to publish message to KAFKA"
                splunk.log_message({
                    'Status': 'failed',
                    'InputFileName': 'send_to_kafka_api',
                    'Message': self.message
                }, get_run_id())
                raise Exception(self.message)

        # except requests.RequestException as e:
        except requests.RequestException as e:
            print(f"Filed to send message to kafka. Exception:{str(e)}")
            self.message = f"Failed to publish message to KAFKA: {str(e)}"
            splunk.log_message({
                'Status': 'failed',
                'InputFileName': 'send_to_kafka_api',
                'Message': self.message
            }, get_run_id())
            raise Exception(self.message)


def set_job_params_as_env_vars():
    # Loop through all command-line arguments starting from the second argument (skip the script name)
    for i in range(1, len(sys.argv), 2):
        if sys.argv[i].startswith('--'):
            key = sys.argv[i][2:]  # Remove the leading '--'
            value = sys.argv[i + 1]
            os.environ[key] = value


# main:
def main():
    try:
        set_job_params_as_env_vars()

    except Exception as e:
        message = f"failed to retrieve input parameters. {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
        raise Exception(message)

    try:
        dp = DataPostgres()
        result = dp.extract_records()
        dp.get_party_contact_type(result)
        dp.upload_to_s3(result)
        dp.send_to_kafka_api()

    except Exception as e:
        message = f"Extraction process failed. {str(e)}"
        splunk.log_message({'Status': 'failed', 'InputFileName': 'Test', 'Message': message}, get_run_id())
        raise Exception(message)


if __name__ == '__main__':
    main()