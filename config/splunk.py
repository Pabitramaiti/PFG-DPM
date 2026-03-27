import os
import time
import boto3
import json
import urllib.request
from urllib.parse import urlparse
from botocore.vendored import requests
from urllib.error import HTTPError, URLError
from urllib.request import urlopen, Request
from urllib.parse import urlencode
import time

error_flag=0

os.environ["SPLUNK_HEC_TOKEN"] = "219e2eeb-3367-6858-5d53-25393e08dd19"
os.environ["SPLUNK_HEC_URL"] = "https://http-inputs-br.splunkcloud.com/services/collector"
os.environ["application_name"] = 'Data Preparation Manager'     
os.environ["application_short_name"] = 'DPM'

# Define logger config using environment variables
logger_config = {
    'url': os.environ.get('SPLUNK_HEC_URL'),
    'token': os.environ.get('SPLUNK_HEC_TOKEN')
}

logger_config_dev = {
    'url': 'https://http-inputs-br-nprd.splunkcloud.com/services/collector',
    'token': '47ae2988-d442-1344-498a-d18179ff4adb'
}

logger_config_qa = {
    'url': 'https://http-inputs-br-nprd.splunkcloud.com/services/collector',
    'token': 'd77c7249-3ac9-d5a8-e921-d5fb5b61529f'
}


message_data = ""
os_environ_keys=[]

def store_message(event):
    message_data = message_data + "\n" + event
    
def flush(event,context):
    json_data = json.loads(event)
    json_data["logs"] = message_data
    log_message(json_data,context)

def log_message(event, context):
    #print('Received event:', event)
    extract_event_setenviron(event, context) 
    data ={}

    # Log JSON objects to Splunk
    # logger.log(event)

    # Log JSON objects with optional 'context' argument (recommended)
    # This adds valuable Lambda metadata including functionName as source, awsRequestId as field
    # logger.log(event, context)

    # Log strings
    # logger.log('value1 = {}'.format(event['key1']), context)

    # Log with user-specified timestamp - useful for forwarding events with embedded
    # timestamps, such as from AWS IoT, AWS Kinesis, AWS CloudWatch Logs
    # Change "int(time.time() * 1000)" below to event timestamp if specified in event payload
    # logger.log_with_time(int(time.time() * 1000), event, context)

    # Advanced:
    # Log event with user-specified request parameters - useful to set input settings per event vs token-level
    # Full list of request parameters available here:
    # http://docs.splunk.com/Documentation/Splunk/latest/RESTREF/RESTinput#services.2Fcollector
    
    my_session = boto3.session.Session()
    my_region = my_session.region_name
    account_id_value = context.split(':')[-1].strip()
    # os.environ["aws_account_id"] = context.split(':')[4]
    os.environ["aws_region"] = my_region
    os.environ["aws_account_id"]=account_id_value
    
    os_environ_keys.append("application_name")
    os_environ_keys.append("application_short_name")
    # os_environ_keys.append("aws_account_id")
    os_environ_keys.append("aws_region")
    os_environ_keys.append("aws_account_id")

    
    # account_id_value = context.split(':')[-1].strip()
    current_stage = 'PROD'
    # event['aws_account_id']=account_id_value
    
    if account_id_value == "187777304606" :
        current_stage = current_stage.replace('PROD','DEV')    
    elif account_id_value == "471112732183" :
        current_stage = current_stage.replace('PROD','PROD')    
    elif account_id_value == "556144470667" :
        current_stage = current_stage.replace('PROD','QA')
        
    if context.find("dpmdev-di") != -1:
        current_stage = current_stage.replace('DEV','INT')
    elif context.find("br_icsuat") != -1:
        current_stage = current_stage.replace('QA','UAT')
    
    os.environ["stage"] = current_stage
    os_environ_keys.append("stage")
    
    if current_stage == 'DEV' or current_stage == 'INT':
       logger = Logger(logger_config_dev)
    elif current_stage == 'QA' or current_stage == 'UAT':
       logger = Logger(logger_config_qa)
    else:
       logger = Logger(logger_config)
       
    for key in os_environ_keys:
        if key in os.environ:
            data[key] = os.environ[key]

    
    logger.log_event({
        'time': int(time.time() * 1000),
        'host': 'serverless',
        'source': 'glue job:{}'.format(context),
        'sourcetype': 'httpevent',
        'event': data
    })

    # Send all the events in a single batch to Splunk
    logger.flush_async()
def extract_event_setenviron(event_data, context):    
    try:
        for key, value in event_data.items():
            os.environ[key] = str(value)
            os_environ_keys.append(key)       
    except Exception as e:
        print('Failed in function : extract_event_setenviron '+str(e))


class Logger:
    def __init__(self, config):
        self.url = config['url']
        self.token = config['token']

        self.addMetadata = True
        self.setSource = True

        self.parsedUrl = urlparse(self.url)

        self.requestOptions = {"Content-Type": "application/json",
		'Authorization': 'Splunk ' + self.token
		}

        self.payloads = ""

    def log_event(self, payload):
        #print('Request payload: ' + json.dumps(payload))
        self.payloads=json.dumps(payload)

    def flush_async(self, callback=None):
        callback = callback or (lambda: None)
        try:

            #print('Sending event(s)')

            data = (self.payloads)
            data = data.encode('utf-8')
            req = urllib.request.urlopen(urllib.request.Request(''+self.url,data,self.requestOptions))
            res = req.getcode()
            #print('Response code is: ' + str(res))

            # if res != 200:
            #     error = ValueError('Error: status code = %d\n\n%s' % (res, req.read()))
            #     print(error)
            # else:
            #     self.payloads = []
        except Exception as e:
            print('Failed while trying to send logs to Splunk for '+self.url+' and token : '+self.token)
            if(error_flag<=5):
                time.sleep(10)
                self.flush_async()
                error_flag+=1
            else:
                print(self.payloads)
                print('Failed After retries to Splunk, Message tried to push is '+str(self.payloads))
            
