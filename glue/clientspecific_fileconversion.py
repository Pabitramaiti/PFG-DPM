import sys
from awsglue.utils import getResolvedOptions
import json
import boto3

Lambda = boto3.client('lambda')

## @params: [JOB_NAME]


def client_specific_file_conversion():
    try:
        # args = getResolvedOptions(sys.argv, ['objectKey', 'lambdaName'])
        args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
        print(str(args))
        fileName = args['key'].split('/')[-1]
        
        response = Lambda.invoke(FunctionName = args['lambdaName'], InvocationType = 'RequestResponse', Payload = json.dumps({"filename": fileName}))
        print(response)
        if response['StatusCode'] == 200:
            return response['StatusCode']
        else:
            raise ValueError(f"Error invoking lambda function {args['lambdaName']}")
    except Exception as e:
        raise e

if __name__ == '__main__':
    client_specific_file_conversion()