import json

def lambda_handler(event, context):
    # Your Lambda function code here

    # For this example, we'll just create a response dictionary
    response = {
        "statusCode": 200,
        "body": json.dumps({"message": "Lambda function executed successfully"})
    }

    return response
