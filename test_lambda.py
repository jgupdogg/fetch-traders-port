# test_lambda.py

import os
import json
from main import lambda_handler

def test_lambda():
    # Load environment variables
    with open('.env') as f:
        for line in f:
            if line.strip() and not line.startswith('#'):
                key, value = line.strip().split('=', 1)
                os.environ[key] = value

    # Define a test event
    event = {
        "httpMethod": "POST",
        "body": json.dumps({"category": "ai"}),
        "isBase64Encoded": False,
        "headers": {
            "Content-Type": "application/json"
        },
        "path": "/fetch-trader-port",
        "requestContext": {
            "http": {
                "method": "POST",
                "path": "/fetch-trader-port",
                "identity": {
                    "sourceIp": "127.0.0.1",
                    "userAgent": "Custom User Agent"
                }
            }
        }
    }

    # Invoke the Lambda function
    response = lambda_handler(event, None)
    print("Lambda Response:")
    print(json.dumps(response, indent=4))

if __name__ == "__main__":
    test_lambda()
