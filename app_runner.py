# ! pip install boto3
import boto3
kinesis_client = boto3.client('kinesis', region_name='', aws_access_key_id='',
     aws_secret_access_key='')

import requests

import boto3
import requests
import json
import time
import pandas as pd

# Initialize Kinesis client


def send_to_kinesis(data):
    # Send data to Kinesis stream
    data_record = json.dumps(data)
    kinesis_client.put_record(StreamName='powerbi-stream', Data=data_record, PartitionKey='1')

def invoke_api():
    # API Gateway endpoint URL

    api_endpoint = '' # APP runner endpoint
    # Define the parameters
    params = {
        'year': '2010',
    }
    # Make a GET request to the API
    response = requests.get(api_endpoint, params=params)

    json_data = json.loads(json.loads(response.text))

    print(len(json_data))

    for i in range(0,len(json_data)):
        send_to_kinesis(json_data[i])

    #print(i)

invoke_api()
