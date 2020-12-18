import json
import boto3
import os
import sys
import uuid
from botocore.vendored import requests
from datetime import *
import urllib3

http = urllib3.PoolManager()
          
REGION = 'us-east-1'
ES_ENDPOINT = 'https://search-photos-tu6pms5iqais62nn2yd2jofypy.us-east-1.es.amazonaws.com'
ES_INDEX = 'photos'
ES_TYPE = 'Photo'

def lambda_handler(event, context):
    print('INPUT EVENT DATA: {}'.format(event))

    for record in event['Records']:
        # if put something other than photo, do not rekog
        key = record['s3']['object']['key']
        parts = key.split('.')
        sufix = parts[-1]
        if sufix.lower() not in ['jpg','jpeg','png','bmp','gif']:
            print('None image: {}'.format(key))
            continue
        
        # if put photo, rekog
        index_item = {}
        reko_response = get_reko_response(record)
        
        if reko_response is not None:
            index_item['objectKey'] = record['s3']['object']['key']
            id = index_item['objectKey']
            index_item["bucket"] = record['s3']['bucket']['name']
            index_item["createdTimestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            index_item["labels"] = []
            labels = reko_response['Labels']

            for label in labels:
                index_item["labels"].append(label['Name'])
            
            index_item = json.dumps(index_item)
            print('idexitem:')
            print(index_item)
            url = ES_ENDPOINT + '/' + ES_INDEX + '/' + ES_TYPE + '/'
            req = http.request('POST', url + str(id), body = index_item, headers = { "Content-Type": "application/json" }, retries = False)
            print(json.loads(req.data.decode('utf-8')))
    
    return {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Origin": "*",
            'Content-Type': 'application/json'
        },
        'body': json.dumps("Upload success!")
    }


def get_reko_response(record):
    reko = boto3.client('rekognition')
    bucket_name = record['s3']['bucket']['name']
    object_key = record['s3']['object']['key']
    reko_response = reko.detect_labels(
        Image={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': object_key,
            },
        },
        MaxLabels=110,
        MinConfidence=70,
    ) 

    return reko_response