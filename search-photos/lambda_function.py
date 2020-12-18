import json
import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3
from botocore.vendored import requests

    
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

headers = {"Content-Type": "application/json" }
host = 'https://search-photos-tu6pms5iqais62nn2yd2jofypy.us-east-1.es.amazonaws.com'
region = 'us-east-1'
lex = boto3.client('lex-runtime', region_name=region)

def lambda_handler(event, context):
    # TODO implement
    print ('event : ', event)
    query = event['queryStringParameters']
    print("Query", query)
    q1 = query['q']
    print(q1)
    
    # put result to s3 after transcribe
    if(q1 == "searchAudio" ):
        q1 = convert_speechtotext()
        s3client = boto3.client('s3')
        s3client.put_object(Body=q1, Bucket='sz.photo-strorage', Key='test.txt')
        
        return {
            'statusCode':200,
            'headers':{
                "Access-Control-Allow-Origin":"*",
                "Access-Control-Allow-Methods":"GET,OPTIONS",
                "Access-Control-Allow-Headers":"Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
                "Content-Type":"application/json"
            },
            'body': json.dumps("trans success, result logged")
        }
    
    # get previous transcribe result    
    if(q1 == "getAudio"):
        s3client = boto3.client('s3')
        data = s3client.get_object(Bucket='sz.photo-strorage', Key='test.txt')
        q1 = data.get('Body').read().decode('utf-8')
        print("Voice query: ", query)
        # s3client.delete_object(Bucket='sz.photo-strorage', Key='test.txt')
    
    
    print("q1:", q1 )
    labels = get_labels(q1)
    print("labels", labels)
    img_paths = []
    
    if len(labels) != 0:
        img_paths = get_photo_path(labels)

    body = {
        'imagePaths':img_paths,
        'userQuery':q1,
        'labels': labels,
    }
    
    return {
        'statusCode':200,
        'headers':{
            "Access-Control-Allow-Origin":"*",
            "Access-Control-Allow-Methods":"GET,OPTIONS",
            "Access-Control-Allow-Headers":"Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Content-Type":"application/json"
        },
        'body': json.dumps(body)
    }
    
    
def get_labels(query):
    response = lex.post_text(
        botName='PhotoBot',                 
        botAlias='album',
        userId="string",           
        inputText=query
    )
    print("lex-response", response)
    
    labels = []
    if 'slots' not in response:
        print("No photo collection for query {}".format(query))
    else:
        print ("slot: ",response['slots'])
        slot_val = response['slots']
        for key,value in slot_val.items():
            if value!=None:
                labels.append(value)
    return labels

def get_photo_path(labels):
    img_paths = []
    '''
    unique_labels = [] 
    for x in labels: 
        if x not in unique_labels: 
            unique_labels.append(x)
    labels = unique_labels
    '''
    print("inside get photo path", labels)
    for i in labels:
        path = host + '/_search?q=labels:'+i
        print(path)
        response = requests.get(path, headers=headers)
        print("response from ES", response)
        dict1 =  json.loads(response.text)
        hits_count = dict1['hits']['total']['value']
        print ("DICT : ", dict1)
        
        for k in range(0, hits_count):
            #img_obj = dict1["hits"]["hits"]
            img_bucket = dict1["hits"]["hits"][k]["_source"]["bucket"]
            # print("img_bucket", img_bucket)
            img_name = dict1["hits"]["hits"][k]["_source"]["objectKey"]
            # print("img_name", img_name)
            img_link = 'https://s3.amazonaws.com/' + str(img_bucket) + '/' + str(img_name)
            print (img_link)
            img_paths.append(img_link)
    
    # print (img_paths)
    return img_paths
  
    
def convert_speechtotext():
    # return "blossom"
    transcribe = boto3.client('transcribe')
   
    job_name = datetime.datetime.now().strftime("%m-%d-%y-%H-%M%S")
    job_uri = "https://s3.amazonaws.com/sz.photo-strorage/Recording.wav"
    storage_uri = "sz.photo-strorage"

    s3 = boto3.client('s3')
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': job_uri},
        MediaFormat='wav',
        LanguageCode='en-US',
        OutputBucketName=storage_uri
    )

    while True:
        status = transcribe.get_transcription_job(TranscriptionJobName=job_name)
        print('status:')
        print(status)
        if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            break
        print("Not ready yet...")   
        time.sleep(1)
    
   
    job_name = str(job_name) + '.json'
    print (job_name)
    obj = s3.get_object(Bucket="sz.photo-strorage", Key=job_name)
    print ("Object : ", obj)
    body = json.loads(obj['Body'].read().decode('utf-8'))
    print ("Body :", body)
   
    
    return body["results"]["transcripts"][0]["transcript"]