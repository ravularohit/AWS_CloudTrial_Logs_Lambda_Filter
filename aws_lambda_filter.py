import json
import urllib.parse
import boto3
import gzip
import io
import re

print('Loading function')

s3 = boto3.client('s3')
sns=boto3.client('sns')
sns_arn='{input you sns arn here}'


def send_sns_mail(msg):
    sns.publish(
    TopicArn=sns_arn,
    Message=json.dumps({'default': json.dumps(msg, indent=4, sort_keys=True, ensure_ascii=False, separators=(',', ': '))}),
    MessageStructure='json')


def filter_read_events(event):
    read_operations=["Get","Describe","List","Head"]
    res_list = [s for s in re.split("([A-Z][^A-Z]*)", event) if s]
    for j in read_operations:
        if(j in res_list):
            return True
    return False
                

def lambda_handler(event, context):
    

    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key) 
        content=response['Body'].read()
        with gzip.GzipFile(fileobj=io.BytesIO(content), mode='rb') as f:
            data = json.load(f)
            #print(data)
            output_dict=[x for x in data['Records'] if not filter_read_events(x['eventName']) ]
            #print(output_dict)
            #print(len(data['Records']),len(output_dict))
            if (len(output_dict)>0):
                for i in output_dict:
                    send_sns_mail(i)
        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
