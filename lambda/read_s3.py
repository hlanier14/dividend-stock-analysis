import json
import boto3
import os
from botocore.exceptions import ClientError

s3 = boto3.client('s3')

def lambda_handler(event, context):

    try:

        bucket_name = os.environ['BUCKET_NAME']
        object_key = os.environ['OBJECT_KEY']

        s3.head_object(Bucket=bucket_name, Key=object_key)

        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        json_content = json.loads(response['Body'].read())

        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Headers" : "Content-Type",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET"
            },
            'body': json.dumps(json_content)
        }
    
    except ClientError as e:

        if e.response['Error']['Code'] == '404':

            return {
                'statusCode': 404,
                'headers': {
                    "Access-Control-Allow-Headers" : "Content-Type",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET"
                },
                'body': 'No data'
            }
        
        else:

            return {
                'statusCode': 500,
                'headers': {
                    "Access-Control-Allow-Headers" : "Content-Type",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET"
                },
                'body': f'Error: {str(e)}'
            }
