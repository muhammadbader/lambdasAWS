import json
import boto3
from urllib import parse as prs

# client s3
s3 = boto3.client('s3')


def lambda_handler(event, context):
    # get the bucket name
    # print(event)
    bucket = event['Records'][0]['s3']['bucket']['name']

    # get the file /key from S3

    key = prs.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    try:
        # fetch the file from S3
        response = s3.get_object(Bucket=bucket, Key=key)

        # deserialize the file's cpntent
        text = response["Body"].read()
        data = json.loads(text)
        j = 0
        while j < len(data):
            if len(data[j]['phone']) != 10:
                data.pop(j)
            else:
                j += 1

        return {
            'statusCode': 200,
            'body': json.dumps(data)
        }

    except Exception as e:
        print(e)
        raise e