import boto3
import zipfile
import io
import os

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    zip_key = event['Records'][0]['s3']['object']['key']

    zip_obj = s3_client.get_object(Bucket=bucket_name, Key=zip_key)
    buffer = io.BytesIO(zip_obj["Body"].read())

    with zipfile.ZipFile(buffer, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            file_data = zip_ref.read(file_name)
            s3_client.put_object(Bucket=bucket_name, Key=f'extracted/{file_name}', Body=file_data)
            print(f'Successfully uploaded {file_name} to s3://{bucket_name}/extracted/{file_name}')

    return {
        'statusCode': 200,
        'body': 'Success'
    }
