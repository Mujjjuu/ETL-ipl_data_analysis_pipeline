import boto3
import zipfile
import io
import json

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    source_bucket = 'ipl-data-analysis-zipfile'  # Replace with your source S3 bucket name
    zip_key = event['s3_key']
    target_bucket = 'ipl-json-file'  # Replace with your target S3 bucket name

    # Download the zip file from S3
    zip_obj = s3_client.get_object(Bucket=source_bucket, Key=zip_key)
    buffer = io.BytesIO(zip_obj["Body"].read())

    # Unzip the file and upload all JSON files to the target bucket
    with zipfile.ZipFile(buffer, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            file_obj = zip_ref.read(file_name)
            s3_client.put_object(Bucket=target_bucket, Key=file_name, Body=file_obj)
            print(f"Uploaded {file_name} to s3://{target_bucket}/{file_name}")

    return {
        'statusCode': 200,
        'body': json.dumps(f"Processed files from {zip_key}")
    }
