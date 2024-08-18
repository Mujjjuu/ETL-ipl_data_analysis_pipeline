import boto3
import zipfile
import io
import logging

def lambda_handler(event, context):
    try:
        # Initialize clients
        s3 = boto3.client('s3')
        dynamodb = boto3.client('dynamodb')

        # Log the received event for debugging
        logging.info(f"Received event: {event}")

        # Extract bucket name and key from the event
        bucket = event['s3_bucket']
        key = event['s3_key']

        # Get the uploaded zip file
        zip_obj = s3.get_object(Bucket=bucket, Key=key)
        buffer = io.BytesIO(zip_obj['Body'].read())

        # Open the zip file
        with zipfile.ZipFile(buffer, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                if file_name.endswith('.json'):
                # Read each file in the zip
                    file_data = zip_ref.read(file_name)
                    
                    # Check if the JSON file already exists in the JSON bucket
                    try:
                        s3.head_object(Bucket='ipl-json-file', Key=file_name)
                        logging.info(f"File {file_name} already exists in the JSON bucket, skipping upload.")
                    except s3.exceptions.ClientError:
                        # Upload new JSON file to the JSON bucket
                        s3.put_object(Bucket='ipl-json-file', Key=file_name, Body=file_data)
                        logging.info(f"Uploaded new file {file_name} to the JSON bucket.")
                        
                        # Update DynamoDB with processed file
                        dynamodb.update_item(
                            TableName='ProcessedFiles',
                            Key={'file_key': {'S': file_name}},
                            UpdateExpression="SET #processed = :p",
                            ExpressionAttributeNames={'#processed': 'processed'},
                            ExpressionAttributeValues={':p': {"BOOL": True}}
                    )
                        logging.info(f"Processed file {file_name} added to DynamoDB.")

        return {
            'statusCode': 200,
            'body': 'Processing completed'
        }

    except Exception as e:
        logging.error(f"Error processing event: {e}")
        raise e