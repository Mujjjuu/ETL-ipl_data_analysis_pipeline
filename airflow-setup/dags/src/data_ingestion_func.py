import requests
import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import json


def download_and_upload_to_s3(**kwargs):
    url = "https://cricsheet.org/downloads/ipl_json.zip"
    bucket_name = 'ipl-data-analysis-zipfile'

    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    s3_key = f'ipl_matches_{timestamp}.zip'
    
    s3_hook = S3Hook(aws_conn_id='aws-default')
    s3_client = s3_hook.get_conn()

    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            s3_client.upload_fileobj(response.raw, bucket_name, s3_key)
        logging.info(f"Streamed file from {url} to s3://{bucket_name}/{s3_key}")
        kwargs['ti'].xcom_push(key='s3_key', value=s3_key)
    except Exception as e:
        logging.error(f"Failed to stream file to S3: {e}")
        raise

def prepare_payload(**kwargs):
    s3_key = kwargs['ti'].xcom_pull(task_ids='download_and_upload_to_s3', key='s3_key')
    if s3_key is None:
        raise ValueError("No S3 key found in XCom")
    
    payload = {
        's3_bucket': 'ipl-data-analysis-zipfile', 
        's3_key': s3_key
    }
    logging.info(f"Prepared payload: {payload}")
    # Push the payload to XCom
    kwargs['ti'].xcom_push(key='payload', value=json.dumps(payload))
    return json.dumps(payload)