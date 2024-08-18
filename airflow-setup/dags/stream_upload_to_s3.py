from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import logging
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'upload_and_invoke_lambda',
    default_args=default_args,
    description='Download IPL zip file, upload to S3, and invoke Lambda to process it',
    schedule_interval=None,  
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def download_and_upload_to_s3(**kwargs):
    url = "https://cricsheet.org/downloads/ipl_json.zip" 
    bucket_name = 'ipl-data-analysis-zipfile'  
    s3_key = 'ipl_matches.zip'
    processed_files_key = 'processed_files.json'
    
    s3_hook = S3Hook(aws_conn_id='s3-bucket-conn') 
    s3_client = s3_hook.get_conn()

    processed_files = {}
    try:
        processed_files_obj = s3_client.get_object(Bucket=bucket_name, Key=processed_files_key)
        processed_files = json.loads(processed_files_obj['Body'].read().decode('utf-8'))
    except s3_client.exceptions.NoSuchKey:
        logging.info("Processed files list does not exist. Creating a new one.")

    if s3_key in processed_files:
        logging.info(f"{s3_key} is already processed. Skipping upload.")
        return

    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            s3_client.upload_fileobj(response.raw, bucket_name, s3_key)
        logging.info(f"Streamed file from {url} to s3://{bucket_name}/{s3_key}")

        processed_files[s3_key] = datetime.now().isoformat()
        s3_client.put_object(Bucket=bucket_name, Key=processed_files_key, Body=json.dumps(processed_files))

        kwargs['ti'].xcom_push(key='s3_key', value=s3_key)
    except Exception as e:
        logging.error(f"Failed to stream file to S3: {e}")
        raise

def prepare_payload(**kwargs):
    s3_key = kwargs['ti'].xcom_pull(task_ids='download_and_upload_to_s3', key='s3_key')
    payload = {'s3_key': s3_key}
    return json.dumps(payload)

upload_task = PythonOperator(
    task_id='download_and_upload_to_s3',
    python_callable=download_and_upload_to_s3,
    provide_context=True,
    dag=dag,
)


prepare_payload_task = PythonOperator(
    task_id='prepare_payload',
    python_callable=prepare_payload,
    provide_context=True,
    dag=dag,
)

invoke_lambda_task = LambdaInvokeFunctionOperator(
    task_id='invoke_lambda_unzip',
    function_name='airflow-lamda-unzip',  
    payload="{{ task_instance.xcom_pull(task_ids='prepare_payload') }}",
    aws_conn_id='s3-bucket-conn',  
    region_name='us-east-2', 
    dag=dag,
)

upload_task >> prepare_payload_task >> invoke_lambda_task
