from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from src.data_ingestion_func import download_and_upload_to_s3, prepare_payload 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['your_email@gmail.com']
}

dag = DAG(
    'initial_data_processing',
    default_args=default_args,
    description='Download zip files, upload to S3, and process using Lambda',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

download_and_upload_to_s3_task = PythonOperator(
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
    function_name='processNewZipFile',  
    payload="{{ task_instance.xcom_pull(task_ids='prepare_payload') }}",
    aws_conn_id='aws-default',  
    region_name='us-east-2', 
    dag=dag,
)

download_and_upload_to_s3_task >>prepare_payload_task>> invoke_lambda_task 
