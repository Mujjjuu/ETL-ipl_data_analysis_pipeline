from airflow import DAG
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import boto3
import logging
import time
from airflow.hooks.base import BaseHook
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['your_email@example.com']
}

dag = DAG(
    'glue_crawler_etl_pipeline',
    default_args=default_args,
    description='Trigger AWS Glue Crawler to update the data catalog and run Glue ETL job',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

def get_aws_credentials(aws_conn_id):
    connection = BaseHook.get_connection(aws_conn_id)
    return {
        'aws_access_key_id': connection.login,
        'aws_secret_access_key': connection.password,
        'region_name': connection.extra_dejson.get('region_name', 'us-east-2')
    }

start_glue_crawler = GlueCrawlerOperator(
    task_id='start_glue_crawler',
    config={
        "Name": "IplJsonCrawler",
        "Role": "arn:aws:iam::590183781257:role/service-role/AWSGlueServiceRole-crawler",
        "DatabaseName": "ipldatabasejson",
        "Targets": {"S3Targets": [{"Path": "s3://ipl-json-file/"}]},
    },
    aws_conn_id='s3-bucket-conn', 
    region_name='us-east-2',
    dag=dag,
)

wait_for_crawler = GlueCrawlerSensor(
    task_id='wait_for_crawler',
    crawler_name='IplJsonCrawler',
    poke_interval=60,
    timeout=600,
    aws_conn_id='s3-bucket-conn',
    dag=dag,
)

def start_glue_job():
    aws_credentials = get_aws_credentials('s3-bucket-conn')
    client = boto3.client(
        'glue',
        aws_access_key_id=aws_credentials['aws_access_key_id'],
        aws_secret_access_key=aws_credentials['aws_secret_access_key'],
        region_name=aws_credentials['region_name']
    )
    job_name = 'etl_s3_to_redshift'
    
    for attempt in range(5):
        try:
            response = client.start_job_run(
                JobName=job_name,
                Arguments={
                    '--scriptLocation': 's3://aws-glue-assets-590183781257-us-east-2/scripts/etl_s3_to_redshift.py',
                    '--TempDir': 's3://ipl-glue-temp/'
                }
            )
            return response['JobRunId']
        except client.exceptions.ConcurrentRunsExceededException:
            wait_time = 2 ** attempt
            logging.info(f"Concurrent runs exceeded, retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    raise Exception("Failed to start Glue job after multiple attempts due to ConcurrentRunsExceededException.")

start_glue_job_task = PythonOperator(
    task_id='start_glue_job',
    python_callable=start_glue_job,
    dag=dag,
)

wait_for_glue_job = GlueJobSensor(
    task_id='wait_for_glue_job',
    job_name='etl_s3_to_redshift', 
    run_id="{{ task_instance.xcom_pull(task_ids='start_glue_job') }}",
    poke_interval=60,
    timeout=3600,
    aws_conn_id='s3-bucket-conn',
    dag=dag,
)

start_glue_crawler >> wait_for_crawler >> start_glue_job_task >> wait_for_glue_job
