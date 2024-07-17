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

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['your_email@example.com']
}

# Define the DAG
dag = DAG(
    'glue_crawler_etl_pipeline',
    default_args=default_args,
    description='Trigger AWS Glue Crawler to update the data catalog and run Glue ETL job',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

# Task to start Glue Crawler
start_glue_crawler = GlueCrawlerOperator(
    task_id='start_glue_crawler',
    config={
        "Name": "IplJsonCrawler",
        "Role": "arn:aws:iam::590183781257:role/service-role/AWSGlueServiceRole-crawler",
        "DatabaseName": "ipldatabasejson",
        "Targets": {"S3Targets": [{"Path": "s3://ipl-json-file/"}]},
    },
    aws_conn_id='aws_default',  # Ensure you have this connection in Airflow
    region_name='us-east-2',
    dag=dag,
)

# Sensor to wait for Glue Crawler completion
wait_for_crawler = GlueCrawlerSensor(
    task_id='wait_for_crawler',
    crawler_name='IplJsonCrawler',
    poke_interval=60,
    timeout=600,
    aws_conn_id='aws_default',
    region_name='us-east-2',
    dag=dag,
)

# Task to start Glue Job with retry mechanism
def start_glue_job():
    client = boto3.client('glue', region_name='us-east-2')
    job_name = 'etl_s3_to_redshift'
    
    for attempt in range(5):
        try:
            response = client.start_job_run(JobName=job_name)
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

# Sensor to wait for Glue Job completion
wait_for_glue_job = GlueJobSensor(
    task_id='wait_for_glue_job',
    job_name='etl_s3_to_redshift',  # Ensure this matches your Glue job name
    run_id="{{ task_instance.xcom_pull(task_ids='start_glue_job') }}",
    poke_interval=60,
    timeout=3600,
    aws_conn_id='aws_default',
    region_name='us-east-2',
    dag=dag,
)

# Define the task dependencies
start_glue_crawler >> wait_for_crawler >> start_glue_job_task >> wait_for_glue_job
