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
from src.data_ingestion_func import download_and_upload_to_s3, prepare_payload 
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator


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
    'crawler_dag',
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

def identify_new_files(**kwargs):
    aws_credentials = get_aws_credentials('##########')
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_credentials['aws_access_key_id'],
        aws_secret_access_key=aws_credentials['aws_secret_access_key'],
        region_name=aws_credentials['region_name']
    )
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=aws_credentials['aws_access_key_id'],
        aws_secret_access_key=aws_credentials['aws_secret_access_key'],
        region_name=aws_credentials['region_name']
    )
    table = dynamodb.Table('ProcessedFiles') 
    bucket_name = '##############'
    prefix = 's3:####################'
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    new_files = []
    for content in response.get('Contents', []):
        file_key = content['Key']
        base_file_key = file_key.rsplit('.', 1)[0]  
        response = table.get_item(Key={'file_key': base_file_key})  
        if 'Item' in response:
            item = response['Item']
            if item.get('ingested', False) and not item.get('crawled', False) and not item.get('transformed', False) and not item.get('loaded', False):
                new_files.append(file_key)
        else:
            logging.info(f"No DynamoDB entry for {base_file_key}")
    return new_files


def update_crawled_status(**context):
    aws_credentials = get_aws_credentials('############')
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=aws_credentials['aws_access_key_id'],
        aws_secret_access_key=aws_credentials['aws_secret_access_key'],
        region_name=aws_credentials['region_name']
    )
    table = dynamodb.Table('ProcessedFiles')
    new_files = context['ti'].xcom_pull(task_ids='identify_new_files')
    if not new_files:
        logging.info("No new files to process.")
        return
    for key in new_files:
        base_file_key = key.rsplit('.', 1)[0] 
        try:
            response = table.update_item(
                Key={'file_key': base_file_key},
                UpdateExpression="SET crawled = :val1",
                ExpressionAttributeValues={':val1': True}
            )
            logging.info(f"Successfully updated {base_file_key} as crawled in DynamoDB.")
        except Exception as e:
            logging.error(f"Error updating {base_file_key} in DynamoDB: {str(e)}")

    logging.info("Finished updating DynamoDB for crawled status.")


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
    aws_conn_id='#############',  
    region_name='us-east-2', 
    dag=dag,
)
identify_new_files_task = PythonOperator(
    task_id='identify_new_files',
    python_callable=identify_new_files,
    provide_context=True,
    dag=dag
)

update_keys_task = PythonOperator(
    task_id='update_keys_in_dynamodb',
    python_callable=update_crawled_status,
    provide_context=True,
    dag=dag
)
def start_glue_crawler_for_new_files(**context):
    try:
        new_files = context['ti'].xcom_pull(task_ids='identify_new_files')
        if not new_files:
            logging.info("No new files to crawl.")
            return

        new_files_prefixes = set([file.rsplit('/', 1)[0] for file in new_files])

        aws_credentials = get_aws_credentials('###############')
        glue_client = boto3.client(
            'glue',
            aws_access_key_id=aws_credentials['aws_access_key_id'],
            aws_secret_access_key=aws_credentials['aws_secret_access_key'],
            region_name=aws_credentials['region_name']
        )
        for prefix in new_files_prefixes:
            logging.info(f"Starting crawler for prefix: {prefix}")
            response = glue_client.start_crawler(
                Name="$#########",
                Targets={"S3Targets": [{"Path": f"s3://{prefix}/"}]}
            )
            logging.info(f"Started crawler for prefix: {prefix} with response: {response}")
    except Exception as e:
        logging.error(f"Failed to start Glue Crawler: {str(e)}")
        raise

start_glue_crawler_task = PythonOperator(
    task_id='start_glue_crawler_for_new_files',
    python_callable=start_glue_crawler_for_new_files,
    provide_context=True,
    dag=dag,
)
wait_for_crawler = GlueCrawlerSensor(
    task_id='wait_for_crawler',
    crawler_name='###############',
    poke_interval=60,
    timeout=600,
    aws_conn_id='aws-default',
    dag=dag,
)

compare_schema = GlueJobOperator(
    task_id='compare_schema',
    job_name='schema_comparision',
    script_location='#################################',
    s3_bucket='##################',
    region_name='us-east-2',
    script_args={
        '--CATALOG_ID': '################',
        '--DB_NAME': '#%%%%%%%%%%%%%%',
        '--TABLE_NAME': '#################',
        '--TOPIC_ARN': '##############################',
        '--DELETE_OLD_VERSIONS': 'true',
        '--NUM_VERSIONS_TO_RETAIN': '5'
    },
    aws_conn_id='#############',
    dag=dag,
)

wait_for_compare_schema = GlueJobSensor(
    task_id='wait_for_compare_schema',
    job_name='schema_comparision',
    run_id="{{ task_instance.xcom_pull(task_ids='compare_schema') }}",
    poke_interval=60,
    timeout=3600,
    aws_conn_id='aws-default',
    dag=dag,
)
def start_glue_job():
    aws_credentials = get_aws_credentials('aws-default')
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
                    '--scriptLocation': 's3://##################################',
                    '--TempDir': 's3:###########################'
                }
            )
            return response['JobRunId']
        except client.exceptions.ConcurrentRunsExceededException:
            wait_time = 2 ** attempt
            logging.info(f"Concurrent runs exceeded, retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    raise Exception("Failed to start Glue job after multiple attempts due to ConcurrentRunsExceededException.")

def start_transform_job():
    aws_credentials = get_aws_credentials('a##################')
    client = boto3.client(
        'glue',
        aws_access_key_id=aws_credentials['aws_access_key_id'],
        aws_secret_access_key=aws_credentials['aws_secret_access_key'],
        region_name=aws_credentials['region_name']
    )
    job_name = 'transformation_job'
    
    for attempt in range(5):
        try:
            response = client.start_job_run(
                JobName=job_name,
                Arguments={
                    '--scriptLocation': '################################',
                    '--TempDir': '##############################'
                }
            )
            job_run_id = response['JobRunId']
            logging.info(f"Started Glue job {job_name} with JobRunId: {job_run_id}")
            return job_run_id
        except client.exceptions.ConcurrentRunsExceededException:
            wait_time = 2 ** attempt
            logging.info(f"Concurrent runs exceeded, retrying in {wait_time} seconds...")
            time.sleep(wait_time)
        except Exception as e:
            logging.error(f"Failed to start Glue job: {str(e)}")
            raise

    raise Exception("Failed to start Glue job after multiple attempts due to ConcurrentRunsExceededException.")


def update_dynamodb_transformed_status(**context):
    new_files = context['ti'].xcom_pull(key='new_files', task_ids='identify_new_files')
    if not new_files:
        logging.info("No new files to update for transformed status.")
        return  
    dynamodb = boto3.resource('dynamodb', region_name='##################')
    table = dynamodb.Table('####################')
    with table.batch_writer() as batch:
        for file in new_files:
            base_file_key = file.rsplit('.', 1)[0]  
            batch.update_item(
                Key={'file_key': base_file_key},
                UpdateExpression="SET transformed = :val1",
                ExpressionAttributeValues={':val1': True}
            )
        logging.info(f"Updated DynamoDB with transformed status for files: {new_files}")

update_transformed_task = PythonOperator(
    task_id='update_transformed_status',
    python_callable=update_dynamodb_transformed_status,
    provide_context=True,
    dag=dag,
)

def update_dynamodb_loaded_status(**context):

    new_files = context['ti'].xcom_pull(key='new_files', task_ids='identify_new_files')
    

    if not new_files:
        logging.info("No new files to update for transformed status.")
        return 
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    table = dynamodb.Table('ProcessedFiles')

    with table.batch_writer() as batch:
        for file in new_files:
            base_file_key = file.rsplit('.', 1)[0] 
            batch.update_item(
                Key={'file_key': base_file_key},
                UpdateExpression="SET loaded = :val1",
                ExpressionAttributeValues={':val1': True}
            )
        logging.info(f"Updated DynamoDB with loaded status for files: {new_files}")

start_transform_job_task = PythonOperator(
    task_id='start_transform_job',
    python_callable=start_transform_job,
    dag=dag,
)

wait_for_transform_glue_job = GlueJobSensor(
    task_id='wait_for_transform_job',
    job_name='transformation_job',  
    run_id="{{ task_instance.xcom_pull(task_ids='start_transform_job') }}",
    poke_interval=60,
    timeout=3600,
    aws_conn_id='aws-default',
    dag=dag,
)


start_etl_glue_job_task = PythonOperator(
    task_id='start_glue_job',
    python_callable=start_glue_job,
    dag=dag,
)

wait_for_etl_glue_job = GlueJobSensor(
    task_id='wait_for_glue_job',
    job_name='etl_s3_to_redshift', 
    run_id="{{ task_instance.xcom_pull(task_ids='start_glue_job') }}",
    poke_interval=60,
    timeout=3600,
    aws_conn_id='#####################33',
    dag=dag,
)
update_loaded_task = PythonOperator(
    task_id='update_loaded_status',
    python_callable=update_dynamodb_loaded_status,
    provide_context=True,
    dag=dag,
)

download_and_upload_to_s3_task >>prepare_payload_task>> invoke_lambda_task>>identify_new_files_task >> start_glue_crawler_task >> wait_for_crawler >> update_keys_task >> compare_schema >> wait_for_compare_schema >> start_transform_job_task>>wait_for_transform_glue_job>>update_transformed_task>>start_etl_glue_job_task>>wait_for_etl_glue_job>>update_loaded_task
