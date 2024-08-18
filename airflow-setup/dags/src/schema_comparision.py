import boto3
import logging

def fetch_glue_schema(database_name, table_name):
    glue = boto3.client('glue')
    response = glue.get_table(DatabaseName=database_name, Name=table_name)
    return response['Table']

def compare_schema(**kwargs):
    new_files = kwargs['ti'].xcom_pull(task_ids='identify_new_files')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('ProcessedFiles')
    
    new_schema = fetch_glue_schema('jsondatacat_temp', 'jsondatacattemp')
    original_schema = fetch_glue_schema('ipldatabasejson', 'jsonipl_json_file')
    
    schema_changed = new_schema != original_schema
    
    for key in new_files:
        table.update_item(
            Key={'file_key': key},
            UpdateExpression="SET crawled = :c",
            ExpressionAttributeValues={':c': True}
        )
    
    if schema_changed:
        return 'send_schema_change_email'
    else:
        return 'etl_pipeline'



def start_glue_crawler(new_files):
    client = boto3.client('glue')
    s3_targets = [{"Path": f"s3://your-json-bucket/{file}"} for file in new_files]
    
    crawler_name = 'IplJsonCrawler'
    try:
        response = client.update_crawler(
            Name=crawler_name,
            Targets={'S3Targets': s3_targets}
        )
        logging.info(f"Crawler updated with new targets: {response}")
        
        response = client.start_crawler(Name=crawler_name)
        logging.info(f"Crawler started: {response}")
    except client.exceptions.CrawlerRunningException:
        logging.info("Crawler is already running.")
