import boto3
import logging

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
