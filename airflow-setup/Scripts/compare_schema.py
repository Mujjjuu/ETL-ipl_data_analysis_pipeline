import sys
import boto3
import pandas as pd
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'CATALOG_ID', 'DB_NAME', 'TABLE_NAME', 'TOPIC_ARN', 'DELETE_OLD_VERSIONS', 'NUM_VERSIONS_TO_RETAIN'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Parameters
catalog_id = args['590183781257']
db_name = args['ipldatabasejson']
table_name = args['jsonipl_json_file']
topic_arn = args['arn:aws:sns:us-east-2:590183781257:Schema-change']
delete_old_versions = args['DELETE_OLD_VERSIONS'].lower() == 'true'
number_of_versions_to_retain = 5

columns_modified = []

# Function to compare the name and type of columns in new column list with old column list to find any newly added column and the columns with changed data type
def findAddedUpdated(new_cols_df, old_cols_df, old_col_name_list):
    for index, row in new_cols_df.iterrows():
        new_col_name = new_cols_df.iloc[index]['Name']
        new_col_type = new_cols_df.iloc[index]['Type']

        # Check if a column with same name exist in old table but the data type has chaged
        if new_col_name in old_col_name_list:
            old_col_idx = old_cols_df.index[old_cols_df['Name'] == new_col_name][0]
            old_col_type = old_cols_df.iloc[old_col_idx]['Type']

            if old_col_type != new_col_type:
                columns_modified.append(f"Data type changed for '{new_col_name}' from '{old_col_type}' to '{new_col_type}'")
        # If a column is only in new column list, it a newly added column
        else:
            columns_modified.append(f"Added new column '{new_col_name}' with data type as '{new_col_type}'")

# Function to iterate through the list of old columns and check if any column doesn't exist in new columns list to find out dropped columns
def findDropped(old_cols_df, new_col_name_list):
    for index, row in old_cols_df.iterrows():
        old_col_name = old_cols_df.iloc[index]['Name']
        old_col_type = old_cols_df.iloc[index]['Type']

        # Check if column doesn't exist in new column list  
        if old_col_name not in new_col_name_list:
            columns_modified.append(f"Dropped old column '{old_col_name}' with data type as '{old_col_type}'")

# Function to publish changes in schema to a SNS topic that can be subscribed to receive email notifications when changes are detected
def notifyChanges(message_to_send):
    sns = boto3.client('sns')
    # Publish a simple message to the specified SNS topic
    response = sns.publish(
        TopicArn=topic_arn,   
        Message=message_to_send,  
        Subject="DWH Notification: Changes in table schema"
    )
    
# Function to convert version_id to int to use for sorting the versions
def version_id(json):
    try:
        return int(json['VersionId'])
    except KeyError:
        return 0

# Function to delete the table versions
def delele_versions(glue_client, versions_list, number_of_versions_to_retain):
    print("deleting old versions...")
    if len(versions_list) > number_of_versions_to_retain:
        version_id_list = []
        for table_version in versions_list:
            version_id_list.append(int(table_version['VersionId']))
        # Sort the versions in descending order
        version_id_list.sort(reverse=True)
        versions_str_list = [str(x) for x in version_id_list]
        versions_to_delete = versions_str_list[number_of_versions_to_retain:]
        
        del_response = glue_client.batch_delete_table_version(
            DatabaseName=db_name,
            TableName=table_name,
            VersionIds=versions_to_delete
        )
        return del_response

# Calling glue API to get the list of table versions. The solution assumes that number of version in the table are less than 100. If you have more than 100 versions, you should use pagination and loop through each page.  
glue = boto3.client('glue')
response = glue.get_table_versions(
    CatalogId=catalog_id,
    DatabaseName=db_name,
    TableName=table_name,
    MaxResults=100
)
table_versions = response['TableVersions']
table_versions.sort(key=version_id, reverse=True)

version_count = len(table_versions)
print(version_count)

# Checking if the version of table to compare exists. You would need to pass the numbers of versions to compare to the job. 
if version_count > 1:
    new_columns = table_versions[0]['Table']['StorageDescriptor']['Columns']
    new_cols_df = pd.DataFrame(new_columns)

    old_columns = table_versions[1]['Table']['StorageDescriptor']['Columns']
    old_cols_df = pd.DataFrame(old_columns)

    new_col_name_list =  new_cols_df['Name'].tolist()
    old_col_name_list =  old_cols_df['Name'].tolist()
    findAddedUpdated(new_cols_df, old_cols_df, old_col_name_list)
    findDropped(old_cols_df, new_col_name_list)
    if len(columns_modified) > 0: 
        email_msg = f"Following changes are identified in '{table_name}' table of '{db_name}' database of your Datawarehouse. Please review.\n\n"
        print("Job completed! -- below is list of changes.")
        for column_modified in columns_modified:
            email_msg += f"\t{column_modified}\n"

        print(email_msg)
        notifyChanges(email_msg)
    else:
        if delete_old_versions:
            delele_versions(glue, table_versions, number_of_versions_to_retain)
        print("Job completed! -- There are no changes in table schema.")
else:
    print("Job completed! -- Selected table doesn't have the number of versions selected to compare. Please verify the list passed in 'versions_to_compare'")

job.commit()
