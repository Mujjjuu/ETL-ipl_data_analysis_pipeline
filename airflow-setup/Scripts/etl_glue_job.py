import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Initialize Glue context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Redshift connection details
redshift_temp_dir = "s3://ipl-glue-temp/"
redshift_connection_options = {
    "url": "jdbc:redshift://ipl-data-analysis-cluster.cpxvlk4tukt8.us-east-2.redshift.amazonaws.com:5439/ipldatawarehouse",
    "user": "susheelredshift",
    "password": "Susheelredshift1",
    "dbtable": "ipl_matches",
    "redshiftTmpDir": redshift_temp_dir
}

# Read JSON data from Glue Catalog
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="ipldatabasejson", 
    table_name="jsonipl_json_file"
)

# Flatten the DynamicFrame
flattened_df = datasource.toDF()
flattened_dynamic_frame = DynamicFrame.fromDF(flattened_df, glueContext, "flattened_dynamic_frame")

# Write the data to Redshift
glueContext.write_dynamic_frame.from_options(
    frame=flattened_dynamic_frame,
    connection_type="redshift",
    connection_options=redshift_connection_options,
    transformation_ctx="redshift_write"
)

job.commit()
