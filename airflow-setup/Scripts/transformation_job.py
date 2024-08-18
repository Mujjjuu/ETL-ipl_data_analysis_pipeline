import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, ArrayType

# Initialize Glue context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Function to iteratively flatten nested JSON structures
def flatten_df(df):
    while True:
        # Identify all columns with nested structures
        nested_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, (StructType, ArrayType))]
        if not nested_columns:
            break# Process each nested column
    for nested_col in nested_columns:
        if isinstance(df.schema[nested_col].dataType, StructType):
                    # Expand StructType fields
            expanded = [col(f"{nested_col}.{sub_field.name}").alias(f"{nested_col}_{sub_field.name}") for sub_field in df.schema[nested_col].dataType.fields]
            df = df.select("*", *expanded).drop(nested_col)
        elif isinstance(df.schema[nested_col].dataType, ArrayType):
                    # Explode ArrayType fields
                    df = df.withColumn(nested_col, explode(col(nested_col)))

    return df

# Specify the S3 path where your JSON files are located
s3_path = "s3://your-s3-bucket/path-to-your-json-files/"# Read JSON data directly from S3
df = spark.read.json(s3_path)

# Flatten the dataframe
flattened_df = flatten_df(df)

if flattened_df.head(1):
    # Write the flattened data to the temporary S3 bucket in Parquet format
    flattened_df.write.mode('overwrite').parquet('s3://ipl-staging-bucket/flattend-data/')
else:
    print("No data found after flattening. Please check the input data or flattening logic.")

# Commit the job
job.commit()
