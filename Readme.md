Project Overview
The goal of this project is to build a data pipeline using Apache Airflow and AWS to download, extract, transform, and store data into an AWS database. The pipeline will upload downloaded zip files into an S3 bucket, transform the data, and store it in a database for further use in data analysis or dashboarding.

Key Components
Apache Airflow
AWS S3
AWS RDS (Relational Database Service)
AWS Lambda (optional for transformation)
AWS Glue (optional for ETL)
Docker (for containerization)
GitHub (for version control)
Steps
1. Setup and Configuration
Install and configure Airflow:

Install Airflow using Docker.
Set up Airflow with necessary connections (AWS credentials).
Set up AWS environment:

Create an S3 bucket for storing zip files.
Set up an RDS instance (MySQL/PostgreSQL) for the database.
Create GitHub repository:

Initialize a GitHub repository to manage your project files and code.
2. Data Ingestion
Download the data:

Create an Airflow DAG that downloads the zip file from the specified URL.
Upload to S3:

Create a task within the DAG to upload the downloaded zip file to the S3 bucket.
3. Data Extraction
Unzip the files:
Create an Airflow task to unzip the files stored in the S3 bucket.
4. Data Transformation
Extract JSON data:

Create a task to extract data from 1000 JSON files (each containing data of one match) from the unzipped files.
Transform the data:

Use AWS Glue or Lambda functions to transform the JSON data into a suitable format for the database.
5. Load Data into Database
Create database schema:

Design the schema for your database in RDS.
Load data into the database:

Create an Airflow task to load the transformed data into the RDS instance.
6. End-user Applications
Dashboarding and Analysis:
Set up tools like Tableau or Power BI to connect to the RDS instance for data visualization and analysis.

1) Connect to WSL(Ubuntu), 
in the terminal, 

sudo apt update

2) Activate the virtual Environment
source myenv/bin/activate

3) 