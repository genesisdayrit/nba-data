from pyspark.sql import SparkSession
import boto3

# Create a Spark Session
spark = SparkSession.builder.appName("AWSGlueDataCatalogListTables").getOrCreate()

# Specify your database name
database_name = "nba-data"

# Create a session using a specific profile
session = boto3.Session(profile_name='myprofile')

# Create a client for the AWS Glue service using the session
glue_client = session.client('glue')

# Get list of tables
tables = glue_client.get_tables(DatabaseName=database_name)

# Print all table names
for table in tables['TableList']:
    print(table['Name'])
