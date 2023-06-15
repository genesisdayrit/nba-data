import boto3

# Create a session using a specific profile
session = boto3.Session(profile_name='myprofile')

# Create a Glue client
glue_client = session.client('glue')

# Create an S3 client
s3_client = session.client('s3')

# Write your script to a file
script = """
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import boto3

def convert_glue_type_to_redshift(glue_type):
    mapping = {
        'bigint': 'BIGINT',
        'int': 'INTEGER',
        'smallint': 'SMALLINT',
        'tinyint': 'SMALLINT',
        'double': 'DOUBLE PRECISION',
        'float': 'REAL',
        'decimal': 'DECIMAL',
        'string': 'VARCHAR(256)',
        'char': 'CHAR',
        'varchar': 'VARCHAR(256)',
        'boolean': 'BOOLEAN',
        'timestamp': 'TIMESTAMP',
        'date': 'DATE',
        'binary': 'BYTEA',
        'array': None,
        'map': None,
        'struct': None,
    }
    return mapping.get(glue_type, 'VARCHAR(256)')

def create_table_statement(schema, table_name, database_name):
    base_table_name = table_name[:-20]
    if database_name == 'cercle-graph-nodes':
        redshift_schema = 'nodes'
    elif database_name == 'cercle-graph-relationships':
        redshift_schema = 'relationships'
    else:
        redshift_schema = 'public'

    create_table_query = f'DROP TABLE IF EXISTS {redshift_schema}.{base_table_name}; CREATE TABLE IF NOT EXISTS {redshift_schema}.{base_table_name} ('
    for column in schema:
        column_name = column['Name']
        redshift_type = convert_glue_type_to_redshift(column['Type'])
        if redshift_type is not None:
            create_table_query += f'{column_name} {redshift_type}, '
    create_table_query = create_table_query[:-2] + ');'
    return create_table_query

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

glueClient = boto3.client('glue')

# Replace with your database and table names
database_name = 'your_database_name'
table_name = 'your_table_name'

table_response = glueClient.get_table(DatabaseName=database_name, Name=table_name)
table = table_response['Table']
schema = table['StorageDescriptor']['Columns']
base_table_name = table_name[:-20]

if database_name == 'cercle-graph-nodes':
    redshift_schema = 'nodes'
elif database_name == 'cercle-graph-relationships':
    redshift_schema = 'relationships'
else:
    redshift_schema = 'public'

create_table_query = create_table_statement(schema, table_name, database_name)

dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="cercle-graph-nodes",
    table_name="artcycle_2023_06_03_00_00_03",
    transformation_ctx="dynamic_frame",
)

glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-376179242148-us-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": redshift_schema + '.' + base_table_name,
        "connectionName": "redshift-connection",
        "preactions": create_table_query,
        "aws_iam_user": "arn:aws:iam::376179242148:role/service-role/AWSGlueServiceRole",
    },
    transformation_ctx="redshift_node",
)

job.commit()
"""

with open("script.py", "w") as f:
    f.write(script)

# Upload the script to S3
bucket_name = 'your_bucket_name'
s3_client.upload_file('script.py', bucket_name, 'script.py')

# Specify the parameters for the job
job_name = 'example-job'
script_location = f's3://{bucket_name}/script.py'

response = glue_client.create_job(
    Name=job_name,
    Role='AWSGlueServiceRole',  # the IAM role with the necessary permissions
    Command={
        'Name': 'glueetl',
        'ScriptLocation': script_location,
        'PythonVersion': '3'
    },
    DefaultArguments={
        '--TempDir': 's3://path/to/temp/dir/',
        '--job-language': 'python',
        '--job-bookmark-option': 'job-bookmark-enable'
    },
    MaxRetries=0,
    Timeout=10,
    MaxCapacity=2.0
)

print(f"Job {job_name} created with response: {response}")

# # Start the job
# response = glue_client.start_job_run(JobName=job_name)
# print(f"Job run started with response: {response}")
