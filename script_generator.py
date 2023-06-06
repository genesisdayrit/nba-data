import boto3
import time
import threading
import queue
from botocore.exceptions import ClientError

def convert_glue_type_to_redshift(glue_type):
    """Function to convert AWS Glue data type to Redshift data type. Returns None for complex types."""
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

def create_table_statement(schema, table):
    create_table_query = f'DROP TABLE IF EXISTS public.{table}; CREATE TABLE IF NOT EXISTS public.{table} ('
    for column in schema:
        column_name = column['Name']
        redshift_type = convert_glue_type_to_redshift(column['Type'])
        if redshift_type is not None:
            create_table_query += f'{column_name} {redshift_type}, '
    create_table_query = create_table_query[:-2] + ');'
    return create_table_query

# Create a session using a specific profile
session = boto3.Session(profile_name='myprofile')

# Create a client for the AWS Glue service
glue_client = session.client('glue')

# Get list of all databases
database_response = glue_client.get_databases()

for database in database_response['DatabaseList']:
    # For each database, get the list of tables
    table_response = glue_client.get_tables(DatabaseName=database['Name'])
    
    for table in table_response['TableList']:
        # For each table, get the schema
        schema = table['StorageDescriptor']['Columns']
        
        # Generate the "CREATE TABLE" statement
        create_table_query = create_table_statement(schema, table['Name'])

        # Generate the ETL job script
        script = f"""
        import sys
        from awsglue.transforms import *
        from awsglue.utils import getResolvedOptions
        from pyspark.context import SparkContext
        from awsglue.context import GlueContext
        from awsglue.job import Job

        args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args["JOB_NAME"], args)

        # Script generated for node S3 bucket
        S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
            database="{database['Name']}",
            table_name="{table['Name']}",
            transformation_ctx="S3bucket_node1",
        )

        # Script generated for node Amazon Redshift
        AmazonRedshift_node1686057833145 = glueContext.write_dynamic_frame.from_options(
            frame=S3bucket_node1,
            connection_type="redshift",
            connection_options={{"redshiftTmpDir": "s3://aws-glue-assets-758443678568-us-east-2/temporary/",
                "useConnectionProperties": "true",
                "dbtable": "public.{table['Name']}",
                "connectionName": "redshift-connection",
                "preactions": "{create_table_query}",
            }},
            transformation_ctx="AmazonRedshift_node1686057833145",
        )

        job.commit()
        """

        # Print the script (or save it to a file, upload it to S3, etc.)
        print(script)
