import boto3

def get_table_schema(database_name, table_name):
    # Create a session using a specific profile
    session = boto3.Session(profile_name='myprofile')

    # Create a client for the AWS Glue service
    glue_client = session.client('glue')

    # Call get_table() API to get the table metadata
    response = glue_client.get_table(DatabaseName=database_name, Name=table_name)

    # Extract the schema from the response
    schema = response['Table']['StorageDescriptor']['Columns']

    return schema

def convert_glue_type_to_redshift(glue_type):
    mapping = {
        'bigint': 'BIGINT',
        'int': 'INTEGER',
        'smallint': 'SMALLINT',
        'tinyint': 'SMALLINT',
        'double': 'DOUBLE PRECISION',
        'float': 'REAL',
        'boolean': 'BOOLEAN',
        'string': 'VARCHAR',
        'date': 'DATE',
        'timestamp': 'TIMESTAMP',
        'binary': 'BYTEA',
        'char': 'CHAR',
        'varchar': 'VARCHAR',
        'decimal': 'DECIMAL',
        'array': None,  # Redshift doesn't support these types
        'map': None,
        'struct': None,
    }
    return mapping.get(glue_type)

def create_table_statement(schema, table):
    # Get the base table name (without the timestamp)
    base_table_name = table.split('_')[0]

    create_table_query = f'DROP TABLE IF EXISTS public.{base_table_name}; CREATE TABLE IF NOT EXISTS public.{base_table_name} ('
    for column in schema:
        column_name = column['Name']
        redshift_type = convert_glue_type_to_redshift(column['Type'])
        if redshift_type is not None:
            create_table_query += f'{column_name} {redshift_type}, '
    create_table_query = create_table_query[:-2] + ');'
    return create_table_query

def generate_etl_script(database, table, schema):
    # Get the base table name (without the timestamp)
    base_table_name = table.split('_')[0]
    
    # Get the create table statement
    create_table_query = create_table_statement(schema, table)
    
    # Now create a template for the Glue ETL script
    etl_script = f"""
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

    S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
        database="{database}",
        table_name="{table}",
        transformation_ctx="S3bucket_node1",
    )

    AmazonRedshift_node1686057833145 = glueContext.write_dynamic_frame.from_options(
        frame=S3bucket_node1,
        connection_type="redshift",
        connection_options={{"redshiftTmpDir": "s3://aws-glue-assets-758443678568-us-east-2/temporary/",
            "useConnectionProperties": "true",
            "dbtable": "public.{base_table_name}",
            "connectionName": "redshift-connection",
            "preactions": "{create_table_query}",
        }},
        transformation_ctx="AmazonRedshift_node1686057833145",
    )

    job.commit()
    """

    return etl_script

# Usage:
database_name = 'your_database_name'
table_name = 'your_table_name'

# Get the table schema
schema = get_table_schema(database_name, table_name)

# Generate the ETL script
etl_script = generate_etl_script(database_name, table_name, schema)
print(etl_script)
