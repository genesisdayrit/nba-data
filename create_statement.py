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
    """Function to convert AWS Glue data type to Redshift data type."""
    mapping = {
        'bigint': 'BIGINT',
        'string': 'VARCHAR(256)',
        'boolean': 'BOOLEAN',
        # Add more mappings as needed
    }
    return mapping.get(glue_type, 'VARCHAR(256)')  # default to VARCHAR(256) if the Glue data type is not in the mapping

def create_table_statement(table_name, schema):
    """Function to create a Redshift-compatible CREATE TABLE statement from the schema."""
    column_definitions = []
    for column in schema:
        column_name = column['Name']
        redshift_data_type = convert_glue_type_to_redshift(column['Type'])
        column_definitions.append(f"{column_name} {redshift_data_type}")
    column_definitions_str = ', '.join(column_definitions)
    drop_table_stmt = f"DROP TABLE IF EXISTS public.{table_name};"
    create_table_stmt = f"CREATE TABLE IF NOT EXISTS public.{table_name} ({column_definitions_str});"
    return drop_table_stmt + ' ' + create_table_stmt

# Usage:
database_name = 'nba-data'
table_name = 'game_details_2023_08_30'
schema = get_table_schema(database_name, table_name)
stmt = create_table_statement(table_name, schema)
print(stmt)
