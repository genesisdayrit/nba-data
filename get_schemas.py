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

# Usage:
schema = get_table_schema('nba-data', 'game_details_2023_08_30')
print(schema)
