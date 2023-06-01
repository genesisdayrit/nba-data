import boto3

def get_table_schema(database_name, table_name):
    glue = boto3.client('glue')

    response = glue.get_table(DatabaseName=database_name, Name=table_name)

    columns = response['Table']['StorageDescriptor']['Columns']

    return [(col['Name'], col['Type']) for col in columns]

# Test the function
print(get_table_schema('nba-data', 'game_details'))
