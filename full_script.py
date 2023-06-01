import boto3
from datetime import datetime
from botocore.exceptions import ClientError

def table_exists(glue, database_name, table_name):
    try:
        glue.get_table(DatabaseName=database_name, Name=table_name)
    except glue.exceptions.EntityNotFoundException:
        return False
    return True

def create_missing_crawlers(bucket_name, folder_prefix, database_name, role_arn):
    session = boto3.Session(profile_name='myprofile')
    s3 = session.client('s3')
    glue = session.client('glue')

    # Get all the folders in the bucket
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket_name, 'Prefix': folder_prefix, 'Delimiter': '/'}
    page_iterator = paginator.paginate(**operation_parameters)

    for page in page_iterator:
        for prefix in page.get('CommonPrefixes', []):
            folder_type = prefix['Prefix']
            table_name = folder_type.rstrip('/').split('/')[-1]

            # Delete the existing table for this folder_type if it exists
            if table_exists(glue, database_name, table_name):
                glue.delete_table(DatabaseName=database_name, Name=table_name)

            # Get the latest subfolder within the folder_type
            subfolders = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_type, Delimiter='/').get('CommonPrefixes')
            latest_subfolder = sorted(subfolders, key=lambda k: k['Prefix'], reverse=True)[0]

            # Path for the latest folder
            latest_folder_path = 's3://' + bucket_name + '/' + latest_subfolder['Prefix']

            # Get all the existing crawlers
            existing_crawlers = glue.get_crawlers()['Crawlers']

            # Find and delete any existing crawlers with the same path
            for crawler in existing_crawlers:
                crawler_path = crawler['Targets']['S3Targets'][0]['Path']
                # Checking if the crawler path starts with the folder prefix, not the full latest sub-folder
                if crawler_path.startswith('s3://' + bucket_name + '/' + folder_type):
                    glue.delete_crawler(Name=crawler['Name'])
                    print(f'Deleted old crawler {crawler["Name"]} for {crawler_path}')

            # Get the updated list of existing crawlers
            existing_crawlers = glue.get_crawlers()['Crawlers']

            # Check if there's an existing crawler for this folder
            existing_crawler_names = [crawler['Name'] for crawler in existing_crawlers]
            if f'{table_name}-crawler' not in existing_crawler_names:
                # Create a new crawler for the folder
                new_crawler = {
                    'Name': f'{table_name}-crawler',
                    'Role': role_arn, 
                    'DatabaseName': database_name, 
                    'Targets': {'S3Targets': [{'Path': latest_folder_path}]},
                    'SchemaChangePolicy': {
                        'UpdateBehavior': 'UPDATE_IN_DATABASE',
                        'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
                    },
                    'RecrawlPolicy': {
                        'RecrawlBehavior': 'CRAWL_EVERYTHING'
                    },
                    'Description': f'Crawler for {latest_folder_path}',
                    'TablePrefix': f'{table_name}_',
                }
                glue.create_crawler(**new_crawler)
                print(f'Created new crawler {table_name}-crawler for {latest_folder_path}')

create_missing_crawlers(
    bucket_name="nba-data-access",
    folder_prefix="data/",
    database_name="nba-data",
    role_arn="arn:aws:iam::758443678568:role/service-role/AWSGlueServiceRole-NBA-data"
)
