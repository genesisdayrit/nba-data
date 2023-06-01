import boto3
from datetime import datetime

def create_missing_crawlers(bucket_name, database_name, role_arn):
    s3 = boto3.client('s3')
    glue = boto3.client('glue')
    
    # Get all the existing crawlers
    existing_crawlers = glue.get_crawlers()['Crawlers']

    # Get all the folder_types in the bucket
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket_name, 'Delimiter': '/'}
    page_iterator = paginator.paginate(**operation_parameters)

    for page in page_iterator:
        for prefix in page.get('CommonPrefixes', []):
            folder_type = prefix['Prefix']  # Use this for crawler path

            # Get all the subfolders (exports) for this folder_type
            operation_parameters = {'Bucket': bucket_name, 'Prefix': folder_type, 'Delimiter': '/'}
            subfolder_page_iterator = paginator.paginate(**operation_parameters)

            subfolder_names = []
            for subfolder_page in subfolder_page_iterator:
                for subfolder_prefix in subfolder_page.get('CommonPrefixes', []):
                    subfolder_name = subfolder_prefix['Prefix']
                    subfolder_names.append(subfolder_name)
            
            # Get the latest subfolder
            latest_subfolder_path = 's3://' + bucket_name + '/' + sorted(subfolder_names, reverse=True)[0]

            # Delete the existing table for this folder_type
            table_name = folder_type.rstrip('/')
            glue.delete_table(DatabaseName=database_name, Name=table_name)
            
            # Create a new crawler for the latest subfolder
            crawler_name = table_name.replace('/', '-')  # Use this for crawler name
            new_crawler = {
                'Name': crawler_name,
                'Role': role_arn, 
                'DatabaseName': database_name, 
                'Targets': {'S3Targets': [{'Path': latest_subfolder_path}]},
                'SchemaChangePolicy': {
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
                },
                'RecrawlPolicy': {
                    'RecrawlBehavior': 'CRAWL_EVERYTHING'
                },
                'Description': f'Crawler for {latest_subfolder_path}',
            }
            glue.create_crawler(**new_crawler)
            print(f'Created new crawler {crawler_name} for {latest_subfolder_path}')

create_missing_crawlers(
    bucket_name="nba-data-access",
    database_name="nba-data",
    role_arn="arn:aws:iam::758443678568:role/service-role/AWSGlueServiceRole-NBA-data"
)
