import boto3

def create_missing_crawlers(bucket_name, folder_prefix, database_name, role_arn):
    s3 = boto3.client('s3')
    glue = boto3.client('glue')
    
    # Get all the existing crawlers
    existing_crawlers = glue.get_crawlers()['Crawlers']
    existing_crawler_paths = [crawler['Targets']['S3Targets'][0]['Path'] for crawler in existing_crawlers]
    
    # Get all the folders in the bucket
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket_name, 'Prefix': folder_prefix, 'Delimiter': '/'}
    page_iterator = paginator.paginate(**operation_parameters)
    
    for page in page_iterator:
        for prefix in page.get('CommonPrefixes', []):
            folder_path = 's3://' + bucket_name + '/' + prefix['Prefix']
            
            # Check if there's an existing crawler for this folder
            if folder_path not in existing_crawler_paths:
                # Create a new crawler for the folder
                crawler_name = prefix['Prefix'].replace('/', '')  # Remove slashes from the prefix to use it as a name
                new_crawler = {
                    'Name': f'{crawler_name}-crawler',
                    'Role': role_arn, 
                    'DatabaseName': database_name, 
                    'Targets': {'S3Targets': [{'Path': folder_path}]},
                    'SchemaChangePolicy': {
                        'UpdateBehavior': 'LOG',
                        'DeleteBehavior': 'LOG'
                    },
                    'RecrawlPolicy': {
                        'RecrawlBehavior': 'CRAWL_NEW_FOLDERS_ONLY'
                    },
                    'Description': f'Crawler for {folder_path}',
                    'TablePrefix': f'{crawler_name}_',
                }
                glue.create_crawler(**new_crawler)
                print(f'Created new crawler {crawler_name} for {folder_path}')

create_missing_crawlers(
    bucket_name="nba-data-access",
    folder_prefix="data/",
    database_name="nba-data",
    role_arn="arn:aws:iam::758443678568:role/service-role/AWSGlueServiceRole-NBA-data"
)
