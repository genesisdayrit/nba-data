import boto3

def create_missing_crawlers(bucket_name, folder_prefix, database_name, role_arn):
    s3 = boto3.client('s3')
    glue = boto3.client('glue')
    
    # Get all the existing crawlers
    existing_crawlers = glue.get_crawlers()['Crawlers']

    # Get all the folders in the bucket
    paginator = s3.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket_name, 'Prefix': folder_prefix, 'Delimiter': '/'}
    page_iterator = paginator.paginate(**operation_parameters)

    for page in page_iterator:
        for prefix in page.get('CommonPrefixes', []):
            folder_name = prefix['Prefix'].replace('data/', '').replace('/', '')  # Use this for crawler name
            folder_path = 's3://' + bucket_name + '/' + prefix['Prefix']  # Use this for crawler path

            # Find and delete any existing crawlers with a different name but the same path
            for crawler in existing_crawlers:
                crawler_path = crawler['Targets']['S3Targets'][0]['Path']
                crawler_name = crawler['Name']
                if folder_path == crawler_path and crawler_name != f'{folder_name}-crawler':
                    glue.delete_crawler(Name=crawler_name)
                    print(f'Deleted old crawler {crawler_name} for {folder_path}')

            # Check if there's an existing crawler for this folder
            existing_crawler_names = [crawler['Name'] for crawler in existing_crawlers]
            if f'{folder_name}-crawler' not in existing_crawler_names:
                # Create a new crawler for the folder
                new_crawler = {
                    'Name': f'{folder_name}-crawler',
                    'Role': role_arn, 
                    'DatabaseName': database_name, 
                    'Targets': {'S3Targets': [{'Path': folder_path}]},
                    'SchemaChangePolicy': {
                        'UpdateBehavior': 'UPDATE_IN_DATABASE',
                        'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
                    },
                    'RecrawlPolicy': {
                        'RecrawlBehavior': 'CRAWL_EVERYTHING'
                    },
                    'Description': f'Crawler for {folder_path}',
                    # 'TablePrefix': f'{folder_name}_',
                }
                glue.create_crawler(**new_crawler)
                print(f'Created new crawler {folder_name} for {folder_path}')

create_missing_crawlers(
    bucket_name="nba-data-access",
    folder_prefix="data/",
    database_name="nba-data",
    role_arn="arn:aws:iam::758443678568:role/service-role/AWSGlueServiceRole-NBA-data"
)
