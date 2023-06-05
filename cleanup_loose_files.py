import boto3
from datetime import datetime

session = boto3.Session(profile_name='myprofile')
s3 = session.client('s3')
bucket_name = 'nba-data-access'

def list_directories(s3, bucket, prefix):
    """Return a list of directories in an S3 bucket with given prefix"""
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
    directories = [content['Prefix'] for content in response.get('CommonPrefixes', [])]
    return directories

def list_files(s3, bucket, prefix, suffix):
    """Return a list of file paths in an S3 bucket with given prefix and suffix"""
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [content['Key'] for content in response['Contents'] if content['Key'].endswith(suffix)]
    return files

def move_s3_object(s3, bucket, old_key, new_key):
    """Move an object within S3 by copying to new location and deleting from old"""
    s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': old_key}, Key=new_key)
    s3.delete_object(Bucket=bucket, Key=old_key)
    print(f"Moved file from {old_key} to {new_key}")

def main():
    entity_types = list_directories(s3, bucket_name, 'data/')
    for entity_type in entity_types:
        loose_files = list_files(s3, bucket_name, entity_type, '.csv')
        for file_path in loose_files:
            # Exclude files that are in the export date folders
            if '/' not in file_path[len(entity_type):]:
                today_folder = datetime.now().strftime('%Y-%m-%d')
                new_key = f'archive/{today_folder}/{file_path.split("/")[-1]}'
                move_s3_object(s3, bucket_name, file_path, new_key)

if __name__ == "__main__":
    main()
