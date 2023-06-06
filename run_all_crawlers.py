import boto3
import time
import threading
import queue

# Create a session using a specific profile
session = boto3.Session(profile_name='myprofile')

# Create a client for the AWS Glue service using the session
glue_client = session.client('glue')

# This function will be executed by each worker thread
def worker():
    while True:
        crawler_name = crawler_queue.get()
        if crawler_name is None:
            break
        run_crawler(crawler_name)
        crawler_queue.task_done()

def run_crawler(crawler_name):
    print(f'Starting crawler: {crawler_name}')
    glue_client.start_crawler(Name=crawler_name)

    # Wait for the crawler to complete
    while True:
        response = glue_client.get_crawler(Name=crawler_name)
        crawler_status = response['Crawler']['State']
        if crawler_status == 'READY':
            print(f'Crawler {crawler_name} has completed.')
            break
        else:
            print(f'Crawler {crawler_name} is still running...')
            time.sleep(60)  # wait for 60 seconds before checking the status again

# Get list of all crawlers
response = glue_client.get_crawlers()

# Create a queue for crawler tasks
crawler_queue = queue.Queue()

# Start worker threads
num_worker_threads = 50  # adjust this value to your AWS Glue crawler limit
threads = []
for i in range(num_worker_threads):
    t = threading.Thread(target=worker)
    t.start()
    threads.append(t)

# Add tasks to the queue
for crawler in response['Crawlers']:
    crawler_queue.put(crawler['Name'])

# Block until all tasks are done
crawler_queue.join()

# Stop worker threads
for i in range(num_worker_threads):
    crawler_queue.put(None)
for t in threads:
    t.join()
