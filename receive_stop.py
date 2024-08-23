import json
import pandas as pd
from google.cloud import pubsub_v1
from google.cloud import storage
from concurrent import futures
import time
import psycopg2
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

project_id = 'dataengproj-420923'
subscription_id = 'titan-sub'
bucket_name = 'titan-bucket420923'
retry_limit = 5

db_config = { 
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'titan',
    'host': 'localhost'
}

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

dataframe_buffer = pd.DataFrame()

service_key_mapping = {
    'W': 'Weekday',
    'S': 'Saturday',
    'U': 'Sunday'
}

direction_mapping = {
    '0': 'Out',
    '1': 'Back'
}

def upload_to_bucket(data, filename, attempt=1):
    try:
        blob = bucket.blob(filename)
        blob.upload_from_string(data)
        logger.info(f"Uploaded {filename} to bucket {bucket.name}")
    except google.api_core.exceptions.TooManyRequests as e:
        if attempt <= retry_limit:
            wait_time = 2 ** attempt
            logger.warning(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            upload_to_bucket(data, filename, attempt + 1)
        else:
            logger.error(f"Failed to upload {filename} after {retry_limit} attempts: {e}")

def update_trip_data(cursor, data):
    data['service_key'] = service_key_mapping.get(data['service_key'], data['service_key'])
    data['direction'] = direction_mapping.get(data['direction'], data['direction'])

    update_query = """
    UPDATE Trip
    SET route_id = %s, service_key = %s, direction = %s
    WHERE trip_id = %s;
    """
    cursor.execute(update_query, (
        data['route_number'],
        data['service_key'],
        data['direction'],
        data['trip_id']
    ))

def callback(message):
    global dataframe_buffer
    data = json.loads(message.data)
    message.ack()

    df = pd.DataFrame([data])
    dataframe_buffer = pd.concat([dataframe_buffer, df], ignore_index=True)

    process_data(data)

def process_data(data):
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        update_trip_data(cursor, data)

        conn.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"Error processing data: {e}")
    finally:
        if conn is not None:
            conn.close()

def listen_for_messages(timeout):
    """Listen for messages on the specified subscription with a timeout."""
    streaming_pull_future = subscriber.subscribe(subscription_path, callback)
    logger.info(f"Listening for messages on {subscription_path} for {timeout} seconds...")

    try:
        streaming_pull_future.result(timeout=timeout)
    except futures.TimeoutError:
        logger.info(f"The subscriber timed out after waiting for messages for {timeout} seconds.")
    except Exception as e:
        logger.error(f"Listening for messages ended with exception: {e}")
    finally:
        streaming_pull_future.cancel()
        process_remaining_messages()

def process_remaining_messages():
    """Process and upload remaining messages in the dataframe buffer."""
    global dataframe_buffer
    if not dataframe_buffer.empty:
        today = datetime.utcnow().strftime('%Y%m%d')
        filename = f"stop_data_{today}.json"
        json_data = dataframe_buffer.to_json(orient='records', indent=4)
        upload_to_bucket(json_data, filename)
        dataframe_buffer = pd.DataFrame()

if __name__ == '__main__':
    listen_for_messages(500)