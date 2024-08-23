import pandas as pd
from google.cloud import pubsub_v1, storage
import json
from datetime import datetime, timedelta
import traceback
import psycopg2

project_id = "dataengproj-420923"
subscription_id = "titan-sub"
bucket_name = "titan-bucket420923"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

data_list = []
last_act_times = {} # used to keep track of the previous act_time for the vehicle, for validation
records_seen = set() # used to check for duplicates of data

# data validation 
def validate(data, last_act_times, records_seen):
    errors = []

    # every breadcrumb needs a vehicle id 
    # also needs a stop ID
    if not data.get('VEHICLE_ID'):
        errors.append("Missing vehicle ID")
    try:
        if 'EVENT_NO_STOP' in data:
            data['EVENT_NO_STOP'] = int(data['EVENT_NO_STOP'])
        else:
            errors.append("EVENT_NO_STOP is missing")
    except ValueError:
        errors.append("EVENT_NO_STOP should be in integer format")

    # no negatives!
    if data.get('METERS', 0) < 0:
        errors.append("Meters cannot be negative")

    # valid num of satellites 
    if data.get('GPS_SATELLITES', 1) == 0:
        errors.append("GPS_SATELLITES should not be 0 during normal operations")


    # correct format of date
    # also check if the time is in the future
    try:
        opd_date = datetime.strptime(data['OPD_DATE'], '%d%b%Y:%H:%M:%S')
        if opd_date > datetime.now():
            errors.append("OPD_DATE should not be in the future")
    except ValueError:
        errors.append("OPD_DATE does not match format DDMMMYYYY:HH:MM:SS")


    # time must be in range (less than seconds of the day)
    act_time = data.get('ACT_TIME', -1)
    if not (0 <= act_time <= 86399):
        errors.append("ACT_TIME should be between 0 and 86399 seconds")



    # the subsequent time must be greater than previous on the same trip
    trip_id = data.get('EVENT_NO_TRIP')
    if trip_id is not None and trip_id in last_act_times:
        if act_time <= last_act_times[trip_id]:
            errors.append("ACT_TIME should be greater than the previous record during the same trip")
    last_act_times[trip_id] = act_time

    
    # every lat must have a long
    if 'GPS_LATITUDE' in data and 'GPS_LONGITUDE' not in data:
        errors.append("Every latitude must have a corresponding longitude")


    # no duplicate data allowed!
    record_signature = tuple(data.items())
    if record_signature in records_seen:
        errors.append("Duplicate data found")
    records_seen.add(record_signature)


    return errors

# loads data from pub/sub messages into a list
def callback(message):
    try:
        data = json.loads(message.data.decode('utf-8'))
        errors = validate(data, last_act_times, records_seen)
        data_list.append(data)
        if errors:
            print("Validation errors found:", errors)
            message.nack() 
        else:
            message.ack()
            print("Data is valid and processed:", data)
    except Exception as e:
        print("Error processing message:", str(e))
        traceback.print_exc()
        message.nack()

# pulls messages from pub/sub 
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"connected to {subscription_path}.")
try:
    streaming_pull_future.result(timeout=600)
except TimeoutError:
    print("Timeout of 10 minutes reached.")
except KeyboardInterrupt:
    print("terminated by user.")
except Exception as e:
    print(f"An error occurred: {type(e).__name__}, {str(e)}")
    traceback.print_exc()
finally:
    streaming_pull_future.cancel()
    print("Stopped listening to messages.")

# creates pandas dataframe out of list and performs transformations
if data_list:
    data_frame = pd.DataFrame(data_list)
    data_frame.sort_values(by=['EVENT_NO_TRIP', 'ACT_TIME'], inplace=True)

    # speed calculations
    if 'Speed' not in data_frame.columns:
        data_frame['Speed'] = 0

    data_frame['Speed'] = data_frame.groupby('EVENT_NO_TRIP')['METERS'].diff() / data_frame.groupby('EVENT_NO_TRIP')['ACT_TIME'].diff()
    data_frame['Speed'] = data_frame.groupby('EVENT_NO_TRIP')['Speed'].transform(lambda x: x.ffill())
    data_frame['Speed'] = data_frame.groupby('EVENT_NO_TRIP')['Speed'].transform(lambda x: x.bfill())

    # timestamp calculation
    def create_timestamp(row):
        date_part = datetime.strptime(row['OPD_DATE'], '%d%b%Y:%H:%M:%S')
        time_delta = timedelta(seconds=row['ACT_TIME'])
        return date_part + time_delta
    data_frame['TIMESTAMP'] = data_frame.apply(create_timestamp, axis=1)

    # dropping unneeded columns
    data_frame.drop(columns=['EVENT_NO_STOP', 'METERS', 'GPS_SATELLITES', 'GPS_HDOP'], inplace=True)
    data_frame.drop(columns=['OPD_DATE', 'ACT_TIME'], inplace=True)

    data_frame.rename(columns={'EVENT_NO_TRIP': 'trip_id', 'VEHICLE_ID': 'vehicle_id', 'GPS_LONGITUDE': 'longitude', 'GPS_LATITUDE': 'latitude', 'Speed': 'speed', 'TIMESTAMP': 'tstamp'}, inplace=True)

    # database parameters
    conn_params = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'titan',
        'host': 'localhost',
}
    
    # connects to database and inserts data from dataframe into tables
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    unique_trips = data_frame[['trip_id', 'vehicle_id']].drop_duplicates()
    try:
        cur.executemany(
            "INSERT INTO Trip (trip_id, vehicle_id) VALUES (%s, %s) ON CONFLICT (trip_id) DO NOTHING;",
            unique_trips.itertuples(index=False, name=None)
        )
        cur.executemany(
            "INSERT INTO BreadCrumb (tstamp, latitude, longitude, speed, trip_id) VALUES (%s, %s, %s, %s, %s);",
            data_frame[['tstamp', 'latitude', 'longitude', 'speed', 'trip_id']].itertuples(index=False, name=None)
        )
        conn.commit()
    except Exception as e:
        print("Failed to insert data:", e)
    finally:
        cur.close()
        conn.close()

    # saves data to csv/json files and uploads to google cloud bucket
    current_date = datetime.now().strftime("%Y%m%d")
    csv_filename = f"transformed_data_{current_date}.csv"
    json_filename = f"transformed_data_{current_date}.json"

    csv_path = f"/tmp/{csv_filename}"
    data_frame.to_csv(csv_path, index=False)
    blob_csv = bucket.blob(csv_filename)
    blob_csv.upload_from_filename(csv_path)
    print(f"Uploaded {csv_filename} to {bucket_name}")

    json_path = f"/tmp/{json_filename}"
    data_frame.to_json(json_path, orient='records', lines=True)
    blob_json = bucket.blob(json_filename)
    blob_json.upload_from_filename(json_path)
    print(f"Uploaded {json_filename} to {bucket_name}")
else:
    print("No data to process.")