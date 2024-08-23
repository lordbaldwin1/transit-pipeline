import requests
import json
from google.cloud import pubsub_v1

project_id = "dataengproj-420923"
topic_id = "titan-topic"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

base_url = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="
vehicle_ids = [
    3624, 3912, 3104, 4044, 3248, 3150, 4023, 3546, 3757, 3324, 3751, 4238, 3259,
    3572, 3045, 4070, 3320, 2921, 3522, 3315, 4043, 2913, 3201, 4054, 4003, 3735,
    3723, 3628, 3905, 3551, 3035, 3508, 3044, 3101, 4216, 3018, 3309, 3560, 3501,
    3254, 3103, 3554, 3531, 3533, 3420, 3422, 3936, 3919, 2910, 3238, 4039, 3754,
    3112, 3647, 3515, 4222, 3014, 3143, 3739, 4056, 2923, 3009, 4516, 3646, 3249,
    4033, 4214, 3406, 3744, 4217, 3909, 3935, 3046, 3960, 3606, 3517, 3602, 3725,
    3216, 4232, 3703, 3927, 3267, 3536, 3637, 3029, 3942, 3122, 4065, 3033, 3003,
    3020, 3047, 3571, 3415, 2903, 3726, 4525, 3930, 4303
]

for vehicle_id in vehicle_ids:
    response = requests.get(f"{base_url}{vehicle_id}")
    if response.status_code == 404:
        print(f"No data found for vehicle ID {vehicle_id}, skipping...")
        continue
    elif response.status_code == 200:
        try:
            records = response.json()
            if records:
                for record in records:
                    message_json = json.dumps(record)
                    message_bytes = message_json.encode('utf-8')
                    publisher.publish(topic_path, message_bytes)
            else:
                print(f"NO DATA FOR vehicle ID {vehicle_id}, skipping...")
        except json.JSONDecodeError:
            print(f"DECODE FAILED (NEVER HAPPENS!) vehicle ID {vehicle_id}")
    else:
        print(f"ERROR UNEXPECTED STATUS CODE {response.status_code} for vehicle ID {vehicle_id}")

print("Data publishing complete.")