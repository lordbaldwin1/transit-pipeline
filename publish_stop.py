import requests
import json
from google.cloud import pubsub_v1
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

project_id = "dataengproj-420923"
topic_id = "titan-topic"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

base_url = "https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num="
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

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        pdx_trip = None
        table_headers = None
        for element in soup.find_all(['h2', 'table']):
            if element.name == 'h2':
                header_text = element.get_text()
                pdx_trip = header_text.split()[-1]
            elif element.name == 'table':
                table = element
                if table_headers is None:
                    table_headers = [th.get_text() for th in table.find_all('th')]
                records = []
                for row in table.find_all('tr')[1:]:
                    cells = row.find_all('td')
                    if len(cells) == len(table_headers):
                        record = {table_headers[i]: cells[i].get_text() for i in range(len(cells))}
                        record['trip_id'] = pdx_trip
                        records.append(record)

                if records:
                    for record in records:
                        message_json = json.dumps(record)
                        message_bytes = message_json.encode('utf-8')
                        trip_number = record.get('trip_number')
                        publisher.publish(topic_path, message_bytes)
                else:
                    logger.warning(f"No data extracted for vehicle ID {vehicle_id}, skipping...")
    elif response.status_code == 404:
        logger.warning(f"No data found for vehicle ID {vehicle_id}, skipping...")
    else:
        logger.error(f"Unexpected status code {response.status_code} for vehicle ID {vehicle_id}")

logger.info("Data publishing complete.")