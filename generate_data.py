import json
import random
import time
from datetime import datetime
import boto3
from faker import Faker

fake = Faker()

root_session = boto3.Session(profile_name='capstone-team4')  # Set profile


kinesis_client = root_session.client(
    "kinesis", region_name="us-west-1"
)  # Ensure this is the correct region


import random
from datetime import datetime, timezone

fake = Faker()


def generate_clickstream_data(user_id):
    # Generate a timestamp based on the current time in UTC
    # event_timestamp = datetime.now(timezone.utc)

    return {
        "user_id": user_id,
        "session_id": fake.uuid4(),
        "event_type": random.choice(["click", "view", "purchase", "add_to_cart"]),
        # "event_timestamp": event_timestamp.isoformat(),
        # "event_timestamp": event_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "event_timestamp": 1,
        "page_url": fake.url(),
        "product_id": fake.random_int(min=1, max=100),
    }


def generate_purchase_history(user_id):
    # Generate a timestamp based on the current time in UTC
    purchase_timestamp = datetime.now(timezone.utc)

    return {
        "user_id": user_id,
        "purchase_id": fake.random_int(min=1000, max=5000),
        "product_id": fake.random_int(min=1, max=100),
        "category": random.choice(["electronics", "books", "clothing", "home"]),
        "purchase_timestamp": purchase_timestamp.isoformat(),
        "amount": round(random.uniform(5.0, 500.0), 2),
    }

def generate_user_profile_data(user_id):
    return {
        "user_id": user_id,
        "name": fake.name(),
        "email": fake.email(),
        "age": fake.random_int(min=18, max=80),
        "country": fake.country(),
        "registration_timestamp": int(time.time()),
    }


def send_to_kinesis(stream_name, data):
    kinesis_client.put_record(
        StreamName=stream_name, Data=json.dumps(data), PartitionKey=str(data["user_id"])
    )


def stream_data():
    click_rate = 2  # clicks per second
    purchase_rate = 1  # purchases per minute
    purchase_interval = 60 / purchase_rate  # time interval between purchases
    last_purchase_time = time.time()

    try:
        while True:
        # while count < 2:
            current_time = time.time()
            # Generate user ID
            user_id = fake.random_int(min=1, max=1000)
            # Generate clickstream data
            clickstream_data = generate_clickstream_data(user_id)
            clickstream_data2 = generate_user_profile_data(user_id)

            send_to_kinesis("MyKinesisDataStream", clickstream_data)
            send_to_kinesis("SecondDataStream", clickstream_data2)

            print("MyKinesisDataStream data:", json.dumps(clickstream_data))
            print("SecondDataStream data:", json.dumps(clickstream_data2))
            time.sleep(1 / click_rate)

    except KeyboardInterrupt:
        print("Script terminated by user.")


if __name__ == "__main__":
    stream_data()
