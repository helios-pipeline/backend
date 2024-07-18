import json
import random
import time
from datetime import datetime, timezone
import boto3
from faker import Faker
import argparse
import logging

fake = Faker()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_kinesis_client(profile_name, region_name):
    try:
        session = boto3.Session(profile_name=profile_name)
        return session.client("kinesis", region_name=region_name)
    except Exception as e:
        logging.error(f"Failed to set up Kinesis client: {e}")
        raise

def generate_user_profile_data():
    return {
        "user_id": fake.uuid4(),
        "name": fake.name(),
        "email": fake.email(),
        "age": fake.random_int(min=18, max=80),
        "country": fake.country(),
        "registration_timestamp": int(time.time()),
    }

def generate_clickstream_data(user_id):
    event_types = ["click", "view", "purchase", "add_to_cart"]
    weights = [0.7, 0.2, 0.05, 0.05]  # Adjusted probabilities
    return {
        "user_id": user_id,
        "session_id": fake.uuid4(),
        "event_type": random.choices(event_types, weights=weights)[0],
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "page_url": fake.url(),
        "product_id": fake.random_int(min=1, max=100),
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "browser": random.choice(["chrome", "firefox", "safari", "edge"]),
    }

def send_to_kinesis(kinesis_client, stream_name, data):
    try:
        response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(data),
            PartitionKey=str(data["user_id"])
        )
        return response
    except Exception as e:
        logging.error(f"Failed to send data to Kinesis stream {stream_name}: {e}")
        return None

def stream_data(kinesis_client, user_profile_stream, clickstream_stream, duration=None):
    start_time = time.time()
    user_count = 0
    event_count = 0

    try:
        while duration is None or time.time() - start_time < duration:
            # Generate user profile data
            user_profile = generate_user_profile_data()
            send_to_kinesis(kinesis_client, user_profile_stream, user_profile)
            logging.info(f"Sent user profile: {user_profile['user_id']}")
            user_count += 1

            # Generate clickstream events for this user
            num_clicks = random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1])[0]
            for _ in range(num_clicks):
                clickstream_data = generate_clickstream_data(user_profile["user_id"])
                send_to_kinesis(kinesis_client, clickstream_stream, clickstream_data)
                logging.info(f"Sent clickstream event: {clickstream_data['event_type']} for user {clickstream_data['user_id']}")
                event_count += 1

            # Add some randomness to the generation interval
            time.sleep(random.expovariate(1))  # Average of 1 second, but with more variability

    except KeyboardInterrupt:
        logging.info("Script terminated by user.")
    finally:
        logging.info(f"Total users generated: {user_count}")
        logging.info(f"Total events generated: {event_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate and stream user profile and clickstream data to Kinesis.")
    parser.add_argument("--profile", default="capstone-team4", help="AWS profile name")
    parser.add_argument("--region", default="us-west-1", help="AWS region name")
    parser.add_argument("--user-stream", default="UserStream", help="Kinesis stream name for user profiles")
    parser.add_argument("--click-stream", default="Clickstream", help="Kinesis stream name for clickstream events")
    parser.add_argument("--duration", type=int, help="Duration to run the script (in seconds)")
    args = parser.parse_args()

    kinesis_client = setup_kinesis_client(args.profile, args.region)
    stream_data(kinesis_client, args.user_stream, args.click_stream, args.duration)