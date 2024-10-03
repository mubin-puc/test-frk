import pandas as pd
from confluent_kafka import Consumer, TopicPartition
import os
import json
import pickle
from validate_schema import validate_schema_consumer, get_config
import sys
from functools import partial
from eh_cert_oauth import get_oauth_cred
import asyncio
from concurrent.futures import ThreadPoolExecutor

credential = get_oauth_cred()

consumer_config = get_config(
    '/home/zk7nv5k/kafka_modules/py_azr_eventhub/Eventhub/kafka/sync/config.json', "consumer"
)
namespace_fqdn = consumer_config.get("bootstrap.servers").split(":")[0]

def oauth_cb(cred, namespace_fqdn, config):
    # Note: confluent_kafka passes 'Sasl.oauthbearer.config' as the config param
    access_token = cred.get_token(f"https://{namespace_fqdn}/.default")
    return access_token.token, access_token.expires_on

consumer_config["oauth_cb"] = partial(oauth_cb, credential, namespace_fqdn)

consumer = Consumer(consumer_config)

topic_name = sys.argv[1]
partition_number = sys.argv[2]

output_path = '/home/zk7nv5k/kafka_modules/py_azr_eventhub/Eventhub/output'
schema_path = '/home/zk7nv5k/kafka_modules/py_azr_eventhub/Eventhub/schemas/schema.json'

def get_filename(filename):
    if not '.csv' in filename:
        filename = f"{filename.split('.')[0]}.csv"
    return filename

async def consume_data():
    consumer.subscribe([topic_name])
    consumer.assign([TopicPartition(topic_name, int(partition_number))])

    # Use a thread pool executor for blocking I/O operations
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        while True:
            # Use asyncio to poll for messages
            msg = await loop.run_in_executor(pool, consumer.poll, 1.0)
            print("Polling for data")
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
            else:
                # Deserialize data
                data = pickle.loads(msg.value())
                df = data['df']
                filename = get_filename(data['filename'])
                batch_number = data['batch_number']
                schema_id = data["schema_id"]
                print(f"Batch number: {batch_number} of file: {filename} has been read from the partition: {msg.partition()}")

                # Validate schema asynchronously
                is_valid = await loop.run_in_executor(pool, validate_schema_consumer, df, schema_path, schema_id, batch_number)
                if is_valid:
                    # Create directory if it doesn't exist
                    if not os.path.isdir(output_path):
                        os.makedirs(output_path)
                        print(f"CREATED {output_path}")

                    key = msg.key().decode("utf-8")
                    # Write to CSV asynchronously
                    if key == "0":
                        await loop.run_in_executor(pool, df.to_csv, f"{output_path}/{filename}", False, "a", True)
                    else:
                        await loop.run_in_executor(pool, df.to_csv, f"{output_path}/{filename}", False, "a", False)
                else:
                    print(f"Schema validation failed for batch number: {batch_number} of file {filename} for schema_id {schema_id}")

if __name__ == "__main__":
    # Run the asynchronous function using asyncio
    asyncio.run(consume_data())
