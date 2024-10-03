import pandas as pd
import os
from confluent_kafka import Producer
import json
import shutil
import pickle
import asyncio
import pandera as pa
from validate_schema import validate_schema_producer, get_config
import sys
from functools import partial
from eh_cert_oauth import get_oauth_cred
from concurrent.futures import ThreadPoolExecutor

topic_name = sys.argv[2]
credential = get_oauth_cred()
producer_config = get_config(
    '/home/zk7nv5k/kafka_modules/py_azr_eventhub/Eventhub/kafka/sync/config.json', "producer"
)
namespace_fqdn = producer_config.get("bootstrap.servers").split(":")[0]

def oauth_cb(cred, namespace_fqdn, config):
    access_token = cred.get_token(f"https://{namespace_fqdn}/.default")
    return access_token.token, access_token.expires_on

producer_config["oauth_cb"] = partial(oauth_cb, credential, namespace_fqdn)
producer = Producer(producer_config)

async def move_file(source_path, destination_folder):
    try:
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"Source file not found: {source_path}")

        if not os.path.isdir(destination_folder):
            os.makedirs(destination_folder)

        shutil.move(source_path, destination_folder)
        print(f"File moved successfully to {destination_folder}")

    except Exception as e:
        print(f"An error occurred: {e}")

def check_format_and_read(file_path, filename, schema_file_path, schema_id):
    df = None
    if ".csv" in filename:
        print(f"Found CSV file: {filename}")
        df = pd.read_csv(file_path)
    elif ".json" in filename:
        print(f"Found JSON file: {filename}")
        df = pd.read_json(file_path)
    elif ".xlsx" in filename:
        print(f"Found Excel file: {filename}")
        df = pd.read_excel(file_path)
    elif ".parquet" in filename:
        print(f"Found Parquet file: {filename}")
        df = pd.read_parquet(file_path)
    elif ".orc" in filename:
        print(f"Found ORC file: {filename}")
        df = pd.read_orc(file_path)
    
    if df is not None:
        valid_schema = validate_schema_producer(file_path, schema_file_path, schema_id)
        return df if valid_schema else None
    return None

def get_batches(df):
    batch_size = 100
    print("Searching for batches for Kafka")
    batches = [df[i:i + batch_size] for i in range(0, len(df), batch_size)]
    return batches

def delivery_callback(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")

async def send_data(producer, topic_name, data, filename, schema_id):
    loop = asyncio.get_event_loop()
    batch_count = 0
    for batch in data:
        message = {
            "batch_number": batch_count,
            "df": batch,
            "filename": filename,
            "schema_id": schema_id
        }
        data = pickle.dumps(message)
        # Using run_in_executor to call the blocking 'produce' method asynchronously
        await loop.run_in_executor(None, producer.produce, topic_name, f"{batch_count}", data, delivery_callback)
        await loop.run_in_executor(None, producer.flush)
        batch_count += 1

async def process_file(source_path, archive_path, schema_file_path, schema_id, filename, stats):
    file_path = os.path.join(source_path, filename)
    df = check_format_and_read(file_path, filename, schema_file_path, schema_id)
    if df is not None:
        stats[filename.split(".")[-1]] += 1
        batches = get_batches(df)
        await send_data(producer, topic_name, batches, filename, schema_id)
        await move_file(file_path, archive_path)
    else:
        print("Provide the file with the given formats only: csv, json, xlsx, parquet, orc")

async def main():
    source_path = f'/home/zk7nv5k/kafka_modules/py_azr_eventhub/Eventhub/{sys.argv[1]}'
    archive_path = '/home/zk7nv5k/kafka_modules/py_azr_eventhub/Eventhub/archive/'
    schema_file_path = '/home/zk7nv5k/kafka_modules/py_azr_eventhub/Eventhub/schemas/schema.json'
    schema_id = sys.argv[3]
    stats = {"csv": 0, "json": 0, "xlsx": 0, "orc": 0, "parquet": 0}

    tasks = []
    with ThreadPoolExecutor() as executor:
        for filename in os.listdir(source_path):
            tasks.append(process_file(source_path, archive_path, schema_file_path, schema_id, filename, stats))
        
        await asyncio.gather(*tasks)

    print(stats)

if __name__ == "__main__":
    asyncio.run(main())
