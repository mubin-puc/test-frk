import os
import json
import base64
import shutil
from confluent_kafka import Producer
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from eh_cert_oauth import get_oauth_cred

credential = get_oauth_cred()
namespace_fqdn = 'enhns-eus-d-73808-app.servicebus.windows.net'

def oauth_cb(cred, namespace_fqdn, config):
    access_token = cred.get_token(f"https://{namespace_fqdn}/.default")
    return access_token.token, access_token.expires_on

producer = Producer({
    'bootstrap.servers': f'{namespace_fqdn}:9093',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'OAUTHBEARER',
    'oauth_cb': partial(oauth_cb, credential, namespace_fqdn),
})

def produce(file_path, dest_file_path, archive_folder):
    if os.path.isfile(file_path):
        send_file(file_path)
        if os.path.exists(dest_file_path):
            print(f"File {dest_file_path} already exists and will be replaced.")
        os.replace(file_path, dest_file_path)
        print(f"File overwritten successfully to {dest_file_path}.")
    else:
        print(f"File {dest_file_path} does not exist.")
        move_file(file_path, archive_folder)
        print(f"File {dest_file_path} has been moved.")

def move_file(source_path, destination_folder):
    try:
        shutil.move(source_path, destination_folder)
        print(f"File moved successfully to {destination_folder}")
    except FileNotFoundError as e:
        print(e)
    except NotADirectoryError as e:
        print(e)
    except Exception as e:
        print(f"An error occurred: {e}")

def send_file(file_path):
    topic = 'evh-eus-d-73808-app-one'
    chunk_size = 1024 * 500
    with open(file_path, 'rb') as file:
        filename = os.path.basename(file_path)
        chunk_number = 0
        while (chunk := file.read(chunk_size)):
            print(f"Reading chunk number {chunk_number}")
            message = {
                'filename': filename,
                'chunk_number': chunk_number,
                'total_chunks': (os.path.getsize(file_path) + chunk_size - 1) // chunk_size,
                'data': base64.b64encode(chunk).decode('utf-8')
            }
            producer.produce(topic, key=str(chunk_number), value=json.dumps(message), callback=delivery_callback)
            chunk_number += 1

def delivery_callback(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def process_files_asynchronously(source_folder, archive_folder):
    with ThreadPoolExecutor() as executor:
        futures = []
        for filename in os.listdir(source_folder):
            if filename == '.DS_Store':
                continue
            file_path = os.path.join(source_folder, filename)
            dest_file_path = os.path.join(archive_folder, filename)
            # Submit file processing tasks asynchronously
            futures.append(executor.submit(produce, file_path, dest_file_path, archive_folder))
        
        # Wait for all futures to complete
        for future in futures:
            try:
                future.result()
            except Exception as e:
                print(f"Error processing file: {e}")
    
    # Flush producer outside the loop to ensure all messages are delivered
    producer.flush()

if __name__ == "__main__":
    source_folder = '/home/zk7nv5k/kafka_module/py_azr_eventhub/Eventhub/input'
    archive_folder = '/home/zk7nv5k/kafka_module/py_azr_eventhub/Eventhub/archive'

    process_files_asynchronously(source_folder, archive_folder)
