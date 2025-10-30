import pandas as pd
import time
import json

from confluent_kafka import Producer
import socket

def create_producer():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname(),
    }
    producer = Producer(conf)
    return producer

# Callback when produce event to kafka successfully
def produce_message_success_callback(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Delivery successfully sent to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


if __name__ == "__main__":

    # Create Producer
    producer = create_producer()

    # Data path
    data_path = "../../data/archive/dataset.csv" # Path to data
    topic = "streaming_layer_topic"

    # Read data
    chunk_size = 1000
    try:
        df = pd.read_csv(data_path, chunksize=chunk_size)
    except FileNotFoundError:
        print(f"File {data_path} not found.")
        exit(1)

    # Produce row
    for chunk_idx, chunk in enumerate(df):
        print(f"Start processing chunk {chunk_idx + 1} with {len(chunk)} records")

        send_count = 0
        for idx, row in chunk.iterrows():
            print(f"Row: {row}, type: {type(row)}, columns: {row}")
            record = row.to_dict()
            print(f"Record: {record}, type: {type(record)}")
            key = str(record.get("id", idx))
            print(f"Key: {key}, type: {type(key)}")
            value = json.dumps(record).encode("utf-8")
            print(f"Value: {value}, type: {type(value)}")

            # Produce event
            producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=produce_message_success_callback
            )

            # Flush buffer
            producer.poll(0)
            send_count += 1

            # 1s produce 1 event
            print(f"Sent row {idx+1}/{len(chunk)} to {topic}")

            if (send_count) % 100 == 0:
                time.sleep(1)

        producer.flush()
        print(f"Finish processing chunk {chunk_idx + 1} with {len(chunk)} records")

    producer.flush()
    print(f"Process all records")