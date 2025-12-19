import pandas as pd
import time
import json

from confluent_kafka import Producer, KafkaException
import socket

def create_producer():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname(),
        'enable.idempotence': True,
        'max.in.flight.requests.per.connection': 1,
        'transactional.id': 'producer-transactional' + str(time.time()),
    }

    producer = Producer(conf)
    producer.init_transactions()
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
    data_path = "./data/dataset.csv"  # Path to data
    topic = "flight_topic"

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

        max_retry = 5
        count_retry = 0
        while count_retry < max_retry:
            try:
                producer.begin_transaction()
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
                    print(f"Sent row {idx+1} to {topic}")

                    if send_count % 100 == 0:
                        time.sleep(1)
                print(f"Finished chunk {chunk_idx+1}/{len(chunk)}")
                time.sleep(5)
                producer.commit_transaction()
                print(f"Committed successfully for chunk {chunk_idx + 1} {send_count} records")
                print(f"Finish processing chunk {chunk_idx + 1} with {len(chunk)} records")
                break

            except KafkaException as e:
                err = e.args[0]
                if err.retriable():
                    print(f"Retry error, retry count {count_retry + 1}/{max_retry} for chunk {chunk_idx + 1} {send_count} records")
                    count_retry = count_retry + 1
                    time.sleep(1)
                    continue
                elif err.txn_requires_abort():
                    print(f"Abort error for chunk {chunk_idx + 1} {send_count} records")
                    producer.abort_transaction()
                    count_retry = count_retry + 1
                    time.sleep(1)
                    continue
                elif err.fatal():
                    print(f"Fatal error for chunk {chunk_idx + 1} {send_count} records")
                    producer.close()
                    raise
                else:
                    print("Unknown error for chunk {chunk_idx + 1} {send_count} records")
                    raise

        if count_retry >= max_retry:
            print(f"Max retry reached for chunk {chunk_idx + 1} {send_count} records")
            producer.abort_transaction()

    producer.flush()
    print(f"Process all records")