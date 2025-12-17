import time
import json
import pandas as pd
from kafka import KafkaProducer
import yaml

with open("./configs/app_config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)


PRODUCER = KafkaProducer(
    # bootstrap_servers=config['kafka']['bootstrap_servers'],
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

TOPIC = config['kafka']['topic']
CSV_FILE = "./data/T_ONTIME_REPORTING_2025_M1.csv"

def run():
    print(f"Bắt đầu Replay dữ liệu từ {CSV_FILE} vào topic '{TOPIC}'...")
    # Đọc từng block 100 dòng để tiết kiệm RAM
    for chunk in pd.read_csv(CSV_FILE, chunksize=100):
        for _, row in chunk.iterrows():
            record = row.to_dict()
            # Xử lý NaN thành None (để JSON hiểu là null)
            record = {k: (None if pd.isna(v) else v) for k, v in record.items()}
            
            PRODUCER.send(TOPIC, value=record)
            print(f"-> Sent: {record.get('OP_UNIQUE_CARRIER')} | Date: {record.get('FL_DATE')}")
            
            time.sleep(0.0001) # Giả lập độ trễ (0.5s một bản tin)

if __name__ == "__main__":
    run()