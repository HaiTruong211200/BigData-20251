import time
import json
import pandas as pd
import yaml
import sys
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Tắt buffering bằng code python
sys.stdout.reconfigure(line_buffering=True)

# Dùng đường dẫn tuyệt đối cho chắc chắn
CONFIG_PATH = "./configs/app_config.yaml"
# CSV_FILE = "/app/data/stream_source/T_ONTIME_REPORTING_2025_M6.csv" 
# Hoặc lấy từ config nếu bạn đã cấu hình:
CSV_FILE = "./data/T_ONTIME_REPORTING_2025_M6.csv"

def load_config():
    print(f"DEBUG: Loading config from {CONFIG_PATH}...", flush=True)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def run():
    config = load_config()
    server = config['kafka']['bootstrap_servers']
    topic = config['kafka']['topic']
    
    # Lấy đường dẫn CSV từ config hoặc hardcode
    csv_path = config.get('paths', {}).get('local_source_data', CSV_FILE)

    print(f"DEBUG: Target Kafka Server: {server}", flush=True)

    # --- RETRY LOGIC (BẮT BUỘC) ---
    producer = None
    for i in range(15):
        try:
            print(f"Connecting to Kafka (Attempt {i+1}/15)...", flush=True)
            producer = KafkaProducer(
                bootstrap_servers=server,
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            print("SUCCESS: Connected to Kafka!", flush=True)
            break
        except NoBrokersAvailable:
            print("WARN: Kafka not ready. Sleeping 5s...", flush=True)
            time.sleep(5)
    
    if not producer:
        print("ERROR: Failed to connect to Kafka. Exiting.", flush=True)
        return
    # -----------------------------

    print(f"Starting Replay data from {csv_path}...", flush=True)
    
    try:
        # Đọc từng chunk để tránh tràn RAM
        for chunk in pd.read_csv(csv_path, chunksize=100):
            for _, row in chunk.iterrows():
                record = row.to_dict()
                # Xử lý NaN
                record = {k: (None if pd.isna(v) else v) for k, v in record.items()}
                
                producer.send(topic, value=record)
                
                # In ít log thôi cho đỡ rối (100 dòng in 1 lần)
                if _ % 100 == 0:
                     print(f"-> Sent sample: {record.get('OP_UNIQUE_CARRIER')} | {record.get('FL_DATE')}", flush=True)
                
                time.sleep(0.01) # Tăng tốc lên chút (0.01s), 0.5s thì chậm lắm
                
        producer.flush()
        print("FINISHED: All data sent.", flush=True)
        
    except FileNotFoundError:
        print(f"ERROR: File CSV not found at {csv_path}", flush=True)
        # Kiểm tra xem file có trong container chưa
        print(f"Current folder content: {os.listdir('/app/data/stream_source')}", flush=True)

if __name__ == "__main__":
    run()