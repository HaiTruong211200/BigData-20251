import pandas as pd
from hdfs import InsecureClient



def store_data_in_hdfs(transaction_data):
    columns = ['YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', 'FL_DATE', 'OP_UNIQUE_CARRIER', 
               'OP_CARRIER_AIRLINE_ID', 'OP_CARRIER', 'TAIL_NUM', 'OP_CARRIER_FL_NUM', 'ORIGIN_AIRPORT_ID', 
               'ORIGIN_AIRPORT_SEQ_ID', 'ORIGIN_CITY_MARKET_ID', 'ORIGIN', 'ORIGIN_CITY_NAME', 'ORIGIN_STATE_ABR', 
               'ORIGIN_STATE_FIPS', 'ORIGIN_STATE_NM', 'ORIGIN_WAC', 'DEST_AIRPORT_ID', 'DEST_AIRPORT_SEQ_ID', 
               'DEST_CITY_MARKET_ID', 'DEST', 'DEST_CITY_NAME', 'DEST_STATE_ABR', 'DEST_STATE_FIPS', 'DEST_STATE_NM', 
               'DEST_WAC', 'CRS_DEP_TIME', 'DEP_TIME', 'DEP_DELAY', 'DEP_DELAY_NEW', 'DEP_DEL15', 'DEP_DELAY_GROUP', 
               'DEP_TIME_BLK', 'TAXI_OUT', 'WHEELS_OFF', 'WHEELS_ON', 'TAXI_IN', 'CRS_ARR_TIME', 'ARR_TIME', 'ARR_DELAY', 
               'ARR_DELAY_NEW', 'ARR_DEL15', 'ARR_DELAY_GROUP', 'ARR_TIME_BLK', 'CANCELLED', 'DIVERTED', 'CRS_ELAPSED_TIME', 
               'ACTUAL_ELAPSED_TIME', 'AIR_TIME', 'FLIGHTS', 'DISTANCE', 'DISTANCE_GROUP', 'DIV_AIRPORT_LANDings']

    # 1. Tạo DataFrame cho bản ghi mới
    df = pd.DataFrame([transaction_data], columns=columns)

    # 2. Kết nối HDFS
    client = InsecureClient('http://localhost:9870', user='hadoop')  

    hdfs_path = '/batch-layer/raw_data.csv'

    try:
        # 3. Nếu file đã tồn tại, đọc dữ liệu cũ
        with client.read(hdfs_path, encoding='utf-8') as reader:
            existing_df = pd.read_csv(reader)
        combined = pd.concat([existing_df, df], ignore_index=True)
    except FileNotFoundError:
        # 4. Nếu file chưa tồn tại → dùng bản ghi mới
        combined = df

    # 5. Ghi lại file (overwrite)
    with client.write(hdfs_path, overwrite=True, encoding='utf-8') as writer:
        combined.to_csv(writer, index=False, header=True)

    print(f"Data appended to HDFS path: {hdfs_path}")


