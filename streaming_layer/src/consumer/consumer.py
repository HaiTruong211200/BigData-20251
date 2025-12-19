import sys
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when,
    concat, lit, lpad,
    current_date
)
from pyspark.sql.functions import to_timestamp, date_format, expr, avg, count, sum, window, current_timestamp, from_json
from pyspark.ml.functions import vector_to_array
from pyspark.sql.types import *
from pyspark.ml import PipelineModel

from src.util.flight_schema import spark_schema



# ==========================================
# 1. KHỞI TẠO SPARK
# ==========================================
load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ASTRA_BUNDLE_PATH = os.path.join(BASE_DIR, "conf", "secure-connect-bigdata-cassandra.zip")

spark = (
    SparkSession.builder
    .appName("PandasToSparkTest")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.cassandra.connection.config.cloud.path", "secure-connect-bigdata-cassandra.zip")
    .config("spark.files", ASTRA_BUNDLE_PATH)
    .config("spark.cassandra.auth.username", "token")
    .config("spark.cassandra.auth.password", os.getenv("ASTRA_PASSWORD"))
    .config("spark.cassandra.output.consistency.level", "LOCAL_QUORUM")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,""com.datastax.spark:spark-cassandra-connector_2.12:3.5.1")
    .config("spark.python.worker.reuse", "false")
    .config("spark.network.timeout", "300s")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.ansi.enabled", "false")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .config("spark.sql.shuffle.partitions", "2") 
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 2. ĐỌC DỮ LIỆU TỪ KAFKA (Phải khớp cột trong CSV)
raw_stream = spark.readStream \
    .schema(spark_schema) \
    .option("header", "true") \
    .option("maxFilesPerTrigger", 1) \
    .csv("./data") 

# 1. Đọc Raw Data từ Kafka
kafka_raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "flight_topic") \
    .option("startingOffsets", "latest") \
    .load()

raw_stream = kafka_raw_df.select(
    from_json(col("value").cast("string"), spark_schema).alias("data")
).select("data.*") 

# 2. Giả lập ngày hiện tại cho dữ liệu (vì dữ liệu cũ từ 2025)

simulated_df = raw_stream.withColumn("FL_DATE", current_date().cast("string"))

def make_safe_ts(time_col):
    """
    Hàm này đảm bảo không bao giờ bị lỗi parsing, kể cả gặp số 2400 hay 9999
    """
    time_int = time_col.cast("int")
    
    safe_time = when(time_int.isNull(), 0) \
                .when(time_int >= 2400, 2359) \
                .when(time_int < 0, 0) \
                .otherwise(time_int)
    
    time_str = concat(current_date().cast("string"), lit(" "), lpad(safe_time.cast("string"), 4, "0"))
    
    return to_timestamp(time_str, "yyyy-MM-dd HHmm")

processed_df = simulated_df \
    .withColumn("now", current_timestamp()) \
    .withColumn("ts_dep", make_safe_ts(col("DEP_TIME"))) \
    .withColumn("ts_off", make_safe_ts(col("WHEELS_OFF"))) \
    .withColumn("ts_on", make_safe_ts(col("WHEELS_ON"))) \
    .withColumn("ts_arr", make_safe_ts(col("ARR_TIME"))) 

final_df = processed_df.withColumn("realtime_status",
    when(col("CANCELLED") == 1, "CANCELLED")
    
    .when(col("now") > col("ts_arr"), "COMPLETED")
    
    .when(col("now") > col("ts_on"), "TAXI_IN")
    
    .when(col("now") > col("ts_off"), "AIRBORNE")
    
    .when(col("now") > col("ts_dep"), "TAXI_OUT")
    
    .otherwise("SCHEDULED")
)

# Chọn các cột cần thiết để hiển thị
display_df = final_df.select(
    col("FL_DATE"),
    col("OP_CARRIER"),
    col("OP_CARRIER_FL_NUM"),
    col("ORIGIN"),
    col("DEST"),
    col("DEP_TIME").alias("Plan_Dep"),
    date_format(col("now"), "HH:mm").alias("Current_Time"), 
    col("realtime_status"),
    col("ARR_TIME").alias("Plan_Arr"),
    col("ts_dep"),
    col("ts_arr"),
    col("ts_off"),
    col("ts_on")
)


# ====================================================
# LOAD MODEL ĐÃ TRAIN
model_path = "./Model/DT"
try:
    flight_model = PipelineModel.load(model_path)
    print(">>> Đã load Model ML thành công!")
except:
    print("!!! Chưa thấy file Model. Hãy chạy file train_model.py trước!")
    sys.exit(1)

ml_input_df = final_df \
    .withColumn("DEP_DELAY", col("DEP_DELAY").cast("double")) \
    .withColumn("DISTANCE", col("DISTANCE").cast("double")) \
    .withColumn("TAXI_OUT", col("TAXI_OUT").cast("double")) \
    .fillna(0, subset=["DEP_DELAY", "DISTANCE", "TAXI_OUT"])

# 2. Dự báo (Prediction)
predicted_df = flight_model.transform(ml_input_df)



enriched_df = predicted_df \
    .withColumn("is_holding", 
        # Logic: Nếu NAS_DELAY > 0 và trạng thái là đang đến/đã đến -> coi như bị Holding
        when((col("NAS_DELAY") > 0) & (col("realtime_status").isin("TAXI_IN", "COMPLETED", "AIRBORNE")), 1)
        .otherwise(0)
    ) \
    .withColumn("prob_array", vector_to_array(col("probability"))) \
    .withColumn("confidence", 
    expr("prob_array[cast(prediction as int)]"))\
    .withColumn("prediction", 
        when(col("prediction") == 0.0, "ON_TIME")      # 0
        .when(col("prediction") == 1.0, "DELAY_RISK")  # 1
        .when(col("prediction") == 2.0, "CANCELLED")   # 2
    ) \
    .withColumn("flight_code", concat(col("OP_CARRIER"), lit(" "), col("OP_CARRIER_FL_NUM")))

# ====================================================
# TẠO CÁC DATAFRAME TỔNG HỢP (AGGREGATION)
# ====================================================

# --- STREAM A: ORIGIN KPI (Sức khỏe sân bay đi) ---
# Chỉ tính các chuyến ĐÃ cất cánh (AIRBORNE/COMPLETED) mới có số Taxi-Out chính xác
origin_kpi_df = enriched_df \
    .withWatermark("ts_dep", "1 hour") \
    .filter(col("realtime_status").isin("AIRBORNE", "COMPLETED", "TAXI_IN")) \
    .groupBy(
        window(col("ts_dep"), "1 hour", "5 minutes"), # Cửa sổ 1 tiếng, trượt 5 phút
        col("ORIGIN")
    ).agg(
        avg("TAXI_OUT").alias("avg_taxi_out"),
        count("*").alias("total_departures")
    )

# --- STREAM B: DESTINATION KPI (Sức khỏe sân bay đến) ---
# Tính toán lượng máy bay phải bay vòng (Holding)
dest_kpi_df = enriched_df \
    .withWatermark("ts_arr", "1 hour") \
    .filter(col("realtime_status").isin("AIRBORNE", "TAXI_IN", "COMPLETED")) \
    .groupBy(
        window(col("ts_arr"), "1 hour", "5 minutes"),
        col("DEST")
    ).agg(
        sum("is_holding").alias("holding_count"),
        avg("NAS_DELAY").alias("avg_nas_delay")
    )

# --- STREAM C: LIVE BOARD (Chi tiết chuyến bay đang hoạt động) ---
# Lọc lấy những chuyến ĐANG BAY hoặc ĐANG LĂN
live_board_df = enriched_df \
    .filter(col("realtime_status").isin("AIRBORNE", "TAXI_OUT", "TAXI_IN")) \
    .select(
        col("flight_code"),
        col("FL_DATE").alias("fl_date"),
        col("OP_CARRIER").alias("op_carrier"),
        col("OP_CARRIER_FL_NUM").alias("op_carrier_fl_num"),
        col("ORIGIN"),
        col("DEST"),
        date_format(col("ts_dep"), "HH:mm").alias("dep_time"), # Giờ khởi hành chuẩn
        col("realtime_status"),
        col("prediction"),
        col("DEP_DELAY").alias("dep_delay"),
        col("confidence"),
        col("ts_dep"),
        col("ts_arr"),
    )

# ====================================================
# 3. XUẤT RA CONSOLE ĐỂ KIỂM TRA
# ====================================================

# Xuất bảng Live Board (Chuyến bay đang hoạt động)
query_live = live_board_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .queryName("LiveBoardQuery") \
    .start()

# Xuất KPI Origin (Sẽ thấy avg_taxi_out)
# Dùng "update" mode cho Aggregation
query_origin = origin_kpi_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .queryName("OriginKPIQuery") \
    .start()

# Xuất KPI Dest
query_dest = dest_kpi_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .queryName("DestKPIQuery") \
    .start()

spark.streams.awaitAnyTermination()


# ==============================================================================
# HÀM GHI DỮ LIỆU VÀO CASSANDRA (FOREACHBATCH)
# ==============================================================================

# --- 1. Ghi Origin Stats & Global Ranking ---
def save_origin_data(batch_df, batch_id):
    if batch_df.isEmpty(): return
    
    # A. Ghi vào bảng 'origin_stats' (Line Chart)
    # Spark Window là struct {start, end}, ta lấy 'start' làm mốc thời gian
    batch_df.select(
        col("ORIGIN").alias("origin"),
        col("window.start").alias("window_start"),
        col("avg_taxi_out"),
        col("total_departures")
    ).write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="origin_stats", keyspace="flight_speed_layer") \
        .save()
        
# --- 2. Ghi Destination Stats ---
def save_dest_data(batch_df, batch_id):
    if batch_df.isEmpty(): return

    batch_df.select(
        col("DEST").alias("dest"),
        col("window.start").alias("window_start"),
        col("holding_count"),
        col("avg_nas_delay")
    ).write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="dest_stats", keyspace="flight_speed_layer") \
        .save()

# --- 3. Ghi Live Board ---
def save_live_board(batch_df, batch_id):
    if batch_df.isEmpty(): return

    # batch_df này lấy từ live_board_df ở bước trước
    batch_df.select(
        col("ts_dep").alias("event_time"), # Lấy giờ khởi hành làm mốc
        col("fl_code"), # VD: VN 123
        col("fl_date"),
        col("op_carrier"),
        col("op_carrier_fl_num"),
        col("ORIGIN").alias("origin"),
        col("DEST").alias("dest"),
        col("ts_dep").alias("dep_time"),
        col("prediction"), # Kết quả từ Model
        col("confidence").cast("double"),
        col("DEP_DELAY").alias("dep_delay")
    ).write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="live_board", keyspace="flight_speed_layer") \
        .save()

# ==============================================================================
# KÍCH HOẠT STREAMS
# ==============================================================================

# Stream 1: Origin & Ranking
# query_origin = origin_kpi_df.writeStream \
#     .foreachBatch(save_origin_data) \
#     .outputMode("update") \
#     .option("checkpointLocation", "/tmp/cp/origin_v1") \
#     .start()

# # Stream 2: Destination Stats
# query_dest = dest_kpi_df.writeStream \
#     .foreachBatch(save_dest_data) \
#     .outputMode("update") \
#     .option("checkpointLocation", "/tmp/cp/dest_v1") \
#     .start()

# # Stream 3: Live Board (Chi tiết chuyến bay)
# # Lưu ý: Live board là dữ liệu chi tiết, dùng mode "append" hoặc "update" đều được
# # Nhưng vì ta filter status, nên dùng "append" hợp lý hơn cho log
# query_live = live_board_df.writeStream \
#     .foreachBatch(save_live_board) \
#     .outputMode("append") \
#     .option("checkpointLocation", "/tmp/cp/live_v1") \
#     .start()

# print(">>> Streaming đang chạy và ghi vào Cassandra...")
# spark.streams.awaitAnyTermination()
