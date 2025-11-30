import argparse
import os
import sys
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# --- 1. DEFINING SCHEMA ---
spark_schema = StructType([
    StructField("YEAR", IntegerType(), True),
    StructField("QUARTER", IntegerType(), True),
    StructField("MONTH", IntegerType(), True),
    StructField("DAY_OF_MONTH", IntegerType(), True),
    StructField("DAY_OF_WEEK", IntegerType(), True),
    StructField("FL_DATE", StringType(), True),
    StructField("OP_UNIQUE_CARRIER", StringType(), True),
    StructField("OP_CARRIER_AIRLINE_ID", IntegerType(), True),
    StructField("OP_CARRIER", StringType(), True),
    StructField("TAIL_NUM", StringType(), True),
    StructField("OP_CARRIER_FL_NUM", IntegerType(), True),
    StructField("ORIGIN_AIRPORT_ID", IntegerType(), True),
    StructField("ORIGIN_AIRPORT_SEQ_ID", IntegerType(), True),
    StructField("ORIGIN_CITY_MARKET_ID", IntegerType(), True),
    StructField("ORIGIN", StringType(), True),
    StructField("ORIGIN_CITY_NAME", StringType(), True),
    StructField("ORIGIN_STATE_ABR", StringType(), True),
    StructField("ORIGIN_STATE_FIPS", IntegerType(), True),
    StructField("ORIGIN_STATE_NM", StringType(), True),
    StructField("ORIGIN_WAC", IntegerType(), True),
    StructField("DEST_AIRPORT_ID", IntegerType(), True),
    StructField("DEST_AIRPORT_SEQ_ID", IntegerType(), True),
    StructField("DEST_CITY_MARKET_ID", IntegerType(), True),
    StructField("DEST", StringType(), True),
    StructField("DEST_CITY_NAME", StringType(), True),
    StructField("DEST_STATE_ABR", StringType(), True),
    StructField("DEST_STATE_FIPS", IntegerType(), True),
    StructField("DEST_STATE_NM", StringType(), True),
    StructField("DEST_WAC", IntegerType(), True),
    StructField("CRS_DEP_TIME", IntegerType(), True),
    StructField("DEP_TIME", IntegerType(), True),
    StructField("DEP_DELAY", DoubleType(), True),
    StructField("DEP_DELAY_NEW", DoubleType(), True),
    StructField("DEP_DEL15", IntegerType(), True),
    StructField("DEP_DELAY_GROUP", IntegerType(), True),
    StructField("DEP_TIME_BLK", StringType(), True),
    StructField("TAXI_OUT", DoubleType(), True),
    StructField("WHEELS_OFF", IntegerType(), True),
    StructField("WHEELS_ON", IntegerType(), True),
    StructField("TAXI_IN", DoubleType(), True),
    StructField("CRS_ARR_TIME", IntegerType(), True),
    StructField("ARR_TIME", IntegerType(), True),
    StructField("ARR_DELAY", DoubleType(), True),
    StructField("ARR_DELAY_NEW", DoubleType(), True),
    StructField("ARR_DEL15", IntegerType(), True),
    StructField("ARR_DELAY_GROUP", IntegerType(), True),
    StructField("ARR_TIME_BLK", StringType(), True),
    StructField("CANCELLED", IntegerType(), True),
    StructField("CANCELLATION_CODE", StringType(), True),
    StructField("DIVERTED", IntegerType(), True),
    StructField("CRS_ELAPSED_TIME", DoubleType(), True),
    StructField("ACTUAL_ELAPSED_TIME", DoubleType(), True),
    StructField("AIR_TIME", DoubleType(), True),
    StructField("FLIGHTS", DoubleType(), True),
    StructField("DISTANCE", DoubleType(), True),
    StructField("DISTANCE_GROUP", IntegerType(), True),
    StructField("CARRIER_DELAY", DoubleType(), True),
    StructField("WEATHER_DELAY", DoubleType(), True),
    StructField("NAS_DELAY", DoubleType(), True),
    StructField("SECURITY_DELAY", DoubleType(), True),
    StructField("LATE_AIRCRAFT_DELAY", DoubleType(), True),
    StructField("DIV_AIRPORT_LANDings", StringType(), True)
])

def run_ingestion(spark, config):
    print("\n" + "="*50)
    print(">>> [DEBUG] STEP 1: CONFIGURING KAFKA SOURCE...")
    
    try:
        # 2. Read Stream from Kafka
        df_kafka = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", config["kafka"]["bootstrap_servers"])
            .option("subscribe", config["kafka"]["topic"])
            .option("startingOffsets", "earliest")
            .load()
        )
        print(">>> [DEBUG] STEP 2: KAFKA CONNECTED SUCCESSFULLY")
        # df_kafka.show(5)
    except Exception as e:
        print(f"❌ [ERROR] Cannot connect to Kafka: {e}")
        sys.exit(1)

    # 3. Parse JSON
    print(">>> [DEBUG] STEP 3: PARSING JSON DATA...")
    try:
        df_parsed = (
            df_kafka
            .select(from_json(col("value").cast("string"), spark_schema).alias("data"), col("timestamp"))
            .select("data.*", "timestamp")
            .withColumn("year", year(col("timestamp")))
            .withColumn("month", month(col("timestamp")))
            .withColumn("day", dayofmonth(col("timestamp")))
        )
        
        print("--- Expected Schema: ---")
        df_parsed.printSchema()
        print(">>> [DEBUG] STEP 4: LOGIC PARSING OK.")
        
    except Exception as e:
        print(f"❌ [ERROR] JSON Parsing Logic Failed: {e}")
        sys.exit(1)

    # 4. Write to MinIO (S3)
    # Important: Config file should have paths starting with s3a://
    minio_path = config["paths"]["hdfs_raw_data"] 
    checkpoint_path = config["paths"]["hdfs_checkpoint"]
    
    print(f">>> [DEBUG] STEP 5: PREPARING WRITE TO MINIO: {minio_path}")
    print(f">>> [DEBUG] STEP 5b: CHECKPOINT AT: {checkpoint_path}")

    try:
        query = (
            df_parsed.writeStream
            .format("parquet")
            .option("path", minio_path)
            .option("checkpointLocation", checkpoint_path)
            .partitionBy("year", "month", "day")
            .outputMode("append")
            .trigger(processingTime="1 minute")
            .start()
        )
        
        print("\n✅ [SUCCESS] Ingestion Job Started! Waiting for data...")
        print("="*50 + "\n")
        
        query.awaitTermination()
        
    except Exception as e:
        print("\n" + "!"*50)
        print("❌ [CRITICAL ERROR] FAILED TO WRITE TO MINIO!")
        print("Possible causes:")
        print("1. MinIO credentials (access/secret key) are wrong.")
        print("2. MinIO bucket does not exist (Create it first!).")
        print("3. Network issue between Spark and MinIO.")
        print("-" * 20)
        print(f"DETAILS: {e}")
        print("!"*50)
        sys.exit(1)

    # def process_batch(df, batch_id):
    #     print(f"\n--- ⚡ ĐANG XỬ LÝ BATCH ID: {batch_id} ---")
        
    #     # 1. Kiểm tra xem có dữ liệu không
    #     count = df.count()
    #     if count == 0:
    #         print("⚠️ Batch này rỗng (Kafka chưa có tin mới).")
    #         return
            
    #     print(f"✅ Tìm thấy {count} dòng dữ liệu mới!")
        
    #     # 2. IN RA MÀN HÌNH (Để bạn kiểm tra)
    #     print("👀 Dữ liệu mẫu:")
    #     df.show(5, truncate=False) # In 5 dòng đầu tiên
        
    #     # 3. GHI XUỐNG MINIO (Thực hiện nhiệm vụ chính)
    #     # Lưu ý: Trong foreachBatch, ta dùng cú pháp Batch (df.write) chứ không phải Streaming
    #     try:
    #         print(f"💾 Đang ghi vào: {minio_path}")
    #         df.write \
    #             .mode("append") \
    #             .partitionBy("year", "month", "day") \
    #             .parquet(minio_path)
    #         print("✅ Ghi file thành công!")
    #     except Exception as err:
    #         print(f"❌ Lỗi khi ghi MinIO: {err}")

    # # ---------------------------------------------------------
    # # KÍCH HOẠT STREAMING
    # # ---------------------------------------------------------
    # print(f">>> [DEBUG] BƯỚC 5: KHỞI ĐỘNG STREAM VỚI FOREACHBATCH...")
    
    # try:
    #     query = (
    #         df_parsed.writeStream
    #         # Sử dụng hàm function ở trên để xử lý
    #         .foreachBatch(process_batch)
    #         .option("path", minio_path)
    #         # .option("checkpointLocation", checkpoint_path)
    #         # Trigger: 30 giây chạy 1 lần cho bạn đỡ phải đợi lâu
    #         .trigger(processingTime="30 seconds") 
    #         .start()
    #     )
        
    #     print("\n✅ [THÀNH CÔNG] Job đang chạy! Hãy nhìn log bên dưới 👇")
    #     print("="*50 + "\n")
        
    #     query.awaitTermination()
        
    # except Exception as e:
    #     print(f"❌ [CRITICAL ERROR]: {e}")
    #     sys.exit(1)

def load_config(config_path):
    if not os.path.exists(config_path):
        print(f"❌ [ERROR] Config file not found: {config_path}")
        sys.exit(1)
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

if __name__ == "__main__":
    # 1. Argument Parsing
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="./configs/app_config.yaml")
    args = parser.parse_args()

    print(f"📂 [INFO] Loading config from: {args.config}")
    config = load_config(args.config)

    # 2. Spark Session with MinIO (S3) Support
    spark_version = "3.5.1"
    hadoop_aws_version = "3.3.4" 
    aws_sdk_version = "1.12.262"

    # Packages required for Kafka + MinIO
    packages = (
        f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version},"
        f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version},"
        f"com.amazonaws:aws-java-sdk-bundle:{aws_sdk_version}"
    )

    print("🔌 [INFO] Initializing Spark Session with MinIO support...")
    
    spark = SparkSession.builder \
        .appName("Flight_Delay_Ingestion_MinIO") \
        .config("spark.jars.packages", packages) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 3. Run
    run_ingestion(spark, config)