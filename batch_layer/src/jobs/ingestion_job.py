import argparse
import os
import sys
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, month, dayofmonth, to_date, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# PARENT_DIR = os.path.dirname(CURRENT_DIR)
# sys.path.append(PARENT_DIR)

from src.utils.spark_session import get_spark_session
from src.utils.flight_schema import spark_schema

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
            .option("failOnDataLoss", "true")
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
            .select("data.*")
            .withColumn("flight_date", to_date(to_timestamp(col("FL_DATE"), "M/d/yyyy h:mm:ss a")))
            .withColumn("year", year(col("flight_date")))
            .withColumn("month", month(col("flight_date")))
            .withColumn("day", dayofmonth(col("flight_date")))
            # .select("data.*", "timestamp")
            # .withColumn("year", year(col("timestamp")))
            # .withColumn("month", month(col("timestamp")))
            # .withColumn("day", dayofmonth(col("timestamp")))
        )
        
        print("--- Expected Schema: ---")
        df_parsed.printSchema()
        print(">>> [DEBUG] STEP 4: LOGIC PARSING OK.")
        
    except Exception as e:
        print(f"❌ [ERROR] JSON Parsing Logic Failed: {e}")
        sys.exit(1)

    # 4. Write to MinIO (S3)
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
            # .trigger(processingTime="1 minute")
            .trigger(availableNow=True)
            .start()
        )
        
        print("\n✅ [SUCCESS] Ingestion Job Started! Waiting for data...")
        print("="*50 + "\n")
        
        query.awaitTermination()
        
    except Exception as e:
        print("\n" + "!"*50)
        print("[CRITICAL ERROR] FAILED TO WRITE TO MINIO!")
        print("Possible causes:")
        print("1. MinIO credentials (access/secret key) are wrong.")
        print("2. MinIO bucket does not exist (Create it first!).")
        print("3. Network issue between Spark and MinIO.")
        print("-" * 20)
        print(f"DETAILS: {e}")
        print("!"*50)
        sys.exit(1)


def load_config(config_path):
    if not os.path.exists(config_path):
        print(f"[ERROR] Config file not found: {config_path}")
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

    # 2. Initialize Spark Session
    print(">>> [DEBUG] STEP 0: INITIALIZING SPARK SESSION...")
    spark = get_spark_session(app_name="Flight_Delay_Ingestion_MinIO")

    # 3. Run
    run_ingestion(spark, config)