import os
import sys
from pyspark.sql import SparkSession
import yaml

CONFIG_PATH = "./configs/app_config.yaml"

def load_config():
    print(f"DEBUG: Loading config from {CONFIG_PATH}...", flush=True)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_spark_session(app_name="BigData_App"):
    """
    Khởi tạo Spark Session với đầy đủ cấu hình cho MinIO và Postgres.

    """
    config = load_config()
    
    spark_version = "3.5.1"
    hadoop_aws_version = "3.3.4" 
    aws_sdk_version = "1.12.262"
    
    packages = [
        f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}",
        f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version}",
        f"com.amazonaws:aws-java-sdk-bundle:{aws_sdk_version}",
        "org.postgresql:postgresql:42.7.2" 
    ]


    minio_endpoint = config['minio_config']['endpoint']
    minio_access_key = config['minio_config']['access_key']
    minio_secret_key = config['minio_config']['secret_key']

    builder = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.sql.shuffle.partitions", "4") 

    builder = builder \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")

    spark = builder.getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    return spark