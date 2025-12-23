import os
import sys
from pyspark.sql import SparkSession
import yaml

CONFIG_PATH = "./configs/app_config.yaml"

def load_config():
    print(f"DEBUG: Loading config from {CONFIG_PATH}...", flush=True)
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_spark_session(app_name="BigData_App", extra_confs: dict | None = None):
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

    # Allow runtime overrides via env vars (handy for local training runs)
    spark_driver_memory = os.environ.get("SPARK_DRIVER_MEMORY")
    spark_executor_memory = os.environ.get("SPARK_EXECUTOR_MEMORY")
    spark_local_dir = os.environ.get("SPARK_LOCAL_DIR")
    spark_shuffle_partitions = os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "4")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.shuffle.partitions", spark_shuffle_partitions)
    )

    if spark_driver_memory:
        builder = builder.config("spark.driver.memory", spark_driver_memory)
    if spark_executor_memory:
        builder = builder.config("spark.executor.memory", spark_executor_memory)
    if spark_local_dir:
        builder = builder.config("spark.local.dir", spark_local_dir)

    builder = builder \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")

    if extra_confs:
        for k, v in extra_confs.items():
            if v is None:
                continue
            builder = builder.config(k, str(v))

    spark = builder.getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    return spark