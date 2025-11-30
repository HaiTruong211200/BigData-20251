
import sys, os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
sys.path.append(PARENT_DIR)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from utils.db_connector import write_data_to_neon
from src.utils.spark_session import get_spark_session
from src.utils.flight_schema import spark_schema

from src.transformations.cleaner import cast_column_types, handle_missing_values
from src.transformations.aggregator import calculate_daily_stats

MINIO_RAW_PATH = "s3a://warehouse/data/raw/flights/"
REPORT_TABLE_FLIGHT_DAILY = "flight_daily_stats"

def run_etl_job(spark: SparkSession, config: dict):
    """
    Thực hiện quy trình ETL Batch: Đọc file Parquet từ MinIO, tính KPI, ghi vào Supabase.
    """
    print("\n" + "="*60)
    print(">>> BATCH ETL JOB: STARTING (MINIO -> SUPABASE) <<<")
    print("="*60)
    
    # 1. EXTRACT: Đọc toàn bộ dữ liệu lịch sử từ MinIO
    try:
        print(f"1. EXTRACT: Đang đọc dữ liệu từ MinIO: {MINIO_RAW_PATH}")
        # Đọc toàn bộ file Parquet đã được Ingestion Job tích lũy
        df_raw = spark.read.parquet(MINIO_RAW_PATH)
        print(f"   -> Đã load thành công {df_raw.count()} dòng dữ liệu.")
    except Exception as e:
        print(f"❌ ERROR: Không thể đọc dữ liệu từ MinIO. (Kiểm tra S3A Key/Endpoint)")
        print(e)
        return

    # 2. TRANSFORM: Cleaning và Aggregation
    print("2. TRANSFORM: Đang làm sạch và tính toán KPIs...")
    
    # a. CLEANING (Chạy hàm từ cleaner.py)
    # Áp dụng chuyển đổi kiểu dữ liệu (Date, Double)
    df_cast = cast_column_types(df_raw) 
    
    # Áp dụng loại bỏ các dòng có giá trị Null
    df_clean = handle_missing_values(df_cast)
    print(f"   -> Đã làm sạch và loại bỏ {df_raw.count() - df_clean.count()} dòng bị thiếu.")
    
    # b. AGGREGATION (Chạy hàm từ aggregator.py)
    # Tính toán thống kê theo ngày và hãng bay
    df_kpi_flight = calculate_daily_stats(df_clean)
    
    df_kpi_flight.show(5)
    print(f"   -> Kết quả KPI: {df_kpi_flight.count()} dòng báo cáo được tạo.")

    # 3. LOAD: Ghi kết quả tổng hợp vào Database (Supabase)
    print(f"3. LOAD: Đang ghi kết quả KPI vào Supabase (Bảng: {REPORT_TABLE_FLIGHT_DAILY})...")
    
    write_data_to_neon(
        spark_df=df_kpi_flight, 
        table_name=REPORT_TABLE_FLIGHT_DAILY, 
        mode="replace"
    )
    
    print("\n✅ BATCH ETL JOB: HOÀN THÀNH. Dữ liệu đã sẵn sàng cho Visualization!")
    print("="*60)

if __name__ == "__main__":
    # Khởi tạo Spark Session
    # spark_version = "3.5.1"
    # hadoop_aws_version = "3.3.4" 
    # aws_sdk_version = "1.12.262"
    # packages = (
    #     f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version},"
    #     f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version},"
    #     f"com.amazonaws:aws-java-sdk-bundle:{aws_sdk_version}"
    # )
    # spark = SparkSession.builder \
    #     .appName("Batch ETL Job") \
    #     .config("spark.jars.packages", packages) \
    #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    #     .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    #     .config("spark.hadoop.fs.s3a.access.key", "admin") \
    #     .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    #     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    #     .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    #     .getOrCreate()
    spark = get_spark_session(app_name="Batch_ETL_Job")
    
    # Cấu hình (nếu cần)
    config = {}
    
    # Chạy ETL Job
    run_etl_job(spark, config)
    
    # Dừng Spark Session
    spark.stop()