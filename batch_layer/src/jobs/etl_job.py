
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
from src.transformations.aggregator import calculate_daily_stats, calculate_daily_stats_by_airline, calculate_daily_stats_by_destination_airport, calculate_daily_stats_by_origin_airport

MINIO_RAW_PATH = "s3a://warehouse/data/raw/flights/"
REPORT_TABLE_FLIGHT_DAILY = "flights_daily_stats"
REPORT_TABLE_FLIGHT_DAILY_BY_AIRLINE = "airline_daily_stats"
REPORT_TABLE_FLIGHT_DAILY_BY_DESTINATION = "destination_airport_daily_stats"
REPORT_TABLE_FLIGHT_DAILY_BY_ORIGIN = "origin_airport_daily_stats"

from pyspark.sql import SparkSession
import time

def run_etl_job(spark: SparkSession, config: dict):
    """
    Thực hiện quy trình ETL Batch: Đọc file Parquet từ MinIO, tính KPI, ghi vào Neon (Supabase).
    """
    print("\n" + "="*60)
    print(">>> BATCH ETL JOB: STARTING (MINIO -> NEON) <<<")
    print("="*60)
    
    start_time = time.time()

    # 1. EXTRACT: Đọc dữ liệu
    try:
        print(f"1. EXTRACT: Đang đọc dữ liệu từ MinIO: {MINIO_RAW_PATH}")
        df_raw = spark.read.parquet(MINIO_RAW_PATH)
        # LƯU Ý: Đã bỏ df_raw.count() để tiết kiệm thời gian
    except Exception as e:
        print(f"❌ ERROR: Không thể đọc dữ liệu từ MinIO. Lỗi: {str(e)}")
        return

    # 2. TRANSFORM: Cleaning
    print("2. TRANSFORM: Cleaning Data...")
    
    # Chuỗi xử lý
    df_cast = cast_column_types(df_raw) 
    df_clean = handle_missing_values(df_cast)

    # --- TỐI ƯU QUAN TRỌNG: CACHE ---
    # Vì df_clean được dùng lại 4 lần bên dưới, ta cache nó vào RAM
    df_clean.cache()
    
    # Chỉ gọi count() 1 lần duy nhất ở đây để kích hoạt cache và log info
    row_count = df_clean.count()
    print(f" -> Dữ liệu sạch đã sẵn sàng trong Cache: {row_count} dòng.")

    if row_count == 0:
        print("⚠️ CẢNH BÁO: Không có dữ liệu sau khi làm sạch. Dừng Job.")
        df_clean.unpersist()
        return

    # 3. AGGREGATION & LOAD (Dùng vòng lặp cho gọn)
    # Định nghĩa danh sách các task: (Hàm tính toán, Tên bảng đích)
    tasks = [
        (calculate_daily_stats, REPORT_TABLE_FLIGHT_DAILY, "KPI Tổng hợp"),
        (calculate_daily_stats_by_airline, REPORT_TABLE_FLIGHT_DAILY_BY_AIRLINE, "KPI theo Hãng bay"),
        (calculate_daily_stats_by_destination_airport, REPORT_TABLE_FLIGHT_DAILY_BY_DESTINATION, "KPI theo Sân bay đến"),
        (calculate_daily_stats_by_origin_airport, REPORT_TABLE_FLIGHT_DAILY_BY_ORIGIN, "KPI theo Sân bay đi")
    ]

    print(f"3. AGGREGATION & LOAD: Bắt đầu xử lý {len(tasks)} báo cáo...")

    for func_calc, table_name, description in tasks:
        try:
            step_start = time.time()
            print(f"   --- Đang xử lý: {description} -> Bảng: {table_name} ---")
            
            # Tính toán
            df_kpi = func_calc(df_clean)
            
            # Ghi vào DB
            write_data_to_neon(
                spark_df=df_kpi, 
                table_name=table_name, 
                mode="replace" # Hoặc "append" tùy logic của bạn
            )
            print(f"   ✅ Hoàn thành {description} trong {time.time() - step_start:.2f}s")
            
        except Exception as e:
            print(f"   ❌ Lỗi khi xử lý {description}: {str(e)}")
            # Không return, tiếp tục chạy các task khác

    # 4. CLEANUP
    # Giải phóng RAM sau khi xong việc
    df_clean.unpersist()
    
    duration = time.time() - start_time
    print("="*60)
    print(f">>> JOB FINISHED SUCCESSFULY IN {duration:.2f} SECONDS <<<")
    print("="*60)
     # Kết thúc ETL Job


    
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