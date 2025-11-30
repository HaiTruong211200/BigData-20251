import yaml
import sys
import pandas as pd
from pyspark.sql import DataFrame
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

def write_data_to_neon(spark_df: DataFrame, table_name: str, mode: str = "replace"):
    """
    Ghi Spark DataFrame vào Neon (Postgres) thông qua SQLAlchemy.
    
    Args:
        spark_df: DataFrame của Spark chứa dữ liệu đã tính toán.
        table_name: Tên bảng muốn lưu trên Neon (ví dụ: 'daily_kpi').
        mode: 'append' (nối thêm) hoặc 'replace' (ghi đè toàn bộ - mặc định).
    """
    print(f"\n[Neon Writer] Bắt đầu ghi vào bảng: '{table_name}'...")
    
    # 1. Chuyển đổi Spark DataFrame -> Pandas DataFrame
    # Lưu ý: Chỉ dùng cách này cho dữ liệu kết quả (KPI, Report) < 1GB RAM
    try:
        pdf = spark_df.toPandas()
    except Exception as e:
        print(f"[Lỗi Spark] Không thể chuyển sang Pandas: {e}")
        return

    if pdf.empty:
        print("[Cảnh báo] DataFrame rỗng, không có dữ liệu để ghi.")
        return

    print(f"   -> Đã chuyển đổi thành công: {len(pdf)} dòng.")

    # 2. Kết nối và Ghi vào Neon
    try:
        load_dotenv()  # Đảm bảo biến môi trường được tải
        db_url = os.getenv("NEON_DB_URL")
        engine = create_engine(db_url)

        with engine.connect() as connection:
            pdf.to_sql(
                name=table_name,
                con=engine,
                if_exists=mode,  # 'replace' sẽ Drop bảng cũ và tạo lại
                index=False,     # Không ghi cột index (0,1,2...) của Pandas
                chunksize=1000,  # Ghi từng lô 1000 dòng để tránh timeout mạng
                method='multi'   # Tối ưu tốc độ insert
            )
            
        print(f"[Thành công] Đã ghi xong vào Neon!")
        
    except Exception as e:
        print(f"[Lỗi Ghi DB] Không thể ghi vào Neon: {e}")