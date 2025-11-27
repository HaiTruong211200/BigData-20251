from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, to_timestamp, when, lit
from pyspark.sql.types import IntegerType, DoubleType

def cast_column_types(df: DataFrame) -> DataFrame:
    """
    Chuyển đổi các cột String sang đúng định dạng số và thời gian.
    """
    return df \
        .withColumn("dep_delay", col("dep_delay").cast(DoubleType())) \
        .withColumn("arr_delay", col("arr_delay").cast(DoubleType())) \
        .withColumn("distance", col("distance").cast(DoubleType())) \
        .withColumn("flight_date", to_date(to_timestamp(col("fl_date"), "M/d/yyyy hh:mm:ss a")))

def handle_missing_values(df: DataFrame) -> DataFrame:
    """
    Xử lý dữ liệu bị thiếu (Null/NaN).
    - Loai bỏ các hàng có giá trị Null/NaN.
    """
    df_clean = df.dropna()
    
    return df_clean