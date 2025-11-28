from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def calculate_overall_statistics(df: DataFrame) -> DataFrame:
    """
    Tính toán các thống kê tổng quan về dữ liệu chuyến bay
    """
    total_flights = df.count()
    total_delayed_flights = df.filter(F.col("is_dep_delayed_15") == 1).count()
    avg_delay_minutes = df.agg(round(F.avg("dep_delay_mins"), 2).alias("avg_delay")).collect()[0]["avg_delay"]
    
    stats = [
        ("total_flights", total_flights),
        ("total_delayed_flights", total_delayed_flights),
        ("avg_delay_minutes", avg_delay_minutes)
    ]
    
    return spark.createDataFrame(stats, ["statistic", "value"])

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def calculate_daily_stats(df: DataFrame) -> DataFrame:
    """
    Tính thống kê theo ngày: tổng chuyến bay, chuyến đúng giờ, trễ, hủy,
    và tổng thời gian delay theo từng loại.
    """

    return (
        df.groupBy("flight_date")
          .agg(
              F.count("*").alias("total_flights"),
              F.sum(F.when(F.col("arr_delay") <= 0, 1).otherwise(0)).alias("on_time_flights"),
              F.sum(F.when(F.col("arr_delay") > 0, 1).otherwise(0)).alias("delayed_flights"),
              F.sum(F.when(F.col("cancelled") == 1, 1).otherwise(0)).alias("cancelled_flights"),

              # Delay sums
              F.sum("carrier_delay").alias("carrier_delay_total"),
              F.sum("weather_delay").alias("weather_delay_total"),
              F.sum("nas_delay").alias("nas_delay_total"),
              F.sum("security_delay").alias("security_delay_total"),
              F.sum("late_aircraft_delay").alias("late_aircraft_delay_total"),
          )
    ).orderBy("flight_date")

def calculate_daily_stats_by_airline(df: DataFrame) -> DataFrame:
    """
    Tính thống kê theo ngày & airline (hãng bay).
    """

    return (
        df.groupBy("flight_date", "op_carrier_airline_id")
          .agg(
              F.count("*").alias("total_flights"),
              F.sum(F.when(F.col("arr_delay") <= 0, 1).otherwise(0)).alias("on_time_flights"),
              F.sum(F.when(F.col("arr_delay") > 0, 1).otherwise(0)).alias("delayed_flights"),
              F.sum(F.when(F.col("cancelled") == 1, 1).otherwise(0)).alias("cancelled_flights"),

              F.sum("carrier_delay").alias("carrier_delay_total"),
              F.sum("weather_delay").alias("weather_delay_total"),
              F.sum("nas_delay").alias("nas_delay_total"),
              F.sum("security_delay").alias("security_delay_total"),
              F.sum("late_aircraft_delay").alias("late_aircraft_delay_total"),
          )
    ).orderBy("op_carrier_airline_id", "flight_date")

def calculate_daily_stats_by_origin_airport(df: DataFrame) -> DataFrame:
    """
    Tính thống kê theo ngày & sân bay khởi hành.
    """

    return (
        df.groupBy("flight_date", "origin_airport_id")
          .agg(
              F.count("*").alias("total_flights"),
              F.sum(F.when(F.col("arr_delay") <= 0, 1).otherwise(0)).alias("on_time_flights"),
              F.sum(F.when(F.col("arr_delay") > 0, 1).otherwise(0)).alias("delayed_flights"),
              F.sum(F.when(F.col("cancelled") == 1, 1).otherwise(0)).alias("cancelled_flights"),

              F.sum("carrier_delay").alias("carrier_delay_total"),
              F.sum("weather_delay").alias("weather_delay_total"),
              F.sum("nas_delay").alias("nas_delay_total"),
              F.sum("security_delay").alias("security_delay_total"),
              F.sum("late_aircraft_delay").alias("late_aircraft_delay_total"),
          )
    ).orderBy("origin_airport_id", "flight_date")

def calculate_daily_stats_by_destination_airport(df: DataFrame) -> DataFrame:
    """
    Tính thống kê theo ngày & sân bay đến.
    """

    return (
        df.groupBy("flight_date", "dest_airport_id")
          .agg(
              F.count("*").alias("total_flights"),
              F.sum(F.when(F.col("arr_delay") <= 0, 1).otherwise(0)).alias("on_time_flights"),
              F.sum(F.when(F.col("arr_delay") > 0, 1).otherwise(0)).alias("delayed_flights"),
              F.sum(F.when(F.col("cancelled") == 1, 1).otherwise(0)).alias("cancelled_flights"),

              F.sum("carrier_delay").alias("carrier_delay_total"),
              F.sum("weather_delay").alias("weather_delay_total"),
              F.sum("nas_delay").alias("nas_delay_total"),
              F.sum("security_delay").alias("security_delay_total"),
              F.sum("late_aircraft_delay").alias("late_aircraft_delay_total"),
          )
    ).orderBy("dest_airport_id", "flight_date")
