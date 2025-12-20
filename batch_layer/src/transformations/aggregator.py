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
            #   F.sum(F.when(F.col("arr_delay") <= 0, 1).otherwise(0)).alias("on_time_flights"),
              F.sum(F.when(F.col("arr_delay") > 0, 1).otherwise(0)).alias("delayed_flights"),
              F.sum(F.col("cancellation_code").isNotNull().cast("int")).alias("cancelled_flights"),
              (
                F.count("*")
                - F.sum(F.when(F.col("arr_delay") > 0, 1).otherwise(0))
                - F.sum(F.col("cancellation_code").isNotNull().cast("int"))
            ).alias("on_time_flights"),

              F.sum(F.when(F.col("cancellation_code") == "A", 1).otherwise(0)).alias("carrier_cancel_total"),

              F.sum(F.when(F.col("cancellation_code") == "B", 1).otherwise(0)).alias("weather_cancel_total"),

              F.sum(F.when(F.col("cancellation_code") == "C", 1).otherwise(0)).alias("national_air_system_cancel_total"),   
              F.sum(F.when(F.col("cancellation_code") == "D", 1).otherwise(0)).alias("security_cancel_total"),

              # Delay sums
              F.sum("carrier_delay").alias("carrier_delay_total_minutes"),
              F.sum("weather_delay").alias("weather_delay_total_minutes"),
              F.sum("nas_delay").alias("nas_delay_total_minutes"),
              F.sum("security_delay").alias("security_delay_total_minutes"),
              F.sum("late_aircraft_delay").alias("late_aircraft_delay_total_minutes"),

              F.sum(F.when(F.col("carrier_delay") > 0, 1).otherwise(0)).alias("carrier_delay_flights"),
              F.sum(F.when(F.col("weather_delay") > 0, 1).otherwise(0)).alias("weather_delay_flights"),
              F.sum(F.when(F.col("nas_delay") > 0, 1).otherwise(0)).alias("nas_delay_flights"),
              F.sum(F.when(F.col("security_delay") > 0, 1).otherwise(0)).alias("security_delay_flights"),
              F.sum(F.when(F.col("late_aircraft_delay") > 0, 1).otherwise(0)).alias("late_aircraft_delay_flights"),

              F.sum(
                    F.coalesce(F.col("carrier_delay"), F.lit(0)) +
                    F.coalesce(F.col("weather_delay"), F.lit(0)) +
                    F.coalesce(F.col("nas_delay"), F.lit(0)) +
                    F.coalesce(F.col("security_delay"), F.lit(0)) +
                    F.coalesce(F.col("late_aircraft_delay"), F.lit(0))
                ).alias("all_delay_total_minutes")
          )
    ).orderBy("flight_date")

def calculate_daily_stats_by_airline(df: DataFrame) -> DataFrame:
    """
    Tính thống kê theo ngày & airline (hãng bay).
    """

    return (
        df.groupBy("flight_date", "op_unique_carrier")
          .agg(
              F.count("*").alias("total_flights"),
            #   F.sum(F.when(F.col("arr_delay") <= 0, 1).otherwise(0)).alias("on_time_flights"),
              F.sum(F.when(F.col("arr_delay") > 0, 1).otherwise(0)).alias("delayed_flights"),
              F.sum(F.col("cancellation_code").isNotNull().cast("int")).alias("cancelled_flights"),
                            (
                F.count("*")
                - F.sum(F.when(F.col("arr_delay") > 0, 1).otherwise(0))
                - F.sum(F.col("cancellation_code").isNotNull().cast("int"))
            ).alias("on_time_flights"),

              F.sum(F.when(F.col("cancellation_code") == "A", 1).otherwise(0)).alias("carrier_cancel_total"),

              F.sum(F.when(F.col("cancellation_code") == "B", 1).otherwise(0)).alias("weather_cancel_total"),

              F.sum(F.when(F.col("cancellation_code") == "C", 1).otherwise(0)).alias("national_air_system_cancel_total"),   
              F.sum(F.when(F.col("cancellation_code") == "D", 1).otherwise(0)).alias("security_cancel_total"),

              F.sum("carrier_delay").alias("carrier_delay_total_minutes"),
              F.sum("weather_delay").alias("weather_delay_total_minutes"),
              F.sum("nas_delay").alias("nas_delay_total_minutes"),
              F.sum("security_delay").alias("security_delay_total_minutes"),
              F.sum("late_aircraft_delay").alias("late_aircraft_delay_total_minutes"),

              F.sum(
                    F.coalesce(F.col("carrier_delay"), F.lit(0)) +
                    F.coalesce(F.col("weather_delay"), F.lit(0)) +
                    F.coalesce(F.col("nas_delay"), F.lit(0)) +
                    F.coalesce(F.col("security_delay"), F.lit(0)) +
                    F.coalesce(F.col("late_aircraft_delay"), F.lit(0))
                ).alias("all_delay_total_minutes")
          )
    ).orderBy("op_unique_carrier", "flight_date")

def calculate_daily_stats_by_origin_airport(df: DataFrame) -> DataFrame:
    """
    Tính thống kê theo ngày & sân bay khởi hành.
    """

    return (
        df.groupBy("flight_date", "origin")
          .agg(
              F.count("*").alias("total_flights"),
            #   F.sum(F.when(F.col("arr_delay") <= 0, 1).otherwise(0)).alias("on_time_flights"),
              F.sum(F.when(F.col("arr_delay") > 0, 1).otherwise(0)).alias("delayed_flights"),
              F.sum(F.col("cancellation_code").isNotNull().cast("int")).alias("cancelled_flights"),
                            (
                F.count("*")
                - F.sum(F.when(F.col("arr_delay") > 0, 1).otherwise(0))
                - F.sum(F.col("cancellation_code").isNotNull().cast("int"))
            ).alias("on_time_flights"),

              F.sum(F.when(F.col("cancellation_code") == "A", 1).otherwise(0)).alias("carrier_cancel_total"),

              F.sum(F.when(F.col("cancellation_code") == "B", 1).otherwise(0)).alias("weather_cancel_total"),

              F.sum(F.when(F.col("cancellation_code") == "C", 1).otherwise(0)).alias("national_air_system_cancel_total"),   
              F.sum(F.when(F.col("cancellation_code") == "D", 1).otherwise(0)).alias("security_cancel_total"),

              F.coalesce(F.sum("carrier_delay"), F.lit(0)).alias("carrier_delay_total_minutes"),
                F.coalesce(F.sum("weather_delay"), F.lit(0)).alias("weather_delay_total_minutes"),
                F.coalesce(F.sum("nas_delay"), F.lit(0)).alias("nas_delay_total_minutes"),
                F.coalesce(F.sum("security_delay"), F.lit(0)).alias("security_delay_total_minutes"),
                F.coalesce(F.sum("late_aircraft_delay"), F.lit(0)).alias("late_aircraft_delay_total_minutes"),

              F.sum(
                    F.coalesce(F.col("carrier_delay"), F.lit(0)) +
                    F.coalesce(F.col("weather_delay"), F.lit(0)) +
                    F.coalesce(F.col("nas_delay"), F.lit(0)) +
                    F.coalesce(F.col("security_delay"), F.lit(0)) +
                    F.coalesce(F.col("late_aircraft_delay"), F.lit(0))
                ).alias("all_delay_total_minutes")
          )
    ).orderBy("origin", "flight_date")

def calculate_daily_stats_by_destination_airport(df: DataFrame) -> DataFrame:
    """
    Tính thống kê theo ngày & sân bay đến.
    """

    return (
        df.groupBy("flight_date", "dest")
          .agg(
              F.count("*").alias("total_flights"),
            #   F.sum(F.when(F.col("arr_delay") <= 0, 1).otherwise(0)).alias("on_time_flights"),
              F.sum(F.when(F.col("arr_delay") > 0, 1).otherwise(0)).alias("delayed_flights"),
              F.sum(F.col("cancellation_code").isNotNull().cast("int")).alias("cancelled_flights"),
                            (
                F.count("*")
                - F.sum(F.when(F.col("arr_delay") > 0, 1).otherwise(0))
                - F.sum(F.col("cancellation_code").isNotNull().cast("int"))
            ).alias("on_time_flights"),

              F.sum(F.when(F.col("cancellation_code") == "A", 1).otherwise(0)).alias("carrier_cancel_total"),

              F.sum(F.when(F.col("cancellation_code") == "B", 1).otherwise(0)).alias("weather_cancel_total"),

              F.sum(F.when(F.col("cancellation_code") == "C", 1).otherwise(0)).alias("national_air_system_cancel_total"),   
              F.sum(F.when(F.col("cancellation_code") == "D", 1).otherwise(0)).alias("security_cancel_total"),

                F.coalesce(F.sum("carrier_delay"), F.lit(0)).alias("carrier_delay_total_minutes"),
                F.coalesce(F.sum("weather_delay"), F.lit(0)).alias("weather_delay_total_minutes"),
                F.coalesce(F.sum("nas_delay"), F.lit(0)).alias("nas_delay_total_minutes"),
                F.coalesce(F.sum("security_delay"), F.lit(0)).alias("security_delay_total_minutes"),
                F.coalesce(F.sum("late_aircraft_delay"), F.lit(0)).alias("late_aircraft_delay_total_minutes"),
              F.sum(
                    F.coalesce(F.col("carrier_delay"), F.lit(0)) +
                    F.coalesce(F.col("weather_delay"), F.lit(0)) +
                    F.coalesce(F.col("nas_delay"), F.lit(0)) +
                    F.coalesce(F.col("security_delay"), F.lit(0)) +
                    F.coalesce(F.col("late_aircraft_delay"), F.lit(0))
                ).alias("all_delay_total_minutes")
          )
    ).orderBy("dest", "flight_date")
