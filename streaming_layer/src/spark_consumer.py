from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import from_json, col, concat_ws
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# spark-submit --master local[*] --packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 --repositories http://repo.hortonworks.com/content/groups/public/ --files /etc/hbase/conf/hbase-site.xml streaming_test_shc.py

# Create spark
spark = (
    SparkSession.builder
    .appName("PandasToSparkTest")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
    .config("spark.python.worker.reuse", "false")
    .config("spark.network.timeout", "300s")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.ansi.enabled", "false")
    .getOrCreate()
)

# Consume kafka event
df_raw_kafka = (spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "streaming_layer_topic")
                .option("includeHeaders", "true")
                .load())
df_str_kafka = df_raw_kafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Schema for kafka event
schema = StructType([
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
    StructField("DEP_TIME", DoubleType(), True),
    StructField("DEP_DELAY", DoubleType(), True),
    StructField("DEP_DELAY_NEW", DoubleType(), True),
    StructField("DEP_DEL15", DoubleType(), True),
    StructField("DEP_DELAY_GROUP", DoubleType(), True),
    StructField("DEP_TIME_BLK", StringType(), True),
    StructField("TAXI_OUT", DoubleType(), True),
    StructField("WHEELS_OFF", DoubleType(), True),
    StructField("WHEELS_ON", DoubleType(), True),
    StructField("TAXI_IN", DoubleType(), True),
    StructField("CRS_ARR_TIME", IntegerType(), True),
    StructField("ARR_TIME", DoubleType(), True),
    StructField("ARR_DELAY", DoubleType(), True),
    StructField("ARR_DELAY_NEW", DoubleType(), True),
    StructField("ARR_DEL15", DoubleType(), True),
    StructField("ARR_DELAY_GROUP", DoubleType(), True),
    StructField("ARR_TIME_BLK", StringType(), True),
    StructField("CANCELLED", DoubleType(), True),
    StructField("DIVERTED", DoubleType(), True),
    StructField("CRS_ELAPSED_TIME", DoubleType(), True),
    StructField("ACTUAL_ELAPSED_TIME", DoubleType(), True),
    StructField("AIR_TIME", DoubleType(), True),
    StructField("FLIGHTS", DoubleType(), True),
    StructField("DISTANCE", DoubleType(), True),
    StructField("DISTANCE_GROUP", IntegerType(), True),
    StructField("DIV_AIRPORT_LANDINGS", DoubleType(), True)
])

# Parse kafka event to structured dataframe
df_parsed = (
    df_str_kafka
    .select(
        col("key"),
        from_json(col("value"), schema).alias("data")
    )
    .select("key", "data.*")
    .withColumn(
        "row_key",
        concat_ws(
            "_",
            col("YEAR").cast(StringType()),
            col("FL_DATE").cast(StringType()),
            col("OP_CARRIER_FL_NUM").cast(StringType()),
            col("TAIL_NUM").cast(StringType()),
        )
    )
)

# Demo: print to terminal
query = (
    df_parsed
    .writeStream
    .format("console")
    .option("truncate", False)
    .option("numRows", 10)
    .start()
)

query.awaitTermination()
