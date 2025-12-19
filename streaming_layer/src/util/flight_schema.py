from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark_schema = StructType([
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
    StructField("DEP_TIME", IntegerType(), True),
    StructField("DEP_DELAY", DoubleType(), True),
    StructField("DEP_DELAY_NEW", DoubleType(), True),
    StructField("DEP_DEL15", IntegerType(), True),
    StructField("DEP_DELAY_GROUP", IntegerType(), True),
    StructField("DEP_TIME_BLK", StringType(), True),

    StructField("TAXI_OUT", DoubleType(), True),
    StructField("WHEELS_OFF", IntegerType(), True),
    StructField("WHEELS_ON", IntegerType(), True),
    StructField("TAXI_IN", DoubleType(), True),

    StructField("CRS_ARR_TIME", IntegerType(), True),
    StructField("ARR_TIME", IntegerType(), True),
    StructField("ARR_DELAY", DoubleType(), True),
    StructField("ARR_DELAY_NEW", DoubleType(), True),
    StructField("ARR_DEL15", IntegerType(), True),
    StructField("ARR_DELAY_GROUP", IntegerType(), True),
    StructField("ARR_TIME_BLK", StringType(), True),

    StructField("CANCELLED", IntegerType(), True),
    StructField("CANCELLATION_CODE", StringType(), True),
    StructField("DIVERTED", IntegerType(), True),

    StructField("CRS_ELAPSED_TIME", DoubleType(), True),
    StructField("ACTUAL_ELAPSED_TIME", DoubleType(), True),
    StructField("AIR_TIME", DoubleType(), True),

    StructField("FLIGHTS", DoubleType(), True),
    StructField("DISTANCE", DoubleType(), True),
    StructField("DISTANCE_GROUP", IntegerType(), True),

    StructField("CARRIER_DELAY", DoubleType(), True),
    StructField("WEATHER_DELAY", DoubleType(), True),
    StructField("NAS_DELAY", DoubleType(), True),
    StructField("SECURITY_DELAY", DoubleType(), True),
    StructField("LATE_AIRCRAFT_DELAY", DoubleType(), True),

    StructField("DIV_AIRPORT_LANDings", StringType(), True)
])
