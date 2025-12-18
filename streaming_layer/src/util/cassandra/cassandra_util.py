from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date, col
from pyspark.ml.functions import vector_to_array

def write_to_cassandra(batch_df: DataFrame, batch_id: int):
    print(f"\n=== BATCH {batch_id} ===")
    print("Rows in batch:", batch_df.count())

    batch_df.show(5, truncate=False)

    if batch_df.isEmpty():
        return
    (
        batch_df
        .select(
            "row_key",
            "ORIGIN",
            "DEST",
            "FL_DATE",
            "TAIL_NUM",
            "DISTANCE",
            "DEP_DELAY",
            "prediction",
            "probability"
        )
        .withColumn(
            "fl_date",
            to_date(col("FL_DATE"), "M/d/yyyy hh:mm:ss a")
        )
        .withColumnRenamed("ORIGIN", "origin")
        .withColumnRenamed("DEST", "dest")
        .withColumnRenamed("TAIL_NUM", "tail_num")
        .withColumnRenamed("DISTANCE", "distance")
        .withColumnRenamed("DEP_DELAY", "dep_delay")
        # .withColumnRenamed("probability", "confidence")
        .withColumn("confidence", vector_to_array(col("probability")))
        .drop("probability")
        .write
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", "default_keyspace")
        .option("table", "flight_table")
        .mode("append")
        .save()
    )

