from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from pyspark.sql import DataFrame


def write_to_cassandra(batch_df: DataFrame, batch_id):
    if batch_df.isEmpty():
        return

    def write_partition(rows):
        cluster = Cluster(["localhost"], port=9042)
        session = cluster.connect("flight")

        prepared = session.prepare("""
            INSERT INTO flight_table (
                row_key,
                origin,
                dest,
                fl_date,
                tail_num,
                distance,
                dep_delay,
                prediction,
                confidence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        prepared.consistency_level = ConsistencyLevel.ONE

        for r in rows:
            session.execute(
                prepared,
                (
                    r.row_key,
                    r.ORIGIN,
                    r.DEST,
                    r.FL_DATE,
                    r.TAIL_NUM,
                    r.DISTANCE,
                    r.DEP_DELAY,
                    int(r.prediction),
                    str(r.probability)
                )
            )

        cluster.shutdown()

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
        .foreachPartition(write_partition)
    )
