import happybase
from pyspark.sql.classic.dataframe import DataFrame


class HBaseClient:
    def __init__(self, host="localhost", port=9090, timeout=5000):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.conn = None

    def open(self):
        if self.conn is None:
            self.conn = happybase.Connection(
                host=self.host,
                port=self.port,
                timeout=self.timeout,
            )
        self.conn.open()

    def close(self):
        if self.conn:
            try:
                self.conn.close()
            except:
                print("Error closing HBase connection")
                pass
            self.conn = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def check_table_existed(self, table_name):
        if (table_name.encode("utf-8") not in self.conn.tables()):
            return False
        return True

    def create_table_local(self, table_name: str, families: dict):
        if (table_name.encode("utf-8") not in self.conn.tables()):
            self.conn.create_table(table_name, families)

    def put_local(self, table_name: str, row_key, data: dict):
        t = self.conn.table(table_name)
        t.put(row_key, data)

    def get_local(self, table_name, row_key):
        t = self.conn.table(table_name)
        return t.get(row_key)


def write_to_hbase(batch_df: DataFrame, batch_id):
    if batch_df.isEmpty():
        return

    with HBaseClient(host="localhost", port=9090) as hbase:
        if not hbase.check_table_existed("flight_table"):
            families = {
                'info': dict(),
                'schedule': dict(),
                'status': dict(),
                'prediction': dict(),
            }
            hbase.create_table_local("flight_table", families)
        for row in batch_df.collect():
            row_key = row["row_key"].encode("utf-8")

            hbase.put_local(
                "flight_table",
                row_key,
                {
                    b"info:origin_code": row.ORIGIN.encode("utf-8"),
                    b"info:fl_date": row.FL_DATE.encode("utf-8"),
                    b"info:dest_code": row.DEST.encode("utf-8"),
                    b"info:tail_num": row.TAIL_NUM.encode("utf-8"),
                    b"info:distance": str(row.DISTANCE).encode("utf-8"),

                    b"schedule:crs_dep_time": str(row.CRS_DEP_TIME).encode("utf-8"),
                    b"schedule:crs_arr_time": str(row.CRS_ARR_TIME).encode("utf-8"),
                    b"schedule:crs_elapsed_time": str(row.CRS_ELAPSED_TIME).encode("utf-8"),

                    b"status:act_dep_time": str(row.DEP_TIME).encode("utf-8"),
                    b"status:dep_delay": str(row.DEP_DELAY).encode("utf-8"),
                    b"status:is_cancelled": str(int(row.CANCELLED)).encode("utf-8"),
                    b"status:is_diverted": str(row.DIVERTED).encode("utf-8"),

                    # b"prediction:pred_label": row.PRED_LABEL.encode("utf-8"),
                    # b"prediction:confidence": row.CONFIDENCE.encode("utf-8")
                }
            )

            print("Put successfully!")
