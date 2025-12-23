import sys, os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)
sys.path.append(PARENT_DIR)

from pyspark.sql import SparkSession, functions as F
from pyspark import StorageLevel
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from src.utils.spark_session import get_spark_session

# Datalake path (MinIO via S3A)
MINIO_RAW_PATH = "s3a://warehouse/data/raw/flights/"

# Where to save the trained PipelineModel
OUTPUT_DIR = os.path.join(os.path.dirname(PARENT_DIR), "model")


def load_data(spark: SparkSession):
    """Read historical parquet data from MinIO."""
    df = spark.read.parquet(MINIO_RAW_PATH)
    return df


def clean_data(df):
    """
    Basic cleaning prior to feature engineering:
    - Keep required columns only
    - Drop rows with NULLs on essentials
    - Ensure numeric columns are correct types
    """
    required = [
        "QUARTER", "month", "DAY_OF_MONTH", "DAY_OF_WEEK",
        "OP_UNIQUE_CARRIER", "ORIGIN", "DEST",
        "DISTANCE", "CRS_DEP_TIME", "DEP_DELAY"
    ]

    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df.select(*required)

    # Cast numeric columns if needed
    df = df \
        .withColumn("DISTANCE", F.col("DISTANCE").cast("double")) \
        .withColumn("CRS_DEP_TIME", F.col("CRS_DEP_TIME").cast("int")) \
        .withColumn("DEP_DELAY", F.col("DEP_DELAY").cast("double"))

    # Drop rows missing essentials
    df = df.dropna(subset=required)
    return df


def add_label(df):
    """
    Create multiclass label from DEP_DELAY as in MLModels.ipynb:
    - 0: DEP_DELAY <= 0
    - 1: 0 < DEP_DELAY <= 30
    - 2: DEP_DELAY > 30
    """
    df = df \
        .withColumn('DEP_DELAY', F.when(F.col('DEP_DELAY') <= 0, 0).otherwise(F.col('DEP_DELAY'))) \
        .withColumn('DEP_DELAY', F.when((F.col('DEP_DELAY') > 0) & (F.col('DEP_DELAY') <= 30), 1).otherwise(F.col('DEP_DELAY'))) \
        .withColumn('DEP_DELAY', F.when(F.col('DEP_DELAY') > 30, 2).otherwise(F.col('DEP_DELAY'))) \
        .withColumnRenamed('DEP_DELAY', 'LABEL') \
        .withColumn('LABEL', F.col('LABEL').cast('int'))
    return df


def build_pipeline(max_depth: int = 16) -> Pipeline:
    """Notebook-aligned pipeline: StringIndexer → OneHotEncoder → VectorAssembler → DecisionTree."""
    cat_cols = ['OP_UNIQUE_CARRIER', 'ORIGIN', 'DEST']
    indexer = StringIndexer(inputCols=cat_cols, outputCols=[c + 'Index' for c in cat_cols], handleInvalid='keep')
    encoder = OneHotEncoder(inputCols=[c + 'Index' for c in cat_cols], outputCols=[c + 'Vec' for c in cat_cols])
    assembler = VectorAssembler(
        inputCols=[
            'QUARTER', 'month', 'DAY_OF_MONTH', 'DAY_OF_WEEK',
            'OP_UNIQUE_CARRIERVec', 'ORIGINVec', 'DESTVec',
            'DISTANCE', 'CRS_DEP_TIME'
        ],
        outputCol='features'
    )
    max_bins = int(os.environ.get("DT_MAX_BINS", "64"))
    min_instances_per_node = int(os.environ.get("DT_MIN_INSTANCES_PER_NODE", "1"))
    clf = DecisionTreeClassifier(
        featuresCol='features',
        labelCol='LABEL',
        maxDepth=max_depth,
        maxBins=max_bins,
        minInstancesPerNode=min_instances_per_node,
    )
    return Pipeline(stages=[indexer, encoder, assembler, clf])


def train_and_save(df, output_dir: str):
    # Reduce repeated recomputation across splits and model.fit()
    df = df.persist(StorageLevel.DISK_ONLY)

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=2025)

    # Avoid tiny partition counts; helps memory pressure per task in local mode
    train_partitions = int(os.environ.get("TRAIN_REPARTITION", "8"))
    train_df = train_df.repartition(train_partitions)
    test_df = test_df.repartition(max(4, train_partitions // 4))

    max_depth = int(os.environ.get("DT_MAX_DEPTH", "12"))
    pipeline = build_pipeline(max_depth=max_depth)
    model = pipeline.fit(train_df)

    evaluator = MulticlassClassificationEvaluator(labelCol='LABEL', metricName='accuracy')
    acc_train = evaluator.evaluate(model.transform(train_df))
    acc_test = evaluator.evaluate(model.transform(test_df))
    print(f"Train accuracy: {acc_train:.4f} | Test accuracy: {acc_test:.4f}")

    os.makedirs(output_dir, exist_ok=True)
    model.write().overwrite().save(output_dir)
    print(f"Model saved to: {output_dir}")

    df.unpersist()


def main():
    # Local training can be memory heavy; these can be overridden via env vars.
    extra_confs = {
        "spark.sql.shuffle.partitions": os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "24"),
        "spark.sql.adaptive.enabled": os.environ.get("SPARK_SQL_ADAPTIVE_ENABLED", "true"),
        "spark.sql.adaptive.coalescePartitions.enabled": os.environ.get("SPARK_SQL_COALESCE_ENABLED", "true"),
        "spark.driver.memory": os.environ.get("SPARK_DRIVER_MEMORY", "4g"),
        "spark.executor.memory": os.environ.get("SPARK_EXECUTOR_MEMORY", "4g"),
        "spark.driver.maxResultSize": os.environ.get("SPARK_DRIVER_MAX_RESULT_SIZE", "2g"),
    }
    spark = get_spark_session(app_name="Train_DecisionTree_Model", extra_confs=extra_confs)
    print("\n=== Loading data from MinIO (parquet) ===")
    df = load_data(spark)
    print(f"Raw rows: {df.count()}")

    print("=== Cleaning data ===")
    df_clean = clean_data(df)
    print(f"After clean rows: {df_clean.count()}")

    print("=== Creating label and selecting features ===")
    df_labeled = add_label(df_clean)
    print(f"Labeled rows: {df_labeled.count()}")

    print("=== Training Decision Tree ===")
    train_and_save(df_labeled, OUTPUT_DIR)

    spark.stop()


if __name__ == "__main__":
    main()